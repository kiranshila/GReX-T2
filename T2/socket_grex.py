import socket

import numpy as np
import datetime
import time
from astropy.time import Time
from astropy.io import ascii

from T2 import cluster_heimdall, socket_grex, names

try:
    from T2 import triggering
except ModuleNotFoundError:
    print("not importing triggering")


#from dsautils import cnf, dsa_store, dsa_syslog
#from etcd3.exceptions import ConnectionFailedError
#from event import names

#ds = dsa_store.DsaStore()
#logger = dsa_syslog.DsaSyslogger()
#logger.subsystem("software")
#logger.app("T2")
# my_cnf = cnf.Conf(use_etcd=True)
# try:
#     t2_cnf = my_cnf.get("t2")
# except (KeyError, ConnectionFailedError):
#     print("Cannot find t2 cnf using etcd. Falling back to hard coded values.")
#     logger.warning(
#         "Cannot find t2 cnf using etcd. Falling back to hard coded values."
#     )
#     my_cnf = cnf.Conf(use_etcd=False)
#     t2_cnf = my_cnf.get("t2")

from collections import deque

import logging as logger
logger.basicConfig(filename='logs/output.log', 
                    encoding='utf-8', 
                    level=logger.DEBUG)

nbeams_queue = deque(maxlen=10)


def filter_candidates(candsfile):
    """ Take a single gulp of candidates, 
    parse, cluster, and then filter to 
    produce highest S/N candidate and save 
    to a json file
    """
    outroot = '/home/liam/data/grex/candidates/T2/'

    col_heimdall = ['snr', 'if', 'itime', 'mjds', 'ibox', 'idm', 'dm', 'ibeam']
    min_dm = 50
    max_ibox = 64
    min_snr = 8.0
    min_snr_t2out = 10.0
    max_ncl = np.inf
    max_cntb = np.inf
    max_cntb0 = np.inf
    target_params = (50., 100., 20.)  # Galactic bursts

    tab = ascii.read(candsfile, names=col_heimdall,
                     guess=True, fast_reader=False,
                     format='no_header')

    if not len(tab):
        return 

    cluster_heimdall.cluster_data(tab, metric='euclidean', 
                                  allow_single_cluster=True, 
                                  return_clusterer=False)

    tab2 = cluster_heimdall.get_peak(tab)
    tab3 = cluster_heimdall.filter_clustered(tab2, 
                                            min_snr=min_snr, 
                                            min_dm=min_dm, 
                                            max_ibox=max_ibox, 
                                            max_cntb=max_cntb,
                                            max_cntb0=max_cntb0, 
                                            max_ncl=max_ncl, 
                                            target_params=target_params)

    itimes = tab3['itime']
    maxsnr = tab3['snr'].max()
    imaxsnr = np.where(tab3['snr'] == maxsnr)[0][0]        
    itime_imax = str(itimes[imaxsnr])
    mjd = tab3['mjds'][imaxsnr]
    
    trigger = False
    lastname = names.get_lastname_grex(outroot)
    cat = None
    beam_model = None
    coords = None
    snrs = None
    nbeams_queue = 0
    prev_trig_time = None
    min_timedelt = 60.
    tab3['mjds'] = 59000.00

    X = cluster_heimdall.dump_cluster_results_json(
                                                    tab3,
                                                    trigger=trigger,
                                                    lastname=lastname,
                                                    cat=cat,
                                                    coords=coords,
                                                    snrs=snrs,
                                                    outroot=outroot,
                                                    frac_wide=0.0,
                                                   )

def parse_socket(
    host,
    ports,
    selectcols=["itime", "idm", "ibox", "ibeam"],
    outroot=None,
    plot_dir=None,
    trigger=False,
    source_catalog=None,
):
    """
    Takes standard MBHeimdall giants socket output and returns full table, classifier inputs and snr tables.
    selectcols will take a subset of the standard MBHeimdall output for cluster.

    host, ports: same with heimdall -coincidencer host:port
    ports can be list of integers.
    selectcol: list of str.  Select columns for clustering.
    source_catalog: path to file containing source catalog for source rejection. default None
    """

    # count of output - separate from gulps
    globct = 0

    if isinstance(ports, int):
        ports = [ports]

    assert isinstance(ports, list)

    lastname = 'blank4now'#names.get_lastname()

    ss = []

    # pre-calculate beam model and get source catalog
    if source_catalog is not None:
        # model = triggering.get_2Dbeam_model()
        # model = triggering.read_beam_model()
        model = None
        coords, snrs = triggering.parse_catalog(source_catalog)
    else:
        print("No source catalog found. No model generated.")
        model = None
        coords = None
        snrs = None

    logger.info(f"Reading from {len(ports)} sockets...")
    print(f"Reading from {len(ports)} sockets...")
    while True:
        if len(ss) != len(ports):
            for port in ports:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.bind((host, port))  # assigns the socket with an address
                s.listen(1)  # accept no. of incoming connections
                ss.append(s)

        # ds.put_dict(
        #     "/mon/service/T2service",
        #     {"cadence": 60, "time": Time(datetime.datetime.utcnow()).mjd},
        # )

        cls = []
        try:
            for s in ss:
                (
                    clientsocket,
                    address,
                ) = s.accept()  # stores the socket details in 2 variables
                cls.append(clientsocket)
        except KeyboardInterrupt:
            print("Escaping socket connection")
            logger.info("Escaping socket connection")
            break

        # read in heimdall socket output
        candsfile = ""
        gulps = []
        for cl in cls:
            cf = recvall(cl, 100000000).decode("utf-8")

            gulp, *lines = cf.split("\n")
            try:
                gulp = int(gulp)
            #                print(f"received gulp {gulp} with {len(lines)-1} lines")
            except ValueError:
                print(
                    f"Could not get int from this read ({gulp}). Skipping this client."
                )
                continue

            gulps.append(gulp)
            cl.close()

            if len(lines) > 1:
                if len(lines[0]) > 0:
                    candsfile += "\n".join(lines)

        print(f"Received gulp_i {gulps}")
        if len(gulps) != len(cls):
            print(f"not all clients are gulping gulp {gulps}. Skipping...")
            gulp_status(1)
            continue

        if len(set(gulps)) > 1:
            logger.info(
                f"not all clients received from same gulp: {set(gulps)}. Restarting socket connections."
            )
            print(
                f"not all clients received from same gulp: {set(gulps)}. Restarting socket connections."
            )

            for s in ss:
                s.close()
            ss = []
            for port in ports:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                try:
                    s.bind((host, port))  # assigns the socket with an address
                except OSError:
                    print("socket bind failed.")
                    continue
                s.listen(1)  # accept no. of incoming connections
                ss.append(s)
            gulp_status(2)
            continue
        else:
            pass 
            # ds.put_dict(
            #     "/mon/service/T2gulp",
            #     {"cadence": 60, "time": Time(datetime.datetime.utcnow()).mjd},
            # )

        if candsfile == "\n" or candsfile == "":  # skip empty candsfile
            print(f"candsfile is empty. Skipping.")
            logger.info(f"candsfile is empty. Skipping.")

            gulp_status(0)
            continue

        try:
            tab = cluster_heimdall.parse_candsfile(candsfile)
            lastname = cluster_and_plot(
                tab,
                globct,
                selectcols=selectcols,
                outroot=outroot,
                plot_dir=plot_dir,
                trigger=trigger,
                lastname=lastname,
                cat=source_catalog,
                beam_model=model,
                coords=coords,
                snrs=snrs,
            )
            globct += 1
        except KeyboardInterrupt:
            print("Escaping parsing and plotting")
            logger.info("Escaping parsing and plotting")
            break
        except OverflowError:
            print("overflowing value. Skipping this gulp...")
            logger.warning("overflowing value. Skipping this gulp...")

            print(candsfile)
            gulp_status(3)
            continue
        gulp_status(0)  # success!


def cluster_and_plot(
    tab,
    globct,
    selectcols=["itime", "idm", "ibox", "ibeam"],
    outroot=None,
    plot_dir=None,
    trigger=False,
    lastname=None,
    max_ncl=None,
    cat=None,
    beam_model=None,
    coords=None,
    snrs=None,
):
    """
    Run clustering and plotting on read data.
    Can optionally save clusters as heimdall candidate table before filtering and json version of buffer trigger.
    lastname is name of previously triggered/named candidate.
    cat: path to source catalog (default None)
    beam_model: pre-calculated beam model (default None)
    coords and snrs: from source catalog (default None)
    """

    # TODO: put these in json config file
    min_dm = t2_cnf["min_dm"]  # smallest dm in filtering
    max_ibox = t2_cnf["max_ibox"]  # largest ibox in filtering
    min_snr = t2_cnf["min_snr"]  # smallest snr in filtering
    min_snr_t2out = t2_cnf[
        "min_snr_t2out"
    ]  # smallest snr to write T2 output cand file
    if max_ncl is None:
        max_ncl = t2_cnf[
            "max_ncl"
        ]  # largest number of clusters allowed in triggering
    max_cntb0 = t2_cnf["max_ctb0"]
    max_cntb = t2_cnf["max_ctb"]
    target_params = (50.0, 100.0, 20.0)  # Galactic bursts

    # cluster
    cluster_heimdall.cluster_data(
        tab,
        metric="euclidean",
        allow_single_cluster=True,
        return_clusterer=False,
    )
    tab2 = cluster_heimdall.get_peak(tab)
    nbeams_gulp = cluster_heimdall.get_nbeams(tab2)
    nbeams_queue.append(nbeams_gulp)
    print(f"nbeams_queue: {nbeams_queue}")

    # Liam edit to preserve real FRBs during RFI storm:
    # if nbeam > 100 and frac_wide < 0.8: do not discard
    maxsnr = tab["snr"].max()
    imaxsnr = np.where(tab["snr"] == maxsnr)[0][0]
    cl_max = tab["cl"][imaxsnr]
    frac_wide = np.sum(tab["ibox"][tab["cl"] == cl_max] >= 32) / float(
        len(tab["ibox"][tab["cl"] == cl_max])
    )

    if len(tab["ibox"][tab["cl"] == cl_max]) == 1:
        frac_wide = 0.0

    # Width filter for false positives
    ibox64_filter = False
    if len(tab2):
        ibox64_cnt = np.sum(tab2["ibox"] == 64) / float(len(tab2["ibox"]))
#        print("here", ibox64_cnt, tab2["ibox"])
        if ibox64_cnt > 0.85 and len(tab2["ibox"]) > 15:
            ibox64_filter = True
            print("ibox64 filter")

    # Done

    tab3 = cluster_heimdall.filter_clustered(
        tab2,
        min_snr=min_snr,
        min_dm=min_dm,
        max_ibox=max_ibox,
        max_cntb=max_cntb,
        max_cntb0=max_cntb0,
        max_ncl=max_ncl,
        target_params=target_params,
    )  # max_ncl rows returned

    col_trigger = np.zeros(len(tab2), dtype=int)
    if outroot is not None and len(tab3) and not ibox64_filter:
        tab4, lastname = cluster_heimdall.dump_cluster_results_json(
            tab3,
            trigger=trigger,
            lastname=lastname,
            cat=cat,
            beam_model=beam_model,
            coords=coords,
            snrs=snrs,
            outroot=outroot,
            nbeams=sum(nbeams_queue),
            frac_wide=frac_wide,
        )
        if tab4 is not None and trigger:
            col_trigger = np.where(
                tab4 == tab2, lastname, 0
            )  # if trigger, then overload

    # write T2 clustered/filtered results
    if outroot is not None and len(tab2):
        tab2["trigger"] = col_trigger
        cluster_heimdall.dump_cluster_results_heimdall(
            tab2,
            outroot + str(np.floor(time.time()).astype("int")) + ".cand",
            min_snr_t2out=min_snr_t2out,
            max_ncl=max_ncl,
        )

    return lastname


def recvall(sock, n):
    """
    helper function to receive all bytes from a socket
    sock: open socket
    n: maximum number of bytes to expect. you can make this ginormous!
    """

    data = bytearray()
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return data
        data.extend(packet)

    return data


def gulp_status(status):
    """Set etcd key to track gulp status.
    0 means good, non-zero means some kind of failure for a gulp.
    1 means not all clients are gulping
    2 means different gulps received, so restarting clients
    3 means overflow error during parsing of table.
    t2_num is the process number running T2. Only one for now.
    """

    t2_num = 1

    # ds.put_dict(
    #     f"/mon/T2/{t2_num}",
    #     {
    #         "gulp_status": int(status),
    #         "t2_num": t2_num,
    #         "time": Time(datetime.datetime.utcnow()).mjd,
    #     },
    # )