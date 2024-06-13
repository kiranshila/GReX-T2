import os
import sqlite3
import numpy as np
import time
from astropy.time import Time
from astropy.io import ascii
from grex_t2 import cluster_heimdall, names
from collections import deque
import requests

nbeams_queue = deque(maxlen=10)


def filter_candidates(
    candsfile,
    outroot,
    db_con: sqlite3.Connection,
    output=True,
    trigger=True,
    last_trigger_time=0.0,
):
    """Take a single gulp of candidates,
    parse, cluster, and then filter to
    produce highest S/N candidate and save
    to a json file
    """

    col_heimdall = ["snr", "if", "itime", "mjds", "ibox", "idm", "dm", "ibeam"]
    min_dm = 50
    max_ibox = 64
    min_snr = 10.0
    min_snr_t2out = 10.0
    max_ncl = np.inf
    max_cntb = np.inf
    target_params = (50.0, 100.0, 20.0)  # Galactic bursts

    tab = ascii.read(
        candsfile, names=col_heimdall, guess=True, fast_reader=False, format="no_header"
    )

    # Ensure that the candidate table is not empty
    if not len(tab):
        return

    cluster_heimdall.cluster_data(
        tab, metric="euclidean", allow_single_cluster=True, return_clusterer=False
    )

    tab2 = cluster_heimdall.get_peak(tab)
    print(tab2)
    col_trigger = np.zeros(len(tab2), dtype=int)

    # Ensure that the candidate table is not empty
    if not len(tab2):
        return

    tab3 = cluster_heimdall.filter_clustered(
        tab2,
        min_snr=min_snr,
        min_dm=min_dm,
        max_ibox=max_ibox,
        max_cntb=max_cntb,
        max_ncl=max_ncl,
        target_params=target_params,
    )

    # Ensure that the candidate table is not empty
    if not len(tab3):
        return

    # itimes = tab3["itime"]
    # maxsnr = tab3["snr"].max()
    # imaxsnr = np.where(tab3["snr"] == maxsnr)[0][0]
    # itime_imax = str(itimes[imaxsnr])
    #   mjd = tab3["mjds"][imaxsnr]
    lastname = names.get_lastname_grex(outroot)
    cat = None
    coords = None
    snrs = None
    # prev_trig_time = None
    # min_timedelt = 60.0

    # Query for the start-time in MJD
    start_time = requests.get("http://localhost:8083/start_time").json()
    tab3["mjds"] = tab3["mjds"] / 86400.0 + start_time
    tab2["mjds"] = tab2["mjds"] / 86400.0 + start_time

    tab4, lastname, last_trigger_time = cluster_heimdall.dump_cluster_results_json(
        tab3,
        db_con,
        trigger=trigger,
        lastname=lastname,
        cat=cat,
        coords=coords,
        snrs=snrs,
        outroot=outroot,
        last_trigger_time=last_trigger_time,
    )

    if tab4 is not None and trigger:
        col_trigger = np.where(tab4 == tab2, lastname, 0)  # if trigger, then overload

    # write T2 clustered/filtered results
    if outroot is not None and len(tab2):
        tab2["trigger"] = col_trigger
        output_file = (
            outroot
            + "cluster_output"
            + str(np.floor(time.time()).astype("int"))
            + ".cand"
        )
        outputted = cluster_heimdall.dump_cluster_results_heimdall(
            tab2, output_file, min_snr_t2out=min_snr_t2out, max_ncl=max_ncl
        )

        # aggregate files
        if outputted:
            a = Time.now().mjd
            output_mjd = str(int(a))
            old_mjd = str(int(a) - 1)

            os.system("cat " + output_file + " >> " + outroot + output_mjd + ".csv")
            os.system(
                "if ! grep -Fxq 'snr,if,specnum,mjds,ibox,idm,dm,ibeam,cl,cntc,cntb,trigger' "
                + outroot
                + output_mjd
                + ".csv; then sed -i '1s/^/snr,if,specnum,mjds,ibox,idm,dm,ibeam,cl,cntc,cntb,trigger\\n/' "
                + outroot
                + output_mjd
                + ".csv; fi"
            )

            os.system(
                "echo 'snr,if,specnum,mjds,ibox,idm,dm,ibeam,cl,cntc,cntb,trigger' > "
                + outroot
                + "cluster_output.csv"
            )
            os.system(
                "test -f "
                + outroot
                + old_mjd
                + ".csv && tail -n +2 "
                + outroot
                + old_mjd
                + ".csv | tr ' ' ',' >> "
                + outroot
                + "cluster_output.csv"
            )
            os.system(
                "tail -n +2 "
                + outroot
                + output_mjd
                + ".csv | tr ' ' ',' >> "
                + outroot
                + "cluster_output.csv"
            )


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
