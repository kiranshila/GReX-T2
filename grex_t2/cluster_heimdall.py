import json
import os.path
import socket
import sqlite3
import hdbscan
import numpy as np
import requests
from astropy.io import ascii
from astropy.io.ascii.core import InconsistentTableError
from astropy.time import Time
from numpy.lib.recfunctions import structured_to_unstructured
from grex_t2 import triggering, names, database
import logging

# half second at heimdall time resolution (after march 18)
OFFSET = 1907
DOWNSAMPLE = 4


def parse_candsfile(candsfile):
    """Takes standard MBHeimdall giants output and returns full table,
    classifier inputs and snr tables.
    (Can add cleaning here, eventually)
    """

    if os.path.exists(candsfile):
        logging.debug(f"Candsfile {candsfile} is path, so opening it")
        candsfile = open(candsfile, "r").read()
    else:
        ncands = len(candsfile.split("\n")) - 1
        logging.debug(f"Received {ncands} candidates")
    col_heimdall = ["snr", "if", "itime", "mjds", "ibox", "idm", "dm", "ibeam"]
    col_T2old = [
        "snr",
        "if",
        "itime",
        "mjds",
        "ibox",
        "idm",
        "dm",
        "ibeam",
        "cl",
        "cntc",
        "cntb",
    ]
    col_T2 = [
        "snr",
        "if",
        "itime",
        "mjds",
        "ibox",
        "idm",
        "dm",
        "ibeam",
        "cl",
        "cntc",
        "cntb",
        "trigger",
    ]

    # flag for heimdall file
    _hdfile = False

    try:
        tab = ascii.read(
            candsfile,
            names=col_heimdall,
            guess=True,
            fast_reader=False,
            format="no_header",
        )
        _hdfile = True
        logging.debug("Read with heimdall columns")
    except InconsistentTableError:
        try:
            tab = ascii.read(
                candsfile,
                names=col_T2,
                guess=True,
                fast_reader=False,
                format="no_header",
            )
            _hdfile = False
            logging.debug("Read with T2 columns")
        except InconsistentTableError:
            try:
                tab = ascii.read(
                    candsfile,
                    names=col_T2old,
                    guess=True,
                    fast_reader=False,
                    format="no_header",
                )
                _hdfile = False
                logging.debug("Read with old style T2 columns")
            except InconsistentTableError:
                logging.warning("Inconsistent table. Skipping...")
                return ([], [], [])

    tab["ibeam"] = tab["ibeam"].astype(int)

    start_time_mjd = requests.get("http://localhost:8083/start_time").json()
    tab["mjds"] = tab["mjds"] / 86400.0 + start_time_mjd

    return tab


def dm_range(dm_max, dm_min=5.0, frac=0.2):
    """Generate list of DM-windows in which
    to search for single pulse groups.

    Parameters
    ----------
    dm_max : float
        max DM
    dm_min : float
        min DM
    frac : float
        fractional size of each window

    Returns
    -------
    dm_list : list
        list of tuples containing (min, max) of each
        DM window
    """

    dm_list = []
    prefac = (1 - frac) / (1 + frac)

    while dm_max > dm_min:
        if dm_max < 100.0:
            prefac = (1 - 2 * frac) / (1 + 2 * frac)
        if dm_max < 50.0:
            prefac = 0.0

        dm_list.append((int(prefac * dm_max), int(dm_max)))
        dm_max = int(prefac * dm_max)

    return dm_list


def cluster_data(
    tab,
    selectcols=["itime", "idm", "ibox", "ibeam"],
    min_cluster_size=2,
    min_samples=5,
    metric="euclidean",
    return_clusterer=False,
    allow_single_cluster=True,
    cluster_selection_epsilon=10,
):
    """Take data from parse_candsfile and identify clusters
    via hamming metric.
    selectcols will take a subset of the standard MBHeimdall output
    """

    data = structured_to_unstructured(
        tab[selectcols].as_array()
    )  # ok for single dtype (int)
    clusterer = None
    try:
        clusterer = hdbscan.HDBSCAN(
            metric=metric,
            min_cluster_size=min_cluster_size,
            min_samples=min_samples,
            cluster_selection_method="eom",
            cluster_selection_epsilon=10,
            allow_single_cluster=allow_single_cluster,
        ).fit(data)

        cl = clusterer.labels_
    except ValueError:
        logging.info(
            "Clustering did not run. Each point \
               assigned to unique cluster."
        )
        cl = np.arange(len(data))

    # hack assumes fixed columns
    #    bl = data[:, 3]
    cntb, cntc = (
        np.zeros((len(data), 1), dtype=int),
        np.zeros((len(data), 1), dtype=int),
    )
    ucl = np.unique(cl)

    for i in ucl:
        ww = np.where(i == cl)
        cntc[ww] = len(ww[0])
    #        ubl = np.unique(bl[ww])
    #        cntb[ww] = len(ubl)

    # modifies tab in place
    tab["cl"] = cl.tolist()
    tab["cntc"] = cntc.flatten().tolist()
    tab["cntb"] = cntb.flatten().tolist()

    if return_clusterer:
        return clusterer


def get_peak(tab):
    """Given labeled data, find max snr row per cluster
    Adds in count of candidates in same beam and same cluster.
    Puts unclustered candidates in as individual events.
    """

    cl = tab["cl"].astype(int)
    snrs = tab["snr"]
    ipeak = []
    for i in np.unique(cl):
        if i == -1:
            continue
        clusterinds = np.where(i == cl)[0]
        maxsnr = snrs[clusterinds].max()
        imaxsnr = np.where(snrs == maxsnr)[0][0]
        ipeak.append(imaxsnr)
    # Append unclustered
    ipeak += [i for i in range(len(tab)) if cl[i] == -1]
    logging.info(f"Found {len(ipeak)} cluster peaks")

    return tab[ipeak]


def filter_clustered(
    tab,
    min_snr=None,
    min_dm=None,
    max_ibox=None,
    min_cntb=None,
    max_cntb=None,
    min_cntc=None,
    max_cntc=None,
    max_ncl=None,
    target_params=None,
):
    """Function to select a subset of clustered output.
    Can set minimum SNR, min/max number of beams in cluster,
    min/max total count in cluster.
    target_params is a tuple (min_dmt, max_dmt, min_snrt)
    for custom snr threshold for target.
    max_ncl is maximum number of clusters returned (sorted by SNR).
    """

    if target_params is not None:
        min_dmt, max_dmt, min_snrt = target_params
    else:
        min_dmt, max_dmt, min_snrt = None, None, None

    good = [True] * len(tab)

    if min_snr is not None:
        if min_snrt is None:
            good *= tab["snr"] > min_snr
        else:
            good0 = (tab["snr"] > min_snr) * (tab["dm"] > max_dmt)
            good1 = (tab["snr"] > min_snr) * (tab["dm"] < min_dmt)
            good2 = (
                (tab["snr"] > min_snrt) * (tab["dm"] > min_dmt) * (tab["dm"] < max_dmt)
            )
            good *= good0 + good1 + good2

    if min_dm is not None:
        good *= tab["dm"] > min_dm
    if max_ibox is not None:
        good *= tab["ibox"] < max_ibox
    if min_cntb is not None:
        good *= tab["cntb"] > min_cntb
    if max_cntb is not None:
        good *= tab["cntb"] < max_cntb
    if min_cntc is not None:
        good *= tab["cntc"] > min_cntc
    if max_cntc is not None:
        good *= tab["cntc"] < max_cntc

    tab_out = tab[good]

    if max_ncl is not None:
        if len(tab_out) > max_ncl:
            min_snr_cl = sorted(tab_out["snr"])[-max_ncl]
            good = tab_out["snr"] >= 50  # min_snr_cl
            tab_out = tab_out[good]
            logging.info(
                f"Limiting output to {max_ncl} \
                    clusters with snr>{min_snr_cl}."
            )

    logging.info(f"Filtering clusters from {len(tab)} to {len(tab_out)} candidates.")

    return tab_out


def dump_cluster_results_json(
    tab,
    db_con: sqlite3.Connection,
    outputfile=None,
    output_cols=["mjds", "snr", "ibox", "dm", "ibeam", "cntb", "cntc"],
    trigger=False,
    lastname=None,
    cat=None,
    coords=None,
    snrs=None,
    outroot="./",
    last_trigger_time=0.0,
):
    """
    Takes tab from parse_candsfile and clsnr from get_peak,
    json file will be named with generated name, unless outputfile is set
    candidate name and specnum is calculated. name is unique.
    trigger is bool to update DsaStore to trigger data dump.
    cat is path to source catalog (default None)
    beam_model is pre-calculated beam model (default None)
    coords and snrs are parsed source file input
    returns row of table that triggered, along with name generated for candidate.
    """

    if coords is None or snrs is None:
        coords, snrs = triggering.parse_catalog(cat)

    itimes = tab["itime"]
    maxsnr = tab["snr"].max()
    imaxsnr = np.where(tab["snr"] == maxsnr)[0][0]
    specnum = (int(itimes[imaxsnr]) - OFFSET) * DOWNSAMPLE
    dm = float(tab["dm"][imaxsnr])
    box = int(tab["ibox"][imaxsnr])
    mjd = float(tab["mjds"][imaxsnr])

    # if no injection file or no coincident injection
    candname = names.increment_name(mjd, lastname=lastname)

    output_dict = {candname: {}}
    if outputfile is None:
        outputfile = f"{outroot}{candname}.json"

    row = tab[imaxsnr]
    for col in output_cols:
        if type(row[col]) == np.int64:
            output_dict[candname][col] = int(row[col])
        else:
            output_dict[candname][col] = row[col]

    output_dict[candname]["specnum"] = specnum

    # json.dumps doesn't know how to serialize numpy integers for some insane reason
    trigger_payload = {"candname": candname, "itime": int(itimes[imaxsnr])}

    # Check to see if the max SNR candidate corresponds with an injection
    injection_id = database.find_injection(mjd, db_con)
    if injection_id is not None:
        logging.info("Candidate corresponds with injection, skipping trigger")

    # Dump this candidate also to the SQL database
    database.insert_candidate(
        candname,
        dm,
        maxsnr,
        mjd,
        box,
        int(itimes[imaxsnr]),
        injection_id,
        db_con,
    )

    if len(tab) > 0:
        with open(outputfile, "w") as f:  # encoding='utf-8'
            logging.info(f"Writing trigger file for index {imaxsnr} with SNR={maxsnr}")
            json.dump(output_dict, f, ensure_ascii=False, indent=4)

        if trigger and injection_id is None:
            send_trigger(trigger_payload)

        return row, candname, last_trigger_time

    else:
        logging.info(f"Not triggering on block with {len(tab)} candidates")
        return None, lastname, last_trigger_time


def send_trigger(trigger_payload):
    trigger_message = json.dumps(trigger_payload).encode("utf-8")
    logging.info(
        f"Sending trigger for candidate {trigger_payload['candname']} at time index {trigger_payload['itime']} at Time {Time.now().mjd}",
    )
    UDP_PORT = 65432
    UDP_IP = "127.0.0.1"
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # Internet  # UDP
    sock.sendto(trigger_message, (UDP_IP, UDP_PORT))


def dump_cluster_results_heimdall(tab, outputfile, min_snr_t2out=None, max_ncl=None):
    """
    Takes tab from parse_candsfile and clsnr from get_peak,
    output T2-clustered results with the same columns as
    heimdall.cand into a file outputfile.
    The output is in pandas format with column names
    in the 1st row.
    min_snr_t2out is a min snr on candidates to write.
    max_ncl is number of rows to write.
    """

    # transform to specnum
    tab["itime"] = (tab["itime"] - OFFSET) * DOWNSAMPLE

    if min_snr_t2out is not None:
        good = [True] * len(tab)
        good *= tab["snr"] > min_snr_t2out
        tab = tab[good]
        if not all(good) and len(tab):
            logging.info(
                f"Limiting output to SNR>{min_snr_t2out} with {len(tab)} clusters."
            )

    if max_ncl is not None:
        if len(tab) > max_ncl:
            min_snr_cl = sorted(tab["snr"])[-max_ncl]
            good = (tab["snr"] >= min_snr_cl) + [
                str(tt) != "0" for tt in tab["trigger"]
            ]  # keep trigger
            tab = tab[good]
            logging.info(
                f"Limiting output to {max_ncl} clusters with snr>{min_snr_cl}."
            )
    else:
        logging.info("max_ncl not set. Not filtering heimdall output file.")

    if len(tab) > 0:
        tab.write(outputfile, format="ascii.no_header", overwrite=True)
        return True

    return False
