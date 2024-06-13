import sqlite3
import logging


def connect(path: str) -> sqlite3.Connection:
    """Connect to the SQLite database"""
    con = sqlite3.connect(path)
    logging.info("Successfully connected to SQLite database")
    return con


def create_tables(con: sqlite3.Connection):
    with con:
        cur = con.cursor()
        # Setup the database's tables if they don't already exist
        cur.execute(
            "CREATE TABLE IF NOT EXISTS candidate (id INTEGER PRIMARY KEY AUTOINCREMENT, dm REAL NOT NULL, snr REAL NOT NULL, mjd REAL NOT NULL, boxcar INTEGER NOT NULL, sample INTEGER NOT NULL) STRICT"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS injection (id INTEGER PRIMARY KEY AUTOINCREMENT, mjd REAL NOT NULL, filename TEXT NOT NULL, sample INTEGER NOT NULL) STRICT"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS cluster (id INTEGER PRIMARY KEY AUTOINCREMENT, peak INTEGER NOT NULL, injection INTEGER, FOREIGN KEY (centroid) REFERENCES candidate (id), FOREIGN KEY (injection) REFERENCES injection (id)) STRICT"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS cluster_member (candidate INTEGER PRIMARY KEY, cluster INTEGER NOT NULL, FOREIGN KEY (candidate) REFERENCES candidate (id), FOREIGN KEY (cluster) REFERENCES cluster (id)) WITHOUT ROWID"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS trigger (id INTEGER PRIMARY KEY AUTOINCREMENT, cand_name TEXT, cluster INTEGER NOT NULL, FOREIGN KEY (cluster) REFERENCES cluster (id))"
        )


def is_injection(mjd: float, con: sqlite3.Connection) -> bool:
    """Run a SQL query to see if T0 performed an injection near candidate time"""

    # Not sure why the offset from T0 is this much
    OFFSET = 15 / 86400  # Seconds to days
    logging.debug(f"Testing if candidate at {mjd} corresponds to an injection")
    with con:
        cur = con.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM injection WHERE mjd BETWEEN ? AND ?",
            (
                mjd - OFFSET / 2,
                mjd + OFFSET / 2,
            ),
        )
        res = cur.fetchone()
        logging.debug(f"SQL Query Result: {res}")
        return res[0] == 1


def insert_candidates(tab, con: sqlite3.Connection):
    """Insert raw candidates (with corrected times) into the SQLite database"""

    # Transform the candidate table to a list of tuples
    con.executemany(
        "INSERT INTO candidate(dm, snr, mjd, boxcar, sample) VALUES(?, ?, ?, ?, ?)"
    )
