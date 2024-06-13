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
            """CREATE TABLE IF NOT EXISTS candidate (
                cand_name TEXT PRIMARY KEY
                dm REAL NOT NULL,
                snr REAL NOT NULL,
                mjd REAL NOT NULL,
                boxcar INTEGER NOT NULL,
                sample INTEGER NOT NULL
                injection INTEGER, 
                FOREIGN KEY (injection) REFERENCES injection (id)
            ) STRICT WITHOUT ROWID"""
        )


def find_injection(mjd: float, con: sqlite3.Connection) -> int:
    """Run a SQL query to see if T0 performed an injection near candidate time and returns the injection id if found, None otherwise"""

    # Not sure why the offset from T0 is this much
    OFFSET = 15 / 86400  # Seconds to days
    logging.debug(f"Testing if candidate at {mjd} corresponds to an injection")
    with con:
        cur = con.cursor()
        cur.execute(
            "SELECT * FROM injection WHERE mjd BETWEEN ? AND ?",
            (
                mjd - OFFSET / 2,
                mjd + OFFSET / 2,
            ),
        )
        res = cur.fetchone()
        logging.debug(f"SQL Query Result: {res}")
        if len(res) == 1:
            return int(res[0])
        else:
            return None


def insert_candidate(
    cand_name: str,
    dm: float,
    snr: float,
    mjd: float,
    boxcar: int,
    sample: int,
    injection_id: int,
    con: sqlite3.Connection,
):
    """Insert clustered candidates into the SQLite database"""
    con.execute(
        "INSERT INTO candidate(cand_name, dm, snr, mjd, boxcar, sample, injection) VALUES(?, ?, ?, ?, ?, ?, ?)",
        (cand_name, dm, snr, mjd, boxcar, sample, injection_id),
    )
