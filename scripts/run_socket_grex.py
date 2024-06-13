import argparse
import socket
from grex_t2 import socket_grex, database
import logging

HOST = "127.0.0.1"
PORT = 12345

# Setup logging to write to a file and stdout, with formatting
# TODO Write to OpenTelemetry as well to collect logs for grafana

logging.basicConfig(
    level=logging.DEBUG,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("output.log"), logging.StreamHandler()],
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run T2 clustering and triggering on heimdall output"
    )
    parser.add_argument(
        "--trigger",
        type=bool,
        default=True,
        help="Enable triggers to dump the voltage buffer",
        required=False,
    )
    parser.add_argument(
        "--outroot",
        type=str,
        default="/hdd/data/candidates/T2/",
        help="Directory to store intermediate candidate files",
        required=False,
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default="/hdd/data/candidates.db",
        help="Path to SQLite database",
        required=False,
    )
    return parser.parse_args()


def main():
    args = parse_args()
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Create a UDP socket
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # Ensure that you can reconnect
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # Bind the socket to the port
    server_address = (HOST, PORT)
    s.bind(server_address)

    # Connect to SQLite
    db_con = database.connect(args.db_path)
    database.create_tables(db_con)

    logging.info(
        "Connected to socket %s:%d. Triggering set to %s" % (HOST, PORT, args.trigger)
    )

    last_trigger_time = 0.0
    # Outer loop that runs as long as T2 is running
    while True:
        candstr_list = ""
        cand_count = 0
        # Inner loop for chunks of Heimdall output data
        while True:
            # Recieve 512 bytes
            data, address = s.recvfrom(512)

            # Waiting for end of text. When chunk is done, break inner loop
            if len(data) == 1 and data == b"\x03":
                break

            candstr = data.decode("utf-8")

            # Removing the \n from the end of line
            candstr_list += candstr
            cand_count += 1

        logging.info(f"Number of candidates {cand_count}")

        if cand_count > 0:
            logging.info(f"Filtering, last trig was {last_trigger_time}")
            last_trigger_time = socket_grex.filter_candidates(
                candstr_list,
                outroot=args.outroot,
                db_con=db_con,
                trigger=args.trigger,
                last_trigger_time=last_trigger_time,
            )

        continue


if __name__ == "__main__":
    main()
