"""azwiki-pulse command line.

    python pulse_cli.py demo                 # offline: generate sample stream, process, report
    python pulse_cli.py stream --minutes 15  # live: consume Azerbaijani Wikipedia edits
    python pulse_cli.py process              # bronze -> silver -> gold (incremental)
    python pulse_cli.py report               # print trending pages, alerts, SCD2 history
"""
import argparse
import logging
import sys

from pulse import config


def main() -> int:
    parser = argparse.ArgumentParser(description="Real-time pipeline for Azerbaijani Wikipedia activity")
    sub = parser.add_subparsers(dest="command", required=True)
    s = sub.add_parser("stream", help="consume the live stream into bronze")
    s.add_argument("--minutes", type=float, default=10)
    s.add_argument("--max-events", type=int, default=None)
    sub.add_parser("process", help="process new bronze files into silver and gold")
    sub.add_parser("report", help="print a report of the gold layer")
    d = sub.add_parser("demo", help="offline end-to-end demo with generated data")
    d.add_argument("--hours", type=int, default=36)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s %(name)-14s %(message)s",
                        datefmt="%H:%M:%S")

    if args.command == "stream":
        from pulse.ingest import consume
        consume(minutes=args.minutes, max_events=args.max_events)
    elif args.command == "process":
        from pulse.process import process
        try:
            process()
        except Exception:
            return 1
    elif args.command == "report":
        from pulse.process import connect
        from pulse.report import print_report
        print_report(connect())
    elif args.command == "demo":
        from pulse.process import connect, process
        from pulse.report import print_report
        from pulse.sample import write_sample_bronze
        if config.WAREHOUSE_PATH.exists() or config.BRONZE_DIR.exists():
            print("data/ already exists. Delete the data folder first to run a clean demo.")
            return 1
        files = write_sample_bronze(hours=args.hours)
        logging.getLogger("pulse").info("generated %d sample bronze files", len(files))
        process()
        print_report(connect())
    return 0


if __name__ == "__main__":
    sys.exit(main())
