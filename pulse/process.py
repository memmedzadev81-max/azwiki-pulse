"""Batch processing of new bronze files: silver -> gold -> quality, in ONE transaction."""
import logging
import sqlite3
from datetime import datetime
from pathlib import Path

from . import config
from .gold import process_gold
from .quality import run_checks
from .silver import load_file, pending_files

log = logging.getLogger("pulse.process")


def connect(db_path: Path | None = None) -> sqlite3.Connection:
    path = Path(db_path or config.WAREHOUSE_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(path)
    con.executescript((config.SQL_DIR / "schema.sql").read_text(encoding="utf-8"))
    return con


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def process(db_path: Path | None = None, bronze_dir: Path | None = None) -> dict:
    bronze_dir = Path(bronze_dir or config.BRONZE_DIR)
    con = connect(db_path)
    run_id = con.execute("INSERT INTO etl_runs (started_at, status) VALUES (?, 'running')",
                         (_now(),)).lastrowid
    con.commit()
    try:
        files = pending_files(con, bronze_dir)
        totals = {"lines": 0, "inserted": 0, "duplicates": 0, "dead": 0}
        for path, rel in files:
            stats = load_file(con, path, rel, run_id)
            for k in totals:
                totals[k] += stats[k]
        log.info("silver: %d files, %d new rows, %d duplicates skipped, %d dead-lettered",
                 len(files), totals["inserted"], totals["duplicates"], totals["dead"])

        gold = process_gold(con)
        log.info("gold:   %d fact rows, %d hour/page buckets rebuilt, %d active alerts touched",
                 gold["fact_rows"], gold["hour_page_buckets"], gold["alerts"])

        checks = run_checks(con)
        log.info("quality: %d checks passed", len(checks))

        con.execute(
            """UPDATE etl_runs SET finished_at = ?, status = 'success', files = ?, silver_rows = ?,
               duplicates = ?, dead_letters = ?, fact_rows = ?, alerts = ? WHERE run_id = ?""",
            (_now(), len(files), totals["inserted"], totals["duplicates"], totals["dead"],
             gold["fact_rows"], gold["alerts"], run_id))
        con.commit()
        return {"run_id": run_id, "status": "success", "files": len(files), **totals, **gold}
    except Exception as exc:
        con.rollback()
        con.execute("UPDATE etl_runs SET finished_at = ?, status = 'failed', message = ? WHERE run_id = ?",
                    (_now(), str(exc)[:1000], run_id))
        con.commit()
        log.error("Run %s failed and was rolled back: %s", run_id, exc)
        raise
    finally:
        con.close()
