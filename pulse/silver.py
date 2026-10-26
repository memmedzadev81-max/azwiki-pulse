"""BRONZE -> SILVER: validate, quarantine, de-duplicate."""
import json
import sqlite3
from datetime import datetime
from pathlib import Path

from .contract import ContractError, to_silver_row

COLUMNS = ["event_id", "event_ts", "ingested_at", "lateness_s", "event_type", "namespace",
           "title", "user_name", "is_bot", "is_minor", "length_old", "length_new",
           "bytes_delta", "comment", "is_revert", "log_type", "move_target", "source_file"]
INSERT_SQL = (f"INSERT OR IGNORE INTO silver_events ({', '.join(COLUMNS)}) "
              f"VALUES ({', '.join('?' for _ in COLUMNS)})")


def load_file(con: sqlite3.Connection, path: Path, rel_name: str, run_id: int) -> dict:
    stats = {"lines": 0, "inserted": 0, "duplicates": 0, "dead": 0}
    now = datetime.now().isoformat(timespec="seconds")
    with Path(path).open(encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, start=1):
            if not line.strip():
                continue
            stats["lines"] += 1
            try:
                row = to_silver_row(json.loads(line), rel_name)
            except json.JSONDecodeError:
                reason = "invalid_json"
            except ContractError as exc:
                reason = str(exc)
            else:
                cur = con.execute(INSERT_SQL, [row[c] for c in COLUMNS])
                if cur.rowcount == 1:
                    stats["inserted"] += 1
                else:
                    stats["duplicates"] += 1       # same event_id already in silver
                continue
            con.execute(
                "INSERT INTO silver_dead_letter (source_file, line_no, reason, raw, quarantined_at) "
                "VALUES (?, ?, ?, ?, ?)", (rel_name, line_no, reason, line.strip()[:4000], now))
            stats["dead"] += 1

    con.execute(
        """INSERT INTO etl_file_manifest
           (file_path, run_id, processed_at, lines_read, rows_inserted, duplicates, dead_letters)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (rel_name, run_id, now, stats["lines"], stats["inserted"], stats["duplicates"], stats["dead"]))
    return stats


def pending_files(con: sqlite3.Connection, bronze_dir: Path) -> list[tuple[Path, str]]:
    done = {r[0] for r in con.execute("SELECT file_path FROM etl_file_manifest")}
    files = sorted(Path(bronze_dir).rglob("*.jsonl"))
    out = []
    for p in files:
        rel = p.relative_to(bronze_dir).as_posix()
        if rel not in done:
            out.append((p, rel))
    return out
