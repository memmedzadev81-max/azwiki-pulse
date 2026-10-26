"""Bronze layer writer + checkpoint.

Bronze files are JSON Lines, partitioned by ingestion date and hour:
    data/bronze/date=2026-10-24/hour=17/batch_20261024T170512_000042.jsonl

Each line is an envelope around the untouched source event:
    {"ingested_at": "...", "sse_id": "...", "event": {...raw event...}}

Files are written to a .tmp name and then renamed, so a crash never leaves a
half-written file that the next stage would read (atomic publish).
"""
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from . import config


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class BronzeWriter:
    def __init__(self, bronze_dir: Path | None = None):
        self.bronze_dir = Path(bronze_dir or config.BRONZE_DIR)
        self._seq = 0

    def write_batch(self, records: list[dict], now: datetime | None = None) -> Path | None:
        if not records:
            return None
        now = now or utc_now()
        self._seq += 1
        folder = self.bronze_dir / f"date={now:%Y-%m-%d}" / f"hour={now:%H}"
        folder.mkdir(parents=True, exist_ok=True)
        final = folder / f"batch_{now:%Y%m%dT%H%M%S}_{now.microsecond:06d}_{self._seq:04d}.jsonl"
        tmp = final.with_suffix(".jsonl.tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            for rec in records:
                fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
        os.replace(tmp, final)
        return final


class Checkpoint:
    """Stores the last SSE id that is safely in bronze.

    On restart the consumer sends it as `Last-Event-ID`, so the stream resumes
    where it stopped. Because the checkpoint is saved *after* the batch file,
    a crash can replay a few events (at-least-once delivery); silver removes
    those duplicates by event_id, making the pipeline effectively exactly-once.
    """

    def __init__(self, path: Path | None = None):
        self.path = Path(path or config.CHECKPOINT_PATH)

    def load(self) -> str | None:
        if not self.path.exists():
            return None
        return json.loads(self.path.read_text(encoding="utf-8")).get("last_event_id")

    def save(self, last_event_id: str, events_total: int) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"last_event_id": last_event_id,
                                   "events_total": events_total,
                                   "saved_at": utc_now().isoformat()}), encoding="utf-8")
        os.replace(tmp, self.path)
