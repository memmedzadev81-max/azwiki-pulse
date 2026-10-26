"""Streaming ingestion: Wikimedia EventStreams -> bronze micro-batches.

- Connects to the global recentchange stream (all wikis, ~dozens of events/s)
  and keeps only Azerbaijani Wikipedia events.
- Buffers events and flushes a bronze file every BATCH_MAX_EVENTS events or
  BATCH_MAX_SECONDS seconds.
- Saves a checkpoint after each flush and resumes from it with the
  `Last-Event-ID` header (restart-safe).
- Reconnects with exponential backoff when the connection drops (the server
  closes long-lived connections regularly, so this is expected behaviour).
"""
import json
import logging
import time

import requests

from . import config
from .bronze import BronzeWriter, Checkpoint, utc_now
from .sse import parse_sse

log = logging.getLogger("pulse.ingest")


def keep_event(evt: dict) -> bool:
    if evt.get("meta", {}).get("domain") == "canary":   # Wikimedia health-check events
        return False
    return evt.get("wiki") == config.TARGET_WIKI


def open_stream(last_event_id: str | None):
    headers = {"User-Agent": config.USER_AGENT, "Accept": "text/event-stream"}
    if last_event_id:
        headers["Last-Event-ID"] = last_event_id
    resp = requests.get(config.STREAM_URL, headers=headers, stream=True, timeout=(10, 60))
    resp.raise_for_status()
    resp.encoding = "utf-8"
    return resp


def consume(minutes: float = 10, max_events: int | None = None,
            writer: BronzeWriter | None = None, checkpoint: Checkpoint | None = None,
            stream_opener=open_stream, sleep=time.sleep) -> dict:
    writer = writer or BronzeWriter()
    checkpoint = checkpoint or Checkpoint()
    deadline = time.monotonic() + minutes * 60

    last_id = checkpoint.load()
    if last_id:
        log.info("Resuming from checkpoint")
    buffer: list[dict] = []
    batch_started = time.monotonic()
    stats = {"seen": 0, "kept": 0, "files": 0, "reconnects": 0, "bad_json": 0}
    backoff = 1

    def flush():
        nonlocal buffer, batch_started
        if buffer:
            path = writer.write_batch(buffer)
            stats["files"] += 1
            log.info("bronze <- %d events  %s", len(buffer), path.name)
            buffer = []
        if last_id:
            checkpoint.save(last_id, stats["kept"])
        batch_started = time.monotonic()

    def done() -> bool:
        return time.monotonic() >= deadline or (max_events is not None and stats["kept"] >= max_events)

    try:
        while not done():
            try:
                resp = stream_opener(last_id)
                backoff = 1
                for sse in parse_sse(resp.iter_lines(decode_unicode=True)):
                    stats["seen"] += 1
                    if sse.id:
                        last_id = sse.id
                    try:
                        evt = json.loads(sse.data)
                    except json.JSONDecodeError:
                        stats["bad_json"] += 1
                        continue
                    if keep_event(evt):
                        buffer.append({"ingested_at": utc_now().isoformat(),
                                       "sse_id": sse.id, "event": evt})
                        stats["kept"] += 1
                    if (len(buffer) >= config.BATCH_MAX_EVENTS
                            or time.monotonic() - batch_started >= config.BATCH_MAX_SECONDS):
                        flush()
                    if done():
                        break
            except (requests.RequestException, ConnectionError) as exc:
                stats["reconnects"] += 1
                log.warning("Stream dropped (%s). Reconnecting in %ss", exc.__class__.__name__, backoff)
                sleep(backoff)
                backoff = min(backoff * 2, 60)
    except KeyboardInterrupt:
        log.info("Stopped by user")
    finally:
        flush()
    log.info("ingest finished: %s", stats)
    return stats
