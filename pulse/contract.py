"""Data contract for incoming events.

Turns one bronze envelope into a typed silver row, or returns a rejection
reason. Rejected records go to the dead-letter table instead of being dropped,
so nothing disappears silently and bad upstream changes are visible.
"""
from datetime import datetime, timezone

from . import config

ALLOWED_TYPES = {"edit", "new", "log", "categorize"}
MIN_TS = datetime(2001, 1, 15, tzinfo=timezone.utc).timestamp()   # Wikipedia's birthday
TS_FMT = "%Y-%m-%d %H:%M:%S"


class ContractError(ValueError):
    pass


def _require(cond: bool, reason: str) -> None:
    if not cond:
        raise ContractError(reason)


def _is_int(v) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def to_silver_row(envelope: dict, source_file: str) -> dict:
    _require(isinstance(envelope, dict) and isinstance(envelope.get("event"), dict), "missing_event")
    evt = envelope["event"]

    event_id = evt.get("meta", {}).get("id") if isinstance(evt.get("meta"), dict) else None
    _require(isinstance(event_id, str) and event_id != "", "missing_event_id")
    _require(evt.get("wiki") == config.TARGET_WIKI, "wrong_wiki")
    _require(evt.get("type") in ALLOWED_TYPES, "unknown_type")
    _require(isinstance(evt.get("title"), str) and evt["title"].strip() != "", "missing_title")
    _require(isinstance(evt.get("user"), str) and evt["user"] != "", "missing_user")
    _require(_is_int(evt.get("namespace")), "bad_namespace")

    ts = evt.get("timestamp")
    now = datetime.now(timezone.utc).timestamp()
    _require(_is_int(ts) and MIN_TS <= ts <= now + 86_400, "bad_timestamp")
    event_dt = datetime.fromtimestamp(ts, tz=timezone.utc)

    try:
        ingested_dt = datetime.fromisoformat(envelope["ingested_at"]).astimezone(timezone.utc)
    except (KeyError, TypeError, ValueError):
        raise ContractError("bad_ingested_at")

    length = evt.get("length") if isinstance(evt.get("length"), dict) else {}
    old_len = length.get("old") if _is_int(length.get("old")) else None
    new_len = length.get("new") if _is_int(length.get("new")) else None
    if evt["type"] == "new" and new_len is not None:
        delta = new_len
    elif old_len is not None and new_len is not None:
        delta = new_len - old_len
    else:
        delta = None

    move_target = None
    if evt["type"] == "log" and evt.get("log_type") == "move":
        params = evt.get("log_params") if isinstance(evt.get("log_params"), dict) else {}
        move_target = params.get("target")
        _require(isinstance(move_target, str) and move_target.strip() != "", "move_without_target")

    comment = evt.get("comment") if isinstance(evt.get("comment"), str) else ""
    return {
        "event_id": event_id,
        "event_ts": event_dt.strftime(TS_FMT),
        "ingested_at": ingested_dt.strftime(TS_FMT),
        "lateness_s": max(0, int((ingested_dt - event_dt).total_seconds())),
        "event_type": evt["type"],
        "namespace": evt["namespace"],
        "title": evt["title"].strip(),
        "user_name": evt["user"],
        "is_bot": int(bool(evt.get("bot", False))),
        "is_minor": int(bool(evt.get("minor", False))),
        "length_old": old_len,
        "length_new": new_len,
        "bytes_delta": delta,
        "comment": comment,
        "is_revert": int(bool(config.REVERT_PATTERN.search(comment))),
        "log_type": evt.get("log_type") if isinstance(evt.get("log_type"), str) else None,
        "move_target": move_target.strip() if move_target else None,
        "source_file": source_file,
    }
