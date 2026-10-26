"""Synthetic event generator (offline demo + tests).

Produces bronze files in exactly the format the live consumer writes, with
realistic Azerbaijani Wikipedia activity AND the kinds of problems a real
stream has, so every part of the pipeline gets exercised:

  scenario                         what it tests
  -------------------------------  ---------------------------------------------
  football match -> edit spike     spike detection (z-score vs baseline)
  two editors reverting each other edit-war detection
  page renamed mid-stream          SCD Type 2 history in dim_page
  one very active editor           SCD Type 2 tier changes in dim_user
  bot account                      bot vs human metrics
  ~2% duplicate deliveries         idempotent de-duplication
  ~3% late-arriving events         event-time vs ingestion-time handling
  ~0.5% malformed events           data contract + dead-letter queue
"""
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .bronze import BronzeWriter

PAGES = [
    "Bakı", "Gəncə şəhəri", "Şuşa", "Xəzər dənizi", "Nizami Gəncəvi", "Füzuli (şair)",
    "Qobustan Milli Parkı", "İçərişəhər", "Muğam", "Novruz bayramı", "Plov",
    "Neftçi (futbol klubu)", "Qarabağ (futbol klubu)", "Azərbaycan dili", "Dolma",
    "Üzeyir Hacıbəyov", "Xan sarayı (Şəki)", "Göygöl", "Lənkəran", "Naxçıvan",
    "Kür çayı", "Tar (musiqi aləti)", "Kəlağayı", "Yanardağ", "Abşeron yarımadası",
    "Mirzə Fətəli Axundzadə", "Xalça", "Şahdağ", "Qəbələ", "Bakı metropoliteni",
]
HUMANS = [f"İstifadəçi{n}" for n in range(1, 21)] + ["Aysel_88", "Murad.K", "Leyla2001", "Tural_az"]
BOTS = ["AzBot", "InternetArchiveBot"]
NORMAL_COMMENTS = ["düzəliş", "mənbə əlavə edildi", "orfoqrafiya", "kateqoriya", "məlumat yeniləndi", ""]


def _event(rng, ts: datetime, title: str, user: str, *, etype="edit", bot=False,
           comment=None, delta=None, offset=0) -> dict:
    old_len = rng.randint(2_000, 60_000)
    delta = delta if delta is not None else rng.randint(-300, 900)
    evt = {
        "$schema": "/mediawiki/recentchange/1.0.0",
        "meta": {"id": str(uuid.UUID(int=rng.getrandbits(128))), "dt": ts.isoformat(),
                 "domain": "az.wikipedia.org", "stream": "mediawiki.recentchange",
                 "partition": 0, "offset": offset},
        "id": rng.randint(10_000_000, 99_999_999),
        "type": etype, "namespace": 0, "title": title,
        "comment": comment if comment is not None else rng.choice(NORMAL_COMMENTS),
        "timestamp": int(ts.timestamp()), "user": user, "bot": bot,
        "minor": rng.random() < 0.3, "server_name": "az.wikipedia.org", "wiki": "azwiki",
    }
    if etype in ("edit", "new"):
        evt["length"] = {"old": None if etype == "new" else old_len, "new": old_len + delta}
        evt["revision"] = {"old": None if etype == "new" else rng.randint(1, 9_000_000),
                           "new": rng.randint(9_000_001, 9_999_999)}
    return evt


def generate_events(start: datetime, hours: int = 36, seed: int = 42) -> list[tuple[datetime, dict]]:
    """Returns (ingested_at, event) pairs, already including dirty data."""
    rng = random.Random(seed)
    out: list[tuple[datetime, dict]] = []
    rename_hour, spike_hour, war_hour = hours // 3, hours - 6, hours - 10
    offset = 0

    def add(ts, evt, lag_s=None):
        nonlocal offset
        offset += 1
        evt["meta"]["offset"] = offset
        lag = timedelta(seconds=lag_s if lag_s is not None else rng.randint(1, 20))
        out.append((ts + lag, evt))

    for h in range(hours):
        hour_start = start + timedelta(hours=h)
        pages = [("Gəncə" if p == "Gəncə şəhəri" and h >= rename_hour else p) for p in PAGES]

        for _ in range(rng.randint(25, 45)):
            ts = hour_start + timedelta(seconds=rng.randint(0, 3599))
            if rng.random() < 0.15:
                add(ts, _event(rng, ts, rng.choice(pages), rng.choice(BOTS), bot=True))
            else:
                add(ts, _event(rng, ts, rng.choice(pages), rng.choice(HUMANS)))

        for _ in range(4):
            ts = hour_start + timedelta(seconds=rng.randint(0, 3599))
            add(ts, _event(rng, ts, rng.choice(pages), "Aysel_88"))

        if rng.random() < 0.3:
            ts = hour_start + timedelta(seconds=rng.randint(0, 3599))
            add(ts, _event(rng, ts, f"Yeni məqalə {h}", rng.choice(HUMANS), etype="new", delta=1500))

        if h == rename_hour:
            ts = hour_start + timedelta(seconds=5)
            evt = _event(rng, ts, "Gəncə şəhəri", "Murad.K", etype="log",
                         comment="ad standartlaşdırıldı")
            evt.update(log_type="move", log_action="move",
                       log_params={"target": "Gəncə", "noredir": "0"})
            add(ts, evt)

        if h == spike_hour:
            for _ in range(30):
                ts = hour_start + timedelta(seconds=rng.randint(0, 3599))
                add(ts, _event(rng, ts, "Neftçi (futbol klubu)", rng.choice(HUMANS),
                               comment="oyunun nəticəsi"))

        if h == war_hour:
            for i in range(7):
                ts = hour_start + timedelta(minutes=5 + i * 6)
                user = "Leyla2001" if i % 2 == 0 else "Tural_az"
                comment = "məlumat əlavə edildi" if i == 0 else "Redaktə geri qaytarıldı (revert)"
                add(ts, _event(rng, ts, "Xəzər dənizi", user, comment=comment,
                               delta=(420 if i % 2 == 0 else -420)))

    n = len(out)
    for i in rng.sample(range(n), k=max(1, n * 3 // 100)):
        ingested, evt = out[i]
        out[i] = (ingested + timedelta(hours=rng.randint(1, 3)), evt)
    for i in rng.sample(range(n), k=max(1, n * 2 // 100)):
        ingested, evt = out[i]
        out.append((ingested + timedelta(minutes=rng.randint(1, 30)), dict(evt)))
    for i in rng.sample(range(n), k=max(2, n // 200)):
        ingested, evt = out[i]
        bad = dict(evt, meta=dict(evt["meta"], id=str(uuid.UUID(int=rng.getrandbits(128)))))
        if rng.random() < 0.5:
            bad.pop("title")
        else:
            bad["timestamp"] = "not-a-timestamp"
        out.append((ingested, bad))

    out.sort(key=lambda pair: pair[0])
    return out


def write_sample_bronze(bronze_dir: Path | None = None, start: datetime | None = None,
                        hours: int = 36, seed: int = 42, file_minutes: int = 10) -> list[Path]:
    start = start or datetime(2026, 10, 20, 0, 0, tzinfo=timezone.utc)
    writer = BronzeWriter(bronze_dir)
    events = generate_events(start, hours, seed)
    files, batch, window_end = [], [], None
    for ingested, evt in events:
        if window_end is None:
            window_end = ingested + timedelta(minutes=file_minutes)
        if ingested >= window_end:
            files.append(writer.write_batch(batch, now=window_end))
            batch, window_end = [], ingested + timedelta(minutes=file_minutes)
        batch.append({"ingested_at": ingested.isoformat(),
                      "sse_id": f'[{{ "partition":0,"offset":{evt["meta"]["offset"]} }}]',
                      "event": evt})
    if batch:
        files.append(writer.write_batch(batch, now=window_end))
    return files
