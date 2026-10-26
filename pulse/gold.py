"""SILVER -> GOLD

Processes new silver rows (seq > watermark) in event-time order and maintains:

  dim_page   SCD Type 2. A page move closes the old version and opens a new one
             with the same durable page_nk, so history stays connected across renames.
  dim_user   SCD Type 2. A new version whenever an editor's tier or bot flag changes.
  fact_edit  One row per edit, pointing to dimension versions valid at event time.

Late events never rewrite dimension history; they attach to the version valid at event time.
"""
import sqlite3
from datetime import datetime

from . import config

OPEN_END = "9999-12-31 23:59:59"


def get_state(con, key: str, default: str) -> str:
    row = con.execute("SELECT value FROM etl_state WHERE key = ?", (key,)).fetchone()
    return row[0] if row else default


def set_state(con, key: str, value: str) -> None:
    con.execute(
        "INSERT INTO etl_state (key, value) VALUES (?, ?) "
        "ON CONFLICT (key) DO UPDATE SET value = excluded.value",
        (key, value),
    )


def _new_page_nk(con) -> int:
    return con.execute("SELECT COALESCE(MAX(page_nk), 0) + 1 FROM dim_page").fetchone()[0]


def _insert_page(con, page_nk, title, ns, valid_from, reason) -> int:
    return con.execute(
        "INSERT INTO dim_page (page_nk, title, namespace, valid_from, valid_to, is_current, change_reason) "
        "VALUES (?, ?, ?, ?, ?, 1, ?)",
        (page_nk, title, ns, valid_from, OPEN_END, reason),
    ).lastrowid


def resolve_page(con, title: str, ns: int, ts: str) -> int:
    """Return the page_sk valid for title at time ts, creating the page if new."""
    row = con.execute(
        "SELECT page_sk FROM dim_page WHERE title = ? AND valid_from <= ? AND ? < valid_to "
        "ORDER BY valid_from DESC LIMIT 1",
        (title, ts, ts),
    ).fetchone()
    if row:
        return row[0]
    row = con.execute(
        "SELECT page_sk FROM dim_page WHERE title = ? AND is_current = 1", (title,)
    ).fetchone()
    if row:
        return row[0]
    row = con.execute(
        "SELECT page_sk FROM dim_page WHERE title = ? ORDER BY valid_to DESC LIMIT 1", (title,)
    ).fetchone()
    if row:
        return row[0]
    return _insert_page(con, _new_page_nk(con), title, ns, ts, "first_seen")


def apply_move(con, old_title: str, new_title: str, ns: int, ts: str) -> None:
    cur = con.execute(
        "SELECT page_sk, page_nk, valid_from FROM dim_page WHERE title = ? AND is_current = 1",
        (old_title,),
    ).fetchone()
    clash = con.execute(
        "SELECT page_sk FROM dim_page WHERE title = ? AND is_current = 1", (new_title,)
    ).fetchone()
    if clash:
        con.execute(
            "UPDATE dim_page SET valid_to = MAX(valid_from, ?), is_current = 0 WHERE page_sk = ?",
            (ts, clash[0]),
        )
    if cur is None:
        _insert_page(con, _new_page_nk(con), new_title, ns, ts, f"first_seen_via_move_from:{old_title}")
        return
    page_sk, page_nk, valid_from = cur
    close_at = max(ts, valid_from)
    con.execute("UPDATE dim_page SET valid_to = ?, is_current = 0 WHERE page_sk = ?", (close_at, page_sk))
    _insert_page(con, page_nk, new_title, ns, close_at, f"renamed_from:{old_title}")


def tier_for(edits: int) -> str:
    tier = config.TIERS[0][1]
    for threshold, name in config.TIERS:
        if edits >= threshold:
            tier = name
    return tier


def track_user(con, user: str, is_bot: int, ts: str) -> None:
    con.execute(
        "INSERT INTO user_edit_counter (user_name, edits) VALUES (?, 1) "
        "ON CONFLICT (user_name) DO UPDATE SET edits = edits + 1",
        (user,),
    )
    edits = con.execute("SELECT edits FROM user_edit_counter WHERE user_name = ?", (user,)).fetchone()[0]
    tier = tier_for(edits)
    cur = con.execute(
        "SELECT user_sk, is_bot, tier, valid_from FROM dim_user WHERE user_name = ? AND is_current = 1",
        (user,),
    ).fetchone()
    if cur and cur[1] == is_bot and cur[2] == tier:
        return
    start = ts
    if cur:
        start = max(ts, cur[3])
        con.execute("UPDATE dim_user SET valid_to = ?, is_current = 0 WHERE user_sk = ?", (start, cur[0]))
    con.execute(
        "INSERT INTO dim_user (user_name, is_bot, tier, edits_at_change, valid_from, valid_to, is_current) "
        "VALUES (?, ?, ?, ?, ?, ?, 1)",
        (user, is_bot, tier, edits, start, OPEN_END),
    )


def resolve_user(con, user: str, ts: str) -> int:
    row = con.execute(
        "SELECT user_sk FROM dim_user WHERE user_name = ? AND valid_from <= ? AND ? < valid_to "
        "ORDER BY valid_from DESC LIMIT 1",
        (user, ts, ts),
    ).fetchone()
    if row:
        return row[0]
    return con.execute(
        "SELECT user_sk FROM dim_user WHERE user_name = ? ORDER BY valid_from LIMIT 1", (user,)
    ).fetchone()[0]


def process_gold(con: sqlite3.Connection) -> dict:
    last_seq = int(get_state(con, "gold_last_seq", "0"))
    rows = con.execute(
        """SELECT seq, event_id, event_ts, event_type, namespace, title, user_name, is_bot,
                  is_minor, is_revert, bytes_delta, lateness_s, log_type, move_target
           FROM silver_events WHERE seq > ? ORDER BY event_ts, seq""",
        (last_seq,),
    ).fetchall()

    affected: set[tuple[str, int]] = set()
    facts = 0
    max_seq = last_seq
    for (seq, event_id, ts, etype, ns, title, user, is_bot, is_minor, is_revert,
         delta, lateness, log_type, move_target) in rows:
        max_seq = max(max_seq, seq)
        if etype == "log" and log_type == "move" and move_target:
            apply_move(con, title, move_target, ns, ts)
            continue
        if etype not in ("edit", "new"):
            continue
        page_sk = resolve_page(con, title, ns, ts)
        track_user(con, user, is_bot, ts)
        user_sk = resolve_user(con, user, ts)
        hour_ts = ts[:13] + ":00:00"
        cur = con.execute(
            """INSERT OR IGNORE INTO fact_edit
               (event_id, event_ts, hour_ts, date_key, page_sk, user_sk, namespace, is_new_page,
                is_minor, is_bot, is_revert, bytes_delta, lateness_s)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (event_id, ts, hour_ts, int(ts[:10].replace("-", "")), page_sk, user_sk, ns,
             int(etype == "new"), is_minor, is_bot, is_revert, delta, lateness),
        )
        facts += cur.rowcount
        page_nk = con.execute("SELECT page_nk FROM dim_page WHERE page_sk = ?", (page_sk,)).fetchone()[0]
        affected.add((hour_ts, page_nk))

    set_state(con, "gold_last_seq", str(max_seq))
    rebuild_hourly(con, affected)
    alerts = detect_alerts(con, affected)
    return {"silver_rows_read": len(rows), "fact_rows": facts,
            "hour_page_buckets": len(affected), "alerts": alerts}


def rebuild_hourly(con, affected: set[tuple[str, int]]) -> None:
    if not affected:
        return
    con.execute("DROP TABLE IF EXISTS _affected")
    con.execute("CREATE TEMP TABLE _affected (hour_ts TEXT, page_nk INTEGER, PRIMARY KEY (hour_ts, page_nk))")
    con.executemany("INSERT INTO _affected VALUES (?, ?)", sorted(affected))
    con.execute("DELETE FROM agg_page_hourly WHERE (hour_ts, page_nk) IN (SELECT hour_ts, page_nk FROM _affected)")
    con.execute(
        """INSERT INTO agg_page_hourly
           (hour_ts, page_nk, edits, human_edits, bot_edits, human_editors, reverts, bytes_added, bytes_removed)
           SELECT f.hour_ts, p.page_nk,
                  COUNT(*),
                  SUM(f.is_bot = 0),
                  SUM(f.is_bot = 1),
                  COUNT(DISTINCT CASE WHEN f.is_bot = 0 THEN f.user_sk_name END),
                  SUM(f.is_revert),
                  COALESCE(SUM(CASE WHEN f.bytes_delta > 0 THEN f.bytes_delta END), 0),
                  COALESCE(-SUM(CASE WHEN f.bytes_delta < 0 THEN f.bytes_delta END), 0)
           FROM (SELECT fe.*, u.user_name AS user_sk_name FROM fact_edit fe
                 JOIN dim_user u ON u.user_sk = fe.user_sk) f
           JOIN dim_page p  ON p.page_sk = f.page_sk
           JOIN _affected a ON a.hour_ts = f.hour_ts AND a.page_nk = p.page_nk
           GROUP BY f.hour_ts, p.page_nk"""
    )
    con.execute("DROP TABLE _affected")


def _current_title(con, page_nk: int) -> str:
    return con.execute(
        "SELECT title FROM dim_page WHERE page_nk = ? ORDER BY is_current DESC, valid_to DESC LIMIT 1",
        (page_nk,),
    ).fetchone()[0]


def detect_alerts(con, affected: set[tuple[str, int]]) -> int:
    now = datetime.now().isoformat(timespec="seconds")
    found = 0
    first_hour = con.execute("SELECT MIN(hour_ts) FROM agg_page_hourly").fetchone()[0]
    for hour_ts, page_nk in sorted(affected):
        agg = con.execute(
            "SELECT edits, human_edits, human_editors, reverts FROM agg_page_hourly WHERE hour_ts = ? AND page_nk = ?",
            (hour_ts, page_nk),
        ).fetchone()
        if not agg:
            continue
        edits, human_edits, human_editors, reverts = agg
        title = _current_title(con, page_nk)

        hist = dict(con.execute(
            """SELECT hour_ts, edits FROM agg_page_hourly
               WHERE page_nk = ? AND hour_ts < ? AND hour_ts >= datetime(?, ?)""",
            (page_nk, hour_ts, hour_ts, f"-{config.SPIKE_BASELINE_HOURS} hours"),
        ).fetchall())
        n = config.SPIKE_BASELINE_HOURS
        values = list(hist.values()) + [0] * (n - len(hist))
        mean = sum(values) / n
        std = (sum((v - mean) ** 2 for v in values) / n) ** 0.5
        z = (edits - mean) / max(std, 1.0)
        history_h = (datetime.fromisoformat(hour_ts) - datetime.fromisoformat(first_hour)).total_seconds() / 3600
        is_spike = (
            edits >= config.SPIKE_MIN_EDITS
            and z >= config.SPIKE_Z_SCORE
            and history_h >= config.SPIKE_MIN_HISTORY_HOURS
        )
        found += _upsert_alert(
            con, "spike", page_nk, hour_ts, title, round(z, 2),
            f"{edits} edits vs baseline {mean:.2f}±{std:.2f}/h", now, is_spike,
        )

        is_war = (
            human_edits >= config.EDIT_WAR_MIN_EDITS
            and human_editors >= config.EDIT_WAR_MIN_USERS
            and reverts >= config.EDIT_WAR_MIN_REVERTS
        )
        found += _upsert_alert(
            con, "edit_war", page_nk, hour_ts, title, reverts,
            f"{human_edits} human edits by {human_editors} editors, {reverts} reverts",
            now, is_war,
        )
    return found


def _upsert_alert(con, kind, page_nk, hour_ts, title, value, details, now, active: bool) -> int:
    if not active:
        con.execute(
            "DELETE FROM alerts WHERE alert_type = ? AND page_nk = ? AND hour_ts = ?",
            (kind, page_nk, hour_ts),
        )
        return 0
    con.execute(
        """INSERT INTO alerts (alert_type, page_nk, hour_ts, title, metric_value, details, detected_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT (alert_type, page_nk, hour_ts) DO UPDATE SET
               title = excluded.title, metric_value = excluded.metric_value, details = excluded.details""",
        (kind, page_nk, hour_ts, title, value, details, now),
    )
    return 1
