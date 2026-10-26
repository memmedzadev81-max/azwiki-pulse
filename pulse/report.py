"""Console report on the gold layer."""
import sqlite3


def _table(rows, headers):
    rows = [[("" if v is None else str(v)) for v in r] for r in rows]
    widths = [max(len(h), *(len(r[i]) for r in rows)) if rows else len(h) for i, h in enumerate(headers)]
    line = "  ".join(h.ljust(w) for h, w in zip(headers, widths))
    print(line)
    print("-" * len(line))
    for r in rows:
        print("  ".join(v.ljust(w) for v, w in zip(r, widths)))
    if not rows:
        print("(none)")
    print()


def print_report(con: sqlite3.Connection) -> None:
    latest = con.execute("SELECT MAX(hour_ts) FROM agg_page_hourly").fetchone()[0]
    if not latest:
        print("No data yet. Run `python pulse_cli.py demo` or `stream` + `process` first.")
        return

    print(f"\n=== TRENDING PAGES (24 h up to {latest} UTC) ===")
    _table(con.execute(
        """SELECT p.title, SUM(a.edits), SUM(a.human_editors), SUM(a.reverts)
           FROM agg_page_hourly a JOIN dim_page p ON p.page_nk = a.page_nk AND p.is_current = 1
           WHERE a.hour_ts > datetime(?, '-24 hours')
           GROUP BY a.page_nk ORDER BY 2 DESC LIMIT 10""", (latest,)).fetchall(),
        ["page", "edits", "editor-hours", "reverts"])

    print("=== ALERTS ===")
    _table(con.execute("SELECT alert_type, hour_ts, title, metric_value, details FROM alerts "
                       "ORDER BY hour_ts DESC, alert_type").fetchall(),
           ["type", "hour (UTC)", "page", "score", "details"])

    print("=== PAGE RENAMES (dim_page SCD2) ===")
    _table(con.execute(
        """SELECT p.page_nk, p.title, p.valid_from, p.valid_to, p.change_reason
           FROM dim_page p WHERE p.page_nk IN (SELECT page_nk FROM dim_page GROUP BY page_nk HAVING COUNT(*) > 1)
           ORDER BY p.page_nk, p.valid_from""").fetchall(),
        ["page_nk", "title", "valid_from", "valid_to", "reason"])

    print("=== EDITOR TIER HISTORY (dim_user SCD2, editors with >1 version) ===")
    _table(con.execute(
        """SELECT user_name, tier, edits_at_change, valid_from, valid_to
           FROM dim_user WHERE user_name IN (SELECT user_name FROM dim_user GROUP BY user_name
                                             HAVING COUNT(*) > 1)
           ORDER BY user_name, valid_from LIMIT 15""").fetchall(),
        ["editor", "tier", "at edit #", "valid_from", "valid_to"])

    print("=== DATA HEALTH ===")
    _table(con.execute(
        """SELECT (SELECT COUNT(*) FROM silver_events),
                  (SELECT COALESCE(SUM(duplicates), 0) FROM etl_file_manifest),
                  (SELECT COUNT(*) FROM silver_dead_letter),
                  (SELECT COUNT(*) FROM fact_edit WHERE lateness_s > 3600),
                  (SELECT ROUND(100.0 * SUM(is_bot) / COUNT(*), 1) FROM fact_edit)""").fetchall(),
        ["silver events", "duplicates dropped", "dead letters", "late (>1h) edits", "bot share %"])

    print("=== DEAD LETTER REASONS ===")
    _table(con.execute("SELECT reason, COUNT(*) FROM silver_dead_letter GROUP BY reason ORDER BY 2 DESC")
           .fetchall(), ["reason", "count"])
