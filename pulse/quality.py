"""Data quality gate. Each check returns the number of violating rows."""
import sqlite3


class DataQualityError(Exception):
    pass


CHECKS = {
    # --- SCD Type 2 integrity -------------------------------------------------
    "dim_page: exactly one current version per page_nk": """
        SELECT COUNT(*) FROM (SELECT page_nk FROM dim_page GROUP BY page_nk
                              HAVING SUM(is_current) <> 1)""",
    "dim_page: no overlapping validity intervals": """
        SELECT COUNT(*) FROM dim_page a JOIN dim_page b
          ON a.page_nk = b.page_nk AND a.page_sk < b.page_sk
         AND a.valid_from < b.valid_to AND b.valid_from < a.valid_to""",
    "dim_page: valid_from <= valid_to": "SELECT COUNT(*) FROM dim_page WHERE valid_from > valid_to",
    "dim_user: exactly one current version per user": """
        SELECT COUNT(*) FROM (SELECT user_name FROM dim_user GROUP BY user_name
                              HAVING SUM(is_current) <> 1)""",
    "dim_user: no overlapping validity intervals": """
        SELECT COUNT(*) FROM dim_user a JOIN dim_user b
          ON a.user_name = b.user_name AND a.user_sk < b.user_sk
         AND a.valid_from < b.valid_to AND b.valid_from < a.valid_to""",
    # --- referential integrity --------------------------------------------------
    "fact_edit: page_sk exists": """
        SELECT COUNT(*) FROM fact_edit f LEFT JOIN dim_page p ON p.page_sk = f.page_sk
        WHERE p.page_sk IS NULL""",
    "fact_edit: user_sk exists": """
        SELECT COUNT(*) FROM fact_edit f LEFT JOIN dim_user u ON u.user_sk = f.user_sk
        WHERE u.user_sk IS NULL""",
    # --- reconciliation between layers ---------------------------------------------
    "silver edits/new == fact_edit rows": """
        SELECT ABS((SELECT COUNT(*) FROM silver_events WHERE event_type IN ('edit', 'new')
                    AND seq <= (SELECT CAST(value AS INTEGER) FROM etl_state WHERE key = 'gold_last_seq'))
                 - (SELECT COUNT(*) FROM fact_edit))""",
    "agg_page_hourly total == fact_edit rows": """
        SELECT ABS(COALESCE((SELECT SUM(edits) FROM agg_page_hourly), 0)
                 - (SELECT COUNT(*) FROM fact_edit))""",
    "agg_page_hourly: human + bot == edits": """
        SELECT COUNT(*) FROM agg_page_hourly WHERE human_edits + bot_edits <> edits""",
}


def run_checks(con: sqlite3.Connection) -> list[tuple[str, int]]:
    results = [(name, con.execute(sql).fetchone()[0]) for name, sql in CHECKS.items()]
    failed = [(n, c) for n, c in results if c]
    if failed:
        raise DataQualityError("; ".join(f"{n} -> {c}" for n, c in failed))
    return results
