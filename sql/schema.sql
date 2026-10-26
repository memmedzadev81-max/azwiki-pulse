-- =====================================================================
-- ETL metadata
-- =====================================================================
CREATE TABLE IF NOT EXISTS etl_runs (
    run_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at   TEXT NOT NULL,
    finished_at  TEXT,
    status       TEXT NOT NULL,              -- running / success / failed
    files        INTEGER DEFAULT 0,
    silver_rows  INTEGER DEFAULT 0,
    duplicates   INTEGER DEFAULT 0,
    dead_letters INTEGER DEFAULT 0,
    fact_rows    INTEGER DEFAULT 0,
    alerts       INTEGER DEFAULT 0,
    message      TEXT
);

-- Which bronze files are already processed (file-level incremental load).
CREATE TABLE IF NOT EXISTS etl_file_manifest (
    file_path     TEXT PRIMARY KEY,
    run_id        INTEGER NOT NULL,
    processed_at  TEXT NOT NULL,
    lines_read    INTEGER NOT NULL,
    rows_inserted INTEGER NOT NULL,
    duplicates    INTEGER NOT NULL,
    dead_letters  INTEGER NOT NULL
);

-- Key/value state, e.g. the last silver sequence number processed into gold.
CREATE TABLE IF NOT EXISTS etl_state (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- =====================================================================
-- SILVER
-- =====================================================================
CREATE TABLE IF NOT EXISTS silver_events (
    seq          INTEGER PRIMARY KEY AUTOINCREMENT,  -- arrival order
    event_id     TEXT NOT NULL UNIQUE,               -- meta.id -> de-duplication key
    event_ts     TEXT NOT NULL,                      -- when it happened (UTC)
    ingested_at  TEXT NOT NULL,                      -- when we received it (UTC)
    lateness_s   INTEGER NOT NULL,
    event_type   TEXT NOT NULL,                      -- edit / new / log / categorize
    namespace    INTEGER NOT NULL,
    title        TEXT NOT NULL,
    user_name    TEXT NOT NULL,
    is_bot       INTEGER NOT NULL,
    is_minor     INTEGER NOT NULL,
    length_old   INTEGER,
    length_new   INTEGER,
    bytes_delta  INTEGER,
    comment      TEXT,
    is_revert    INTEGER NOT NULL,
    log_type     TEXT,
    move_target  TEXT,
    source_file  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_silver_ts ON silver_events (event_ts);

-- Records that broke the data contract are quarantined, never silently dropped.
CREATE TABLE IF NOT EXISTS silver_dead_letter (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file    TEXT NOT NULL,
    line_no        INTEGER NOT NULL,
    reason         TEXT NOT NULL,
    raw            TEXT NOT NULL,
    quarantined_at TEXT NOT NULL
);

-- =====================================================================
-- GOLD
-- =====================================================================
-- SCD Type 2: a page keeps one stable page_nk while its title changes.
CREATE TABLE IF NOT EXISTS dim_page (
    page_sk       INTEGER PRIMARY KEY AUTOINCREMENT,  -- surrogate key (one per version)
    page_nk       INTEGER NOT NULL,                   -- durable key (same across renames)
    title         TEXT NOT NULL,
    namespace     INTEGER NOT NULL,
    valid_from    TEXT NOT NULL,
    valid_to      TEXT NOT NULL,                      -- '9999-12-31 23:59:59' = open
    is_current    INTEGER NOT NULL,
    change_reason TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_page_current_title ON dim_page (title) WHERE is_current = 1;
CREATE UNIQUE INDEX IF NOT EXISTS ux_page_current_nk    ON dim_page (page_nk) WHERE is_current = 1;
CREATE INDEX        IF NOT EXISTS ix_page_title         ON dim_page (title);

-- SCD Type 2: a new version whenever an editor's tier or bot flag changes.
CREATE TABLE IF NOT EXISTS dim_user (
    user_sk         INTEGER PRIMARY KEY AUTOINCREMENT,
    user_name       TEXT NOT NULL,
    is_bot          INTEGER NOT NULL,
    tier            TEXT NOT NULL,                    -- newcomer / active / power
    edits_at_change INTEGER NOT NULL,
    valid_from      TEXT NOT NULL,
    valid_to        TEXT NOT NULL,
    is_current      INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_user_current ON dim_user (user_name) WHERE is_current = 1;
CREATE INDEX        IF NOT EXISTS ix_user_name    ON dim_user (user_name);

CREATE TABLE IF NOT EXISTS user_edit_counter (
    user_name TEXT PRIMARY KEY,
    edits     INTEGER NOT NULL
);

-- One row per edit; keys point to the dimension version valid at event time.
CREATE TABLE IF NOT EXISTS fact_edit (
    event_id     TEXT PRIMARY KEY,
    event_ts     TEXT NOT NULL,
    hour_ts      TEXT NOT NULL,
    date_key     INTEGER NOT NULL,
    page_sk      INTEGER NOT NULL REFERENCES dim_page (page_sk),
    user_sk      INTEGER NOT NULL REFERENCES dim_user (user_sk),
    namespace    INTEGER NOT NULL,
    is_new_page  INTEGER NOT NULL,
    is_minor     INTEGER NOT NULL,
    is_bot       INTEGER NOT NULL,
    is_revert    INTEGER NOT NULL,
    bytes_delta  INTEGER,
    lateness_s   INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_fact_hour ON fact_edit (hour_ts);

CREATE TABLE IF NOT EXISTS agg_page_hourly (
    hour_ts        TEXT    NOT NULL,
    page_nk        INTEGER NOT NULL,
    edits          INTEGER NOT NULL,
    human_edits    INTEGER NOT NULL,
    bot_edits      INTEGER NOT NULL,
    human_editors  INTEGER NOT NULL,
    reverts        INTEGER NOT NULL,
    bytes_added    INTEGER NOT NULL,
    bytes_removed  INTEGER NOT NULL,
    PRIMARY KEY (hour_ts, page_nk)
);

CREATE TABLE IF NOT EXISTS alerts (
    alert_type   TEXT    NOT NULL,                    -- spike / edit_war
    page_nk      INTEGER NOT NULL,
    hour_ts      TEXT    NOT NULL,
    title        TEXT    NOT NULL,
    metric_value REAL    NOT NULL,
    details      TEXT    NOT NULL,
    detected_at  TEXT    NOT NULL,
    PRIMARY KEY (alert_type, page_nk, hour_ts)
);

-- Convenient reporting view: every edit with the page title valid at that time.
CREATE VIEW IF NOT EXISTS vw_edits AS
SELECT f.*, p.page_nk, p.title AS title_at_edit_time, u.user_name, u.tier AS tier_at_edit_time
FROM fact_edit f
JOIN dim_page p ON p.page_sk = f.page_sk
JOIN dim_user u ON u.user_sk = f.user_sk;
