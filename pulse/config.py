"""Central configuration."""
import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"
CHECKPOINT_PATH = DATA_DIR / "checkpoint.json"
WAREHOUSE_PATH = DATA_DIR / "warehouse.db"
SQL_DIR = PROJECT_ROOT / "sql"

# ---- Source: Wikimedia EventStreams (Server-Sent Events, public, no key) ----
STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
TARGET_WIKI = "azwiki"                      # Azerbaijani Wikipedia
# Wikimedia asks every client to send a descriptive User-Agent with contact info.
USER_AGENT = "azwiki-pulse/1.0 (data engineering portfolio project; contact: your-email@example.com)"

# ---- Micro-batching ----
BATCH_MAX_EVENTS = 200        # flush a bronze file after this many events ...
BATCH_MAX_SECONDS = 60        # ... or after this many seconds, whichever comes first

# ---- Editor tiers (drive SCD Type 2 history in dim_user) ----
TIERS = [(0, "newcomer"), (10, "active"), (100, "power")]   # cumulative edits >= threshold

# ---- Alert rules ----
SPIKE_MIN_EDITS = 8           # an hour needs at least this many edits to be a spike
SPIKE_MIN_HISTORY_HOURS = 6   # no spike alerts until the pipeline has this much history
SPIKE_Z_SCORE = 3.0           # and must be this many std-devs above the page's baseline
SPIKE_BASELINE_HOURS = 24     # baseline window (missing hours count as zero)
EDIT_WAR_MIN_EDITS = 4        # human edits on one page within one hour
EDIT_WAR_MIN_USERS = 2
EDIT_WAR_MIN_REVERTS = 2

# Edit summaries that indicate a revert (English + Azerbaijani wording).
REVERT_PATTERN = re.compile(r"(revert|undo|\brv\b|geri qaytar|ləğv|legv)", re.IGNORECASE)
