import logging
import sqlite3

from config import DB_PATH, DETAILS_DB_PATH, TABLE

log = logging.getLogger(__name__)


def connect() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def connect_details() -> sqlite3.Connection:
    con = sqlite3.connect(DETAILS_DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def create_indexes() -> None:
    try:
        with connect() as con:
            for col in ("city", "agent", "broker", "postal", "address", "state"):
                con.execute(f"CREATE INDEX IF NOT EXISTS idx_{col} ON {TABLE}({col})")
        log.info("DB indexes ready")
    except Exception as e:
        log.error("Index creation failed: %s", e)


# Detect which region column(s) exist so queries work against both schemas
with connect() as _con:
    _cols = {r["name"].lower() for r in _con.execute(f"PRAGMA table_info({TABLE})")}

HAS_STATE = "state" in _cols
HAS_PROVINCE = "province" in _cols
REGION_SQL = (
    "COALESCE(province, state)" if HAS_PROVINCE and HAS_STATE
    else ("province" if HAS_PROVINCE else "state")
)
