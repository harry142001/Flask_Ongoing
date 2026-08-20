import json
import logging
import os

log = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DB_PATH = os.getenv("DB_PATH", os.path.join(BASE_DIR, "data", "Database1.db"))
DETAILS_DB_PATH = os.getenv("DETAILS_DB_PATH", os.path.join(BASE_DIR, "data", "property_details.db"))
TABLE = os.getenv("TABLE", "properties")

MOCK_OVERRIDES_PATH = os.path.join(BASE_DIR, "mock_overrides.json")

# Comparable sale field schema (matches the Teranet/GeoWarehouse report format) —
# empty values populated at runtime
COMPARABLE_SCHEMA = {
    "address": "",
    "date": "",
    "sale_price": "",
    "lot_size_sqft": "",
    "price_per_sqft": "",
    "distance_m": "",
    "pin": "",
}


def load_mock_overrides() -> dict:
    if os.path.exists(MOCK_OVERRIDES_PATH):
        with open(MOCK_OVERRIDES_PATH, "r") as f:
            log.info("Loaded mock overrides from %s", MOCK_OVERRIDES_PATH)
            return json.load(f)
    log.info("No mock_overrides.json found, skipping")
    return {}


MOCK_OVERRIDES: dict = load_mock_overrides()
