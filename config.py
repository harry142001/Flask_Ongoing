import json
import logging
import os

log = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DB_PATH = os.getenv("DB_PATH", os.path.join(BASE_DIR, "data", "Database1.db"))
DETAILS_DB_PATH = os.getenv("DETAILS_DB_PATH", os.path.join(BASE_DIR, "data", "property_details.db"))
TABLE = os.getenv("TABLE", "properties")

MOCK_OVERRIDES_PATH = os.path.join(BASE_DIR, "mock_overrides.json")

# MPAC comparable field schema — empty values populated at runtime
COMPARABLE_SCHEMA = {
    "property_location": "",
    "basement_finish_area": "",
    "compsaledate": "",
    "compsaleamount": "",
    "comparablesequence": "",
    "lot_depth": "",
    "lot_frontage": "",
    "municipality": "",
    "number_of_bedrooms": "",
    "number_of_full_bathrooms": "",
    "number_of_half_bathrooms": "",
    "postal_code": "",
    "property_type_style": "",
    "property_type_description": "",
    "province": "",
    "comp_roll": "",
    "lot_area": "",
    "unit_of_measure_of_lot_area": "",
    "primary_struc_area_above_grd": "",
    "year_built": "",
}


def load_mock_overrides() -> dict:
    if os.path.exists(MOCK_OVERRIDES_PATH):
        with open(MOCK_OVERRIDES_PATH, "r") as f:
            log.info("Loaded mock overrides from %s", MOCK_OVERRIDES_PATH)
            return json.load(f)
    log.info("No mock_overrides.json found, skipping")
    return {}


MOCK_OVERRIDES: dict = load_mock_overrides()
