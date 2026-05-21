import logging
from typing import Any, Dict

from config import COMPARABLE_SCHEMA, DB_PATH, DETAILS_DB_PATH, TABLE
from database import connect, connect_details

log = logging.getLogger(__name__)

CACHE: Dict[str, Any] = {
    "properties": [],
    "property_details": {},
    "loaded": False,
}


def load_cache() -> None:
    global CACHE
    log.info("Using DB path: %s", DB_PATH)
    log.info("Loading data into memory...")

    try:
        with connect() as con:
            rows = con.execute(f"SELECT rowid AS id, * FROM {TABLE}").fetchall()
            CACHE["properties"] = [dict(r) for r in rows]
        log.info("Loaded %d properties", len(CACHE["properties"]))
    except Exception as e:
        log.error("Failed to load properties: %s", e)
        CACHE["properties"] = []

    try:
        main_addrs = {
            (r.get("address") or "").lower().strip()
            for r in CACHE["properties"]
        }
        with connect_details() as con:
            rows = con.execute("SELECT * FROM property_details").fetchall()
            for r in rows:
                d = dict(r)
                addr = d.get("address", "").lower().strip()
                if addr:
                    CACHE["property_details"][addr] = d
                    # Records in details DB but not in main DB become synthetic entries
                    if addr not in main_addrs:
                        synthetic = {
                            "id": None,
                            "address": d.get("address", ""),
                            "city": d.get("city", ""),
                            "agent": "",
                            "broker": "",
                            "price": "",
                            "latitude": d.get("latitude", ""),
                            "longitude": d.get("longitude", ""),
                            "province": d.get("province", "") or d.get("state", ""),
                            "postcode": (
                                d.get("postal_code", "")
                                or d.get("postcode", "")
                                or d.get("postal", "")
                            ),
                        }
                        CACHE["properties"].append(synthetic)
        log.info("Loaded %d property details", len(CACHE["property_details"]))
    except Exception as e:
        log.error("Failed to load property details: %s", e)
        CACHE["property_details"] = {}

    CACHE["loaded"] = True
    log.info("Cache ready")
