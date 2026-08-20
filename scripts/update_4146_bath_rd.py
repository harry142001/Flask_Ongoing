"""
Admin script: replaces the placeholder property_details row for 4146 Bath Rd, Kingston
with real data from the GeoWarehouse/Teranet report (PIN 361260510, generated 2026-07-29).

Usage:
    python scripts/update_4146_bath_rd.py
"""
import json
import os
import sqlite3

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "property_details.db")

SALES_HISTORY = [
    {"date": "2012-08-10", "price": 220000, "type": "Transfer"},
    {"date": "1991-05-28", "price": 97, "type": "Transfer"},
    {"date": "1990-04-09", "price": 62000, "type": "Transfer"},
]

# Closest 4 comparables from the report's 9, by distance
COMPARABLES = [
    {"address": "1399 Tamarac St, Kingston, K7M7J2", "date": "2026-05-01", "sale_price": 1240000,
     "lot_size_sqft": 10333.0, "price_per_sqft": 120, "distance_m": 201, "pin": "361260418"},
    {"address": "578 Sycamore St, Kingston, K7M7L8", "date": "2026-05-04", "sale_price": 840000,
     "lot_size_sqft": 8428.0, "price_per_sqft": 100, "distance_m": 351, "pin": "361260478"},
    {"address": "4205 Bath Rd, Kingston, K7M4Y8", "date": "2026-02-12", "sale_price": 1140000,
     "lot_size_sqft": 479456.0, "price_per_sqft": 2, "distance_m": 414, "pin": "361260648"},
    {"address": "4220 Bath Rd, Kingston, K7M4Y7", "date": "2026-04-30", "sale_price": 870000,
     "lot_size_sqft": 13659.0, "price_per_sqft": 64, "distance_m": 509, "pin": "361260386"},
]

TAX_HISTORY = [
    {"year": 2022, "tax_estimate": 12726},
    {"year": 2023, "tax_estimate": 13200},
    {"year": 2024, "tax_estimate": 13610},
    {"year": 2025, "tax_estimate": 14347},
]

ASSESSED_VALUE_HISTORY = [
    {"year": 2016, "phased_in_assessment": 946000},
    {"year": 2017, "phased_in_assessment": 952250},
    {"year": 2018, "phased_in_assessment": 958500},
    {"year": 2019, "phased_in_assessment": 964750},
    {"year": 2020, "phased_in_assessment": 971000},
    {"year": 2021, "phased_in_assessment": 971000},
    {"year": 2022, "phased_in_assessment": 971000},
    {"year": 2023, "phased_in_assessment": 971000},
    {"year": 2024, "phased_in_assessment": 971000},
    {"year": 2025, "phased_in_assessment": 971000},
    {"year": 2026, "phased_in_assessment": 971000},
]

SITE_FEATURES = (
    "Official Plan: Natural Heritage System; Topography: Steep Slope; "
    "Easement on Property; Traffic Pattern: Medium; Waterfront: Lake; "
    "Shoreline: Rocky, Shallow; Exposure: South; Permanent Docking"
)

UPDATE_FIELDS = {
    "pin": "361260510",
    "arn": "101108013211100",
    "land_registry_office": "FRONTENAC (13)",
    "land_registry_status": "Active",
    "registration_type": "Certified (Land Titles)",
    "ownership_type": "Freehold",
    "legal_description": (
        "LT 111, PL 1860, PT BLK 126, PL 1860, PT 9, 13R5888; PT MILE SQUARE LT, "
        "PT 8, 13R5888 (FORMERLY PT HWY 33, CLOSED BY ORDER IN COUNCIL FR398232) ; "
        "S/T FR552527 ; S/T FR539015 KINGSTON TOWNSHIP"
    ),
    "area_sqft": 8589.59,
    "area_acres": 0.197,
    "perimeter_ft": 429.79,
    "frontage_ft": 156.62,
    "depth_ft": None,
    "assessed_value": 971000,
    "valuation_date": "2016-01-01",
    "property_code": 313,
    "property_description": "Single family detached on water",
    "year_built": 2015,
    "bedrooms": 3,
    "full_bathrooms": 4,
    "half_bathrooms": 1,
    "storeys": 2,
    "fireplace_total": 2,
    "garage_type": "Attached Garage, Basement Garage",
    "garage_spaces": 3,
    "indoor_pool": "N",
    "outdoor_pool": "N",
    "zoning": "EPA, R1-17",
    "driveway": "Separate or Private Driveway",
    "variance": "Irregular",
    "site_features": SITE_FEATURES,
    "sales_history": json.dumps(SALES_HISTORY),
    "comparables": json.dumps(COMPARABLES),
    "tax_history": json.dumps(TAX_HISTORY),
    "assessed_value_history": json.dumps(ASSESSED_VALUE_HISTORY),
}

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

row = cur.execute(
    "SELECT id FROM property_details WHERE LOWER(TRIM(address)) = ?", ("4146 bath rd",)
).fetchone()

if not row:
    raise SystemExit("No property_details row found for '4146 Bath Rd' — check the address value.")

row_id = row[0]
set_clause = ", ".join(f"{col} = ?" for col in UPDATE_FIELDS)
cur.execute(
    f"UPDATE property_details SET {set_clause} WHERE id = ?",
    (*UPDATE_FIELDS.values(), row_id),
)
con.commit()

print(f"Updated property_details row id={row_id} for 4146 Bath Rd with real Teranet data.")
con.close()
