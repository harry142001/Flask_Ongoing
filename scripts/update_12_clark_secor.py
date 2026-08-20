"""
Admin script: replaces the placeholder property_details row for 12 Clark Secor Pl,
Scarborough with real data from the GeoWarehouse/Teranet report (PIN 065020114,
generated 2026-08-07).

Note: this report has no Comparable Sales section, so comparables is left empty.

Usage:
    python scripts/update_12_clark_secor.py
"""
import json
import os
import sqlite3

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "property_details.db")

SALES_HISTORY = [
    {"date": "2016-06-30", "price": 960000, "type": "Transfer"},
    {"date": "1981-09-18", "price": 0, "type": "Transfer"},
]

TAX_HISTORY = [
    {"year": 2016, "tax_estimate": 4087},
    {"year": 2017, "tax_estimate": 4345},
    {"year": 2018, "tax_estimate": 4572},
    {"year": 2019, "tax_estimate": 4809},
    {"year": 2020, "tax_estimate": 5068},
    {"year": 2021, "tax_estimate": 5163},
    {"year": 2022, "tax_estimate": 5340},
    {"year": 2023, "tax_estimate": 5630},
    {"year": 2024, "tax_estimate": 6044},
    {"year": 2025, "tax_estimate": 6372},
]

ASSESSED_VALUE_HISTORY = [
    {"year": 2016, "phased_in_assessment": 594000},
    {"year": 2017, "phased_in_assessment": 656750},
    {"year": 2018, "phased_in_assessment": 719500},
    {"year": 2019, "phased_in_assessment": 782250},
    {"year": 2020, "phased_in_assessment": 845000},
    {"year": 2021, "phased_in_assessment": 845000},
    {"year": 2022, "phased_in_assessment": 845000},
    {"year": 2023, "phased_in_assessment": 845000},
    {"year": 2024, "phased_in_assessment": 845000},
    {"year": 2025, "phased_in_assessment": 845000},
    {"year": 2026, "phased_in_assessment": 845000},
]

SITE_FEATURES = "Official Plan: Residential; Cul-de-sac/Court/Dead End"

UPDATE_FIELDS = {
    "pin": "065020114",
    "arn": "190109665006400",
    "land_registry_office": "METROPOLITAN TORONTO (80)",
    "land_registry_status": "Active",
    "registration_type": "Certified (Land Titles)",
    "ownership_type": "Freehold",
    "legal_description": "PCL 4-1, SEC M1979 ; LT 4, PL M1979 ; SCARBOROUGH, CITY OF TORONTO",
    "area_sqft": 6609.03,
    "area_acres": 0.152,
    "perimeter_ft": 344.49,
    "frontage_ft": 40.78,
    "depth_ft": None,
    "assessed_value": 845000,
    "valuation_date": "2016-01-01",
    "property_code": 301,
    "property_description": "Single-family detached (not on water)",
    "year_built": 1981,
    "bedrooms": 4,
    "full_bathrooms": 2,
    "half_bathrooms": 1,
    "storeys": 2,
    "fireplace_total": 1,
    "garage_type": "Attached Garage",
    "garage_spaces": 2,
    "indoor_pool": "N",
    "outdoor_pool": "N",
    "zoning": "RD*626)",
    "driveway": "Unspecified/Not Applicable",
    "variance": "Irregular",
    "site_features": SITE_FEATURES,
    "sales_history": json.dumps(SALES_HISTORY),
    "comparables": json.dumps([]),
    "tax_history": json.dumps(TAX_HISTORY),
    "assessed_value_history": json.dumps(ASSESSED_VALUE_HISTORY),
}

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

row = cur.execute(
    "SELECT id FROM property_details WHERE LOWER(TRIM(address)) = ?", ("12 clark secor pl",)
).fetchone()

if not row:
    raise SystemExit("No property_details row found for '12 Clark Secor Pl' — check the address value.")

row_id = row[0]
set_clause = ", ".join(f"{col} = ?" for col in UPDATE_FIELDS)
cur.execute(
    f"UPDATE property_details SET {set_clause} WHERE id = ?",
    (*UPDATE_FIELDS.values(), row_id),
)
con.commit()

print(f"Updated property_details row id={row_id} for 12 Clark Secor Pl with real Teranet data.")
con.close()
