"""
Admin script: updates the comparables for 4146 Bath Rd, Kingston to match the
same schema used for 12 Clark Secor Pl (usable / type / reason fields).

The original 4 comparables (1399 Tamarac St, 578 Sycamore St, 4205 Bath Rd,
4220 Bath Rd) came straight from 4146 Bath Rd's own embedded "Comparable
Sales" report, with an official Teranet-measured distance_m -- those values
are kept as-is, not recalculated.

2 new comparables were added from individually-sent GeoWarehouse reports
received Aug 31, 2026, the same way the Clark Secor batch was assembled:
686 Willis St and 1367 Waverley Cres. Neither report gave a distance to 4146
Bath Rd (each only shows distances to its own nearby comps), so those two
distance_m values are geocoded estimates (OpenStreetMap/Nominatim), not
Teranet-confirmed -- same caveat as the Clark Secor comparables.

Duplicates received in the same batch (4205 Bath Rd, 1399 Tamarac St -- both
already in the list below) were skipped, not re-added.

Usage:
    python scripts/update_4146_bath_rd_comparables.py
"""
import json
import os
import sqlite3

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "property_details.db")

COMPARABLES = [
    # -- Original 4, from 4146 Bath Rd's own report (official distance_m) --
    {"address": "1399 Tamarac St, Kingston, K7M7J2", "date": "2026-05-01", "sale_price": 1240000,
     "lot_size_sqft": 10333.0, "price_per_sqft": 120, "distance_m": 201, "pin": "361260418",
     "usable": True, "type": "Usable"},
    {"address": "578 Sycamore St, Kingston, K7M7L8", "date": "2026-05-04", "sale_price": 840000,
     "lot_size_sqft": 8428.0, "price_per_sqft": 100, "distance_m": 351, "pin": "361260478",
     "usable": True, "type": "Usable"},
    {"address": "4205 Bath Rd, Kingston, K7M4Y8", "date": "2026-02-12", "sale_price": 1140000,
     "lot_size_sqft": 479456.0, "price_per_sqft": 2, "distance_m": 414, "pin": "361260648",
     "usable": True, "type": "Usable"},
    {"address": "4220 Bath Rd, Kingston, K7M4Y7", "date": "2026-04-30", "sale_price": 870000,
     "lot_size_sqft": 13659.0, "price_per_sqft": 64, "distance_m": 509, "pin": "361260386",
     "usable": True, "type": "Usable"},

    # -- New, Aug 31 2026 batch (geocoded distance_m, not Teranet-confirmed) --
    {"address": "1367 Waverley Cres, Kingston, K7M6J8", "date": "2026-06-19", "sale_price": 462500,
     "lot_size_sqft": 6609.0, "price_per_sqft": 70, "distance_m": 984, "pin": "362670167",
     "usable": True, "type": "Usable",
     "reason": "Sold under Power of Sale (lender-forced), not a standard voluntary sale"},
    {"address": "686 Willis St, Kingston, K7M6H3", "date": "2026-03-12", "sale_price": 2,
     "lot_size_sqft": 6749.0, "distance_m": 849, "pin": "362670199",
     "usable": False, "type": "Nominal Transfer",
     "reason": "Nominal $2 transfer; no recent genuine sale (prior real sale 1990, too stale)"},
]

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

row = cur.execute(
    "SELECT id FROM property_details WHERE LOWER(TRIM(address)) = ?", ("4146 bath rd",)
).fetchone()

if not row:
    raise SystemExit("No property_details row found for '4146 Bath Rd' — check the address value.")

row_id = row[0]
cur.execute(
    "UPDATE property_details SET comparables = ? WHERE id = ?",
    (json.dumps(COMPARABLES), row_id),
)
con.commit()

print(f"Updated property_details row id={row_id} for 4146 Bath Rd with {len(COMPARABLES)} comparables.")
con.close()
