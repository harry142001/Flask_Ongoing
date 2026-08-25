"""
Admin script: fills in the comparables for 12 Clark Secor Pl, Scarborough.

The GeoWarehouse report for 12 Clark Secor Pl had no Comparable Sales section
(see update_12_clark_secor.py), so these were assembled from 31 individual
GeoWarehouse reports received Aug 25, 2026 for nearby properties, keeping only
the 15 with a genuine arm's-length sale (excluding nominal $0-$2 transfers,
estate transmissions, and stale sales).

distance_m is a straight-line distance from each property's geocoded address
to 12 Clark Secor Pl (OpenStreetMap/Nominatim), not a Teranet-confirmed figure
-- none of the source reports were generated with Clark Secor as the subject,
so no official distance exists yet.

Usage:
    python scripts/update_12_clark_secor_comparables.py
"""
import json
import os
import sqlite3

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "property_details.db")

COMPARABLES = [
    {"address": "308 Jaybell Grove, Toronto, M1C2X5", "date": "2026-03-30", "sale_price": 850000,
     "lot_size_sqft": 7815.0, "price_per_sqft": 109, "distance_m": 268, "pin": "062140131"},
    {"address": "22 Hartsville Ave, Toronto, M1C3K4", "date": "2026-04-28", "sale_price": 1100000,
     "lot_size_sqft": 5317.37, "price_per_sqft": 207, "distance_m": 345, "pin": "065020090"},
    {"address": "349 Jaybell Grove, Toronto, M1C2X4", "date": "2026-05-28", "sale_price": 1140000,
     "lot_size_sqft": 7513.2, "price_per_sqft": 152, "distance_m": 385, "pin": "062140170"},
    {"address": "245 Port Union Rd, Toronto, M1C2L2", "date": "2026-04-08", "sale_price": 870700,
     "lot_size_sqft": 6006.26, "price_per_sqft": 145, "distance_m": 494, "pin": "062160004"},
    {"address": "132 Beaverbrook Crt, Scarborough, M1C3A9", "date": "2026-07-02", "sale_price": 1019000,
     "lot_size_sqft": 6296.88, "price_per_sqft": 162, "distance_m": 641, "pin": "062130017"},
    {"address": "12 Cameron Glen Blvd, Toronto", "date": "2026-05-29", "sale_price": 900000,
     "lot_size_sqft": 2292.71, "price_per_sqft": 392, "distance_m": 646, "pin": "065060298"},
    {"address": "138 Beaverbrook Crt, Toronto, M1C3A9", "date": "2026-06-18", "sale_price": 1065000,
     "lot_size_sqft": 6985.77, "price_per_sqft": 152, "distance_m": 669, "pin": "062130020"},
    {"address": "29 Golders Green Ave, Scarborough, M1C3N5", "date": "2026-05-06", "sale_price": 935000,
     "lot_size_sqft": 3692.0, "price_per_sqft": 253, "distance_m": 706, "pin": "062160160"},
    {"address": "420 Brownfield Gdns, Scarborough", "date": "2026-04-30", "sale_price": 1150000,
     "lot_size_sqft": 8740.0, "price_per_sqft": 132, "distance_m": 720, "pin": "062120263"},
    {"address": "106 Beaverbrook Crt, Toronto, M1C3A9", "date": "2024-02-07", "sale_price": 900000,
     "lot_size_sqft": 6027.78, "price_per_sqft": 149, "distance_m": 740, "pin": "062130004"},
    {"address": "271 Rouge Hills Dr, Toronto, M1C2Z2", "date": "2026-06-08", "sale_price": 640000,
     "lot_size_sqft": 7201.05, "price_per_sqft": 89, "distance_m": 797, "pin": "062100111"},
    {"address": "1 Wheeling Dr, Toronto", "date": "2026-03-16", "sale_price": 930000,
     "lot_size_sqft": 4553.13, "price_per_sqft": 204, "distance_m": 799, "pin": "062150195"},
    {"address": "29 Andona Cres, Toronto, M1C5J6", "date": "2026-06-10", "sale_price": 870000,
     "lot_size_sqft": 2604.86, "price_per_sqft": 334, "distance_m": 806, "pin": "065060398"},
    {"address": "399 Lawson Rd, Toronto, M1C2J8", "date": "2026-06-22", "sale_price": 1100000,
     "lot_size_sqft": 6533.69, "price_per_sqft": 168, "distance_m": 816, "pin": "062200026"},
    {"address": "24 Adenmore Rd, Toronto, M1C5B4", "date": "2026-02-23", "sale_price": 968000,
     "lot_size_sqft": 3940.0, "price_per_sqft": 246, "distance_m": 904, "pin": "062160366"},
]

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

row = cur.execute(
    "SELECT id FROM property_details WHERE LOWER(TRIM(address)) = ?", ("12 clark secor pl",)
).fetchone()

if not row:
    raise SystemExit("No property_details row found for '12 Clark Secor Pl' — check the address value.")

row_id = row[0]
cur.execute(
    "UPDATE property_details SET comparables = ? WHERE id = ?",
    (json.dumps(COMPARABLES), row_id),
)
con.commit()

print(f"Updated property_details row id={row_id} for 12 Clark Secor Pl with {len(COMPARABLES)} comparables.")
con.close()
