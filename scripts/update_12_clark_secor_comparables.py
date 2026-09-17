"""
Admin script: fills in the comparables for 12 Clark Secor Pl, Scarborough.

The GeoWarehouse report for 12 Clark Secor Pl had no Comparable Sales section
(see update_12_clark_secor.py), so these were assembled from 31 individual
GeoWarehouse reports received Aug 25, 2026 for nearby properties. All 31 are
kept here for the full audit trail; 15 have a genuine arm's-length sale
("usable": true, "type": "Usable") and 16 don't ("usable": false, with a
"type" of Nominal Transfer / Estate Transfer / Stale Sale / Vacant Land, plus
a fuller "reason").

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
    # -- Usable: genuine arm's-length sale --
    {"address": "308 Jaybell Grove, Toronto, M1C2X5", "date": "2026-03-30", "sale_price": 850000,
     "lot_size_sqft": 7815.0, "price_per_sqft": 109, "distance_m": 268, "pin": "062140131",
     "usable": True, "type": "Usable"},
    {"address": "22 Hartsville Ave, Toronto, M1C3K4", "date": "2026-04-28", "sale_price": 1100000,
     "lot_size_sqft": 5317.37, "price_per_sqft": 207, "distance_m": 345, "pin": "065020090",
     "usable": True, "type": "Usable"},
    {"address": "349 Jaybell Grove, Toronto, M1C2X4", "date": "2026-05-28", "sale_price": 1140000,
     "lot_size_sqft": 7513.2, "price_per_sqft": 152, "distance_m": 385, "pin": "062140170",
     "usable": True, "type": "Usable"},
    {"address": "245 Port Union Rd, Toronto, M1C2L2", "date": "2026-04-08", "sale_price": 870700,
     "lot_size_sqft": 6006.26, "price_per_sqft": 145, "distance_m": 494, "pin": "062160004",
     "usable": True, "type": "Usable"},
    {"address": "132 Beaverbrook Crt, Scarborough, M1C3A9", "date": "2026-07-02", "sale_price": 1019000,
     "lot_size_sqft": 6296.88, "price_per_sqft": 162, "distance_m": 641, "pin": "062130017",
     "usable": True, "type": "Usable"},
    {"address": "12 Cameron Glen Blvd, Toronto", "date": "2026-05-29", "sale_price": 900000,
     "lot_size_sqft": 2292.71, "price_per_sqft": 392, "distance_m": 646, "pin": "065060298",
     "usable": True, "type": "Usable"},
    {"address": "138 Beaverbrook Crt, Toronto, M1C3A9", "date": "2026-06-18", "sale_price": 1065000,
     "lot_size_sqft": 6985.77, "price_per_sqft": 152, "distance_m": 669, "pin": "062130020",
     "usable": True, "type": "Usable"},
    {"address": "29 Golders Green Ave, Scarborough, M1C3N5", "date": "2026-05-06", "sale_price": 935000,
     "lot_size_sqft": 3692.0, "price_per_sqft": 253, "distance_m": 706, "pin": "062160160",
     "usable": True, "type": "Usable"},
    {"address": "420 Brownfield Gdns, Scarborough", "date": "2026-04-30", "sale_price": 1150000,
     "lot_size_sqft": 8740.0, "price_per_sqft": 132, "distance_m": 720, "pin": "062120263",
     "usable": True, "type": "Usable"},
    {"address": "106 Beaverbrook Crt, Toronto, M1C3A9", "date": "2024-02-07", "sale_price": 900000,
     "lot_size_sqft": 6027.78, "price_per_sqft": 149, "distance_m": 740, "pin": "062130004",
     "usable": True, "type": "Usable"},
    {"address": "271 Rouge Hills Dr, Toronto, M1C2Z2", "date": "2026-06-08", "sale_price": 640000,
     "lot_size_sqft": 7201.05, "price_per_sqft": 89, "distance_m": 797, "pin": "062100111",
     "usable": True, "type": "Usable"},
    {"address": "1 Wheeling Dr, Toronto", "date": "2026-03-16", "sale_price": 930000,
     "lot_size_sqft": 4553.13, "price_per_sqft": 204, "distance_m": 799, "pin": "062150195",
     "usable": True, "type": "Usable"},
    {"address": "29 Andona Cres, Toronto, M1C5J6", "date": "2026-06-10", "sale_price": 870000,
     "lot_size_sqft": 2604.86, "price_per_sqft": 334, "distance_m": 806, "pin": "065060398",
     "usable": True, "type": "Usable"},
    {"address": "399 Lawson Rd, Toronto, M1C2J8", "date": "2026-06-22", "sale_price": 1100000,
     "lot_size_sqft": 6533.69, "price_per_sqft": 168, "distance_m": 816, "pin": "062200026",
     "usable": True, "type": "Usable"},
    {"address": "24 Adenmore Rd, Toronto, M1C5B4", "date": "2026-02-23", "sale_price": 968000,
     "lot_size_sqft": 3940.0, "price_per_sqft": 246, "distance_m": 904, "pin": "062160366",
     "usable": True, "type": "Usable"},

    # -- Not usable: no genuine market sale to go on --
    {"address": "268 Ridgewood Rd, Toronto", "date": "2026-03-10", "sale_price": 0,
     "distance_m": 70, "pin": "065020014", "usable": False, "type": "Estate Transfer",
     "reason": "Estate transfer (personal representative), no market price"},
    {"address": "44 Ravine Park Cres, Scarborough", "date": "2026-06-25", "sale_price": 0,
     "lot_size_sqft": 6781.26, "distance_m": 161, "pin": "062170123", "usable": False, "type": "Estate Transfer",
     "reason": "Estate transfer, no market price"},
    {"address": "338 Jaybell Grove, Toronto", "date": "2026-03-16", "sale_price": 0,
     "lot_size_sqft": 8956.0, "distance_m": 314, "pin": "062140146", "usable": False, "type": "Nominal Transfer",
     "reason": "Nominal transfer; real 2012 sale too stale"},
    {"address": "15 Birdsilver Gdns, Toronto", "date": "2000-04-27", "sale_price": 305000,
     "lot_size_sqft": 5511.0, "distance_m": 419, "pin": "062120157", "usable": False, "type": "Nominal Transfer",
     "reason": "Last genuine sale 26 yrs stale; recent transfer nominal"},
    {"address": "237 Port Union Rd, Scarborough, M1C2L2", "date": "2000-04-18", "sale_price": 2,
     "lot_size_sqft": 1872.92, "distance_m": 496, "pin": "062160557", "usable": False, "type": "Vacant Land",
     "reason": "Vacant road-allowance strip owned by the City of Toronto, not a residence"},
    {"address": "392 East Ave, Toronto", "date": "2026-04-08", "sale_price": 1,
     "lot_size_sqft": 7502.0, "distance_m": 552, "pin": "062180161", "usable": False, "type": "Nominal Transfer",
     "reason": "Nominal transfer"},
    {"address": "26 Greybeaver Trail, Toronto", "date": "2012-11-21", "sale_price": 554000,
     "distance_m": 698, "pin": "062140306", "usable": False, "type": "Stale Sale",
     "reason": "Last sale 14 yrs stale"},
    {"address": "31 Starspray Blvd, Toronto", "date": "2026-07-17", "sale_price": 2,
     "lot_size_sqft": 3315.0, "distance_m": 720, "pin": "062140389", "usable": False, "type": "Nominal Transfer",
     "reason": "Nominal transfer"},
    {"address": "46 Elkwood Dr, Toronto", "date": "2026-02-23", "sale_price": 2,
     "lot_size_sqft": 5597.23, "distance_m": 801, "pin": "062210037", "usable": False, "type": "Nominal Transfer",
     "reason": "Nominal transfer"},
    {"address": "461 Friendship Ave, Scarborough", "date": "1987-09-01", "sale_price": 52000,
     "lot_size_sqft": 12378.0, "distance_m": 818, "pin": "062120246", "usable": False, "type": "Stale Sale",
     "reason": "Last sale 39 yrs stale"},
    {"address": "429 Rouge Hills Dr, Scarborough", "date": "2026-02-23", "sale_price": 1,
     "lot_size_sqft": 9203.0, "distance_m": 832, "pin": "062100019", "usable": False, "type": "Estate Transfer",
     "reason": "Estate transmission, no market price"},
    {"address": "136 Andona Cres, Toronto", "date": "2026-03-13", "sale_price": 2,
     "lot_size_sqft": 2239.0, "distance_m": 904, "pin": "065060112", "usable": False, "type": "Nominal Transfer",
     "reason": "Nominal transfer; real 2021 sale too dated"},
    {"address": "131 Andona Cres, Toronto", "date": "2026-02-20", "sale_price": 1,
     "lot_size_sqft": 2217.0, "distance_m": 942, "pin": "065060138", "usable": False, "type": "Nominal Transfer",
     "reason": "Nominal transfer"},
    {"address": "177 Island Rd, Toronto", "date": "2026-05-22", "sale_price": 0,
     "lot_size_sqft": 8138.0, "distance_m": 960, "pin": "062120285", "usable": False, "type": "Estate Transfer",
     "reason": "Estate transmission, no market price"},
    {"address": "112 Andona Cres, Toronto", "date": "2018-03-26", "sale_price": 2,
     "distance_m": 986, "pin": "065060102", "usable": False, "type": "Nominal Transfer",
     "reason": "Nominal transfer; real 2004 sale too stale"},
    {"address": "496 Rouge Hills Dr, Scarborough", "date": "2026-03-13", "sale_price": 0,
     "lot_size_sqft": 6792.02, "distance_m": 1050, "pin": "062120314", "usable": False, "type": "Vacant Land",
     "reason": "Corporate-owned, no assessment/structure data — likely vacant/teardown"},
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
