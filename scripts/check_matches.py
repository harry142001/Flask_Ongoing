"""
Admin script: check which addresses in property_details.db
have a matching record in Database1.db.

Usage:
    python scripts/check_matches.py
"""
import sqlite3
import sys
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

con1 = sqlite3.connect(os.path.join(BASE_DIR, "data", "Database1.db"))
con2 = sqlite3.connect(os.path.join(BASE_DIR, "data", "property_details.db"))

main_addrs = {
    r[0].lower().strip()
    for r in con1.execute("SELECT address FROM properties WHERE address IS NOT NULL").fetchall()
}
detail_rows = con2.execute("SELECT address FROM property_details").fetchall()

matched = unmatched = 0
for r in detail_rows:
    addr = r[0].lower().strip()
    status = "MATCH    " if addr in main_addrs else "NO MATCH "
    print(f"{status} — {r[0]}")
    if addr in main_addrs:
        matched += 1
    else:
        unmatched += 1

print(f"\nMatched: {matched}, Unmatched: {unmatched}")
con1.close()
con2.close()
