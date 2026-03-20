# save as check_matches.py
import sqlite3

con1 = sqlite3.connect('data/Database1.db')
con2 = sqlite3.connect('data/property_details.db')

main_addrs = {r[0].lower().strip() for r in con1.execute("SELECT address FROM properties WHERE address IS NOT NULL").fetchall()}
detail_rows = con2.execute("SELECT address FROM property_details").fetchall()

matched = 0
unmatched = 0
print("Details DB addresses and match status:")
for r in detail_rows:
    addr = r[0].lower().strip()
    if addr in main_addrs:
        print(f"MATCH     — {r[0]}")
        matched += 1
    else:
        print(f"NO MATCH  — {r[0]}")
        unmatched += 1

print(f"\nMatched: {matched}, Unmatched: {unmatched}")