"""
Admin script: pull up to 20 new addresses from Database1.db
that haven't been processed yet, and log them to data/sent_addresses.txt.

Usage:
    python scripts/seed_addresses.py
"""
import os
import sqlite3
from datetime import date

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_FILE = os.path.join(BASE_DIR, "data", "sent_addresses.txt")

already_sent: set = set()
if os.path.exists(LOG_FILE):
    with open(LOG_FILE, "r") as f:
        for line in f:
            already_sent.add(line.strip().lower())

con1 = sqlite3.connect(os.path.join(BASE_DIR, "data", "Database1.db"))
con2 = sqlite3.connect(os.path.join(BASE_DIR, "data", "property_details.db"))

existing = {
    row[0]
    for row in con2.execute("SELECT LOWER(TRIM(address)) FROM property_details").fetchall()
}

rows = con1.execute("""
    SELECT address, city, state, postal
    FROM properties
    WHERE address IS NOT NULL
      AND address != ''
      AND address NOT LIKE '#%'
      AND address NOT LIKE '%-%'
      AND city IS NOT NULL
      AND postal IS NOT NULL
    LIMIT 500
""").fetchall()

count = 0
new_addresses = []
for r in rows:
    addr = r[0].lower().strip()
    if addr not in existing and addr not in already_sent:
        print(f"  {r[0]}, {r[1]}, {r[2]}, {r[3]}")
        new_addresses.append(addr)
        count += 1
        if count == 20:
            break

with open(LOG_FILE, "a") as f:
    for addr in new_addresses:
        f.write(f"{addr}\n")

print(f"\nTotal: {count}")
print(f"Logged to {LOG_FILE} ({date.today()})")
con1.close()
con2.close()
