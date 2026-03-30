import sqlite3
import os
from datetime import date

LOG_FILE = 'sent_addresses.txt'

# Load previously sent addresses
already_sent = set()
if os.path.exists(LOG_FILE):
    with open(LOG_FILE, 'r') as f:
        for line in f:
            already_sent.add(line.strip().lower())

con1 = sqlite3.connect('data/Database1.db')
con2 = sqlite3.connect('data/property_details.db')

# Get addresses already in details db
existing = set()
for row in con2.execute('SELECT LOWER(TRIM(address)) FROM property_details').fetchall():
    existing.add(row[0])

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

# Save today's addresses to log
with open(LOG_FILE, 'a') as f:
    for addr in new_addresses:
        f.write(f"{addr}\n")

print(f"\nTotal: {count}")
print(f"Logged to {LOG_FILE} ({date.today()})")
con1.close()
con2.close()