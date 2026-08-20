import sqlite3
import hashlib

def normalize(value):
    return (value or "").strip().lower()

conn = sqlite3.connect("data/Database1.db")
conn.row_factory = sqlite3.Row
rows = conn.execute(
    "SELECT rowid, address, city, postal FROM properties WHERE property_key IS NULL OR property_key = ''"
).fetchall()
print(f"Found {len(rows)} rows to update.")

count = 0
for row in rows:
    if not row["address"]:
        continue
    raw = normalize(row["address"]) + "|" + normalize(row["city"]) + "|" + normalize(row["postal"])
    key = hashlib.sha256(raw.encode()).hexdigest()[:16]
    conn.execute("UPDATE properties SET property_key = ? WHERE rowid = ?", (key, row["rowid"]))
    count += 1

conn.commit()
print(f"Updated {count} rows.")