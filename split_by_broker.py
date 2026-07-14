import sqlite3
import os
import re

SOURCE_DB = "data/Database1.db"
OUTPUT_DIR = "broker_dbs"

os.makedirs(OUTPUT_DIR, exist_ok=True)

def safe_filename(name):
    """Convert broker name to a safe filename."""
    name = name.strip().lower()
    name = re.sub(r'[^a-z0-9]+', '_', name)
    return name.strip('_')

src = sqlite3.connect(SOURCE_DB)
src.row_factory = sqlite3.Row

# Get all brokers
brokers = [row[0] for row in src.execute("SELECT DISTINCT broker FROM properties WHERE broker != ''")]

print(f"Found {len(brokers)} brokers. Splitting...")

for broker in brokers:
    filename = safe_filename(broker) + ".db"
    out_path = os.path.join(OUTPUT_DIR, filename)

    dst = sqlite3.connect(out_path)
    dst.execute("""
        CREATE TABLE IF NOT EXISTS properties (
            address TEXT, city TEXT, state TEXT, postal TEXT,
            agent TEXT, broker TEXT, price TEXT,
            latitude TEXT, longitude TEXT, date_added TEXT
        )
    """)

    rows = src.execute(
        "SELECT * FROM properties WHERE broker = ?", (broker,)
    ).fetchall()

    dst.executemany(
        "INSERT INTO properties VALUES (?,?,?,?,?,?,?,?,?,?)",
        [tuple(r) for r in rows]
    )
    dst.commit()
    dst.close()

    print(f"  {broker}: {len(rows)} records → {filename}")

src.close()
print(f"\nDone. {len(brokers)} databases saved to '{OUTPUT_DIR}/'")
