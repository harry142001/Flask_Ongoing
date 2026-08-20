"""
Admin script: adds the new Teranet-sourced columns to property_details.db.
Safe to re-run — skips any column that already exists.

Usage:
    python scripts/migrate_property_details_schema.py
"""
import os
import sqlite3

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "property_details.db")

NEW_COLUMNS = {
    "land_registry_status": "TEXT",
    "legal_description": "TEXT",
    "driveway": "TEXT",
    "variance": "TEXT",
    "site_features": "TEXT",
    "tax_history": "TEXT",
    "assessed_value_history": "TEXT",
}

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

existing_cols = {row[1] for row in cur.execute("PRAGMA table_info(property_details)")}

for col, col_type in NEW_COLUMNS.items():
    if col in existing_cols:
        print(f"Skipping '{col}' — already exists.")
        continue
    cur.execute(f"ALTER TABLE property_details ADD COLUMN {col} {col_type}")
    print(f"Added column '{col}' ({col_type}).")

con.commit()
con.close()
print("\nSchema migration complete.")
