"""
Admin script: normalizes sales_history shape across all property_details rows.
Old placeholder data used {"date", "amount", "type", "party_to"} — renames
"amount" -> "price" and drops "party_to" to match the current schema
({"date", "price", "type"}). Rows already in the new shape are left alone.

Usage:
    python scripts/normalize_sales_history.py
"""
import json
import os
import sqlite3

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "property_details.db")

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

rows = cur.execute(
    "SELECT id, sales_history FROM property_details WHERE sales_history IS NOT NULL"
).fetchall()

updated = 0
for row_id, raw in rows:
    try:
        entries = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        print(f"Skipping id={row_id} — could not parse sales_history")
        continue

    changed = False
    normalized = []
    for entry in entries:
        if not isinstance(entry, dict):
            normalized.append(entry)
            continue
        new_entry = dict(entry)
        if "amount" in new_entry:
            new_entry["price"] = new_entry.pop("amount")
            changed = True
        if "party_to" in new_entry:
            new_entry.pop("party_to")
            changed = True
        normalized.append(new_entry)

    if changed:
        cur.execute(
            "UPDATE property_details SET sales_history = ? WHERE id = ?",
            (json.dumps(normalized), row_id),
        )
        updated += 1

con.commit()
con.close()
print(f"Normalized sales_history on {updated} row(s).")
