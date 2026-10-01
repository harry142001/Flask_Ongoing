"""
Admin script: makes `date_added` mean "a real, confirmed listing date" for
every property, instead of the synthetic sequence it held before.

- Properties whose mls_number appears in the CSV get date_added set to the
  real listing_date (date only, no time -- e.g. "2024-02-01").
- Every other property gets date_added cleared to NULL. A blank date_added
  now means "we don't have a real date for this one yet", which is what
  lets /api/v1/recent tell real dates apart from the old fake ones: the
  fake ones are gone, not just old-looking.
- Running this again later (e.g. next week's CSV) only ever adds more real
  dates -- it never re-introduces fake ones.

Usage:
    python scripts/backfill_real_dates.py
"""
import csv
import os
import shutil
import sqlite3

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "Database1.db")
BACKUP_PATH = os.path.join(BASE_DIR, "data", "Database1_1_backup.db")
CSV_PATH = os.path.join(BASE_DIR, "data", "real_dates_last3weeks.csv")

shutil.copy2(DB_PATH, BACKUP_PATH)
print(f"Backed up {DB_PATH} -> {BACKUP_PATH}")

with open(CSV_PATH, newline="", encoding="utf-8") as f:
    rows = list(csv.DictReader(f))
print(f"Read {len(rows)} rows from {CSV_PATH}")

# Last row wins if the CSV has the same mls_number more than once.
# Keep the date part only -- "2024-02-01 00:00:00" -> "2024-02-01".
real_date_by_mls = {r["mls_number"]: r["listing_date"][:10] for r in rows}

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

# One bulk read instead of one query per CSV row. Keep rowid so the writes
# below can target "WHERE rowid = ?" -- rowid is indexed by nature in
# SQLite, unlike mls_number, so this avoids a full table scan per UPDATE.
existing = cur.execute("SELECT rowid, mls_number, date_added FROM properties").fetchall()

real_updates = []    # (rowid, mls_number, old_date_added, new_date_added)
cleared = []         # (rowid, mls_number, old_date_added)
not_found = []
seen_mls = set()

for rowid, mls_number, old_date in existing:
    seen_mls.add(mls_number)
    if mls_number in real_date_by_mls:
        new_date = real_date_by_mls[mls_number]
        if old_date != new_date:
            real_updates.append((rowid, mls_number, old_date, new_date))
    elif old_date is not None:
        cleared.append((rowid, mls_number, old_date))

for mls_number in real_date_by_mls:
    if mls_number not in seen_mls:
        not_found.append(mls_number)

print(f"\n{len(real_updates)} rows will get a real date_added.")
print(f"{len(cleared)} rows will have date_added cleared to NULL (no confirmed date yet).")
print(f"{len(not_found)} mls_numbers from the CSV have no matching property row (skipped).")

print("\nExample rows getting a real date:")
for rowid, mls_number, old_date, new_date in real_updates[:5]:
    print(f"  mls_number={mls_number}  old date_added={old_date!r}  new date_added={new_date!r}")

print("\nExample rows being cleared:")
for rowid, mls_number, old_date in cleared[:5]:
    print(f"  mls_number={mls_number}  old date_added={old_date!r}  new date_added=NULL")

if not real_updates and not cleared:
    print("\nNothing to update. Exiting without touching the database.")
    con.close()
    raise SystemExit(0)

cur.execute("BEGIN")
cur.executemany(
    "UPDATE properties SET date_added = ? WHERE rowid = ?",
    [(new_date, rowid) for rowid, mls_number, old_date, new_date in real_updates],
)
cur.executemany(
    "UPDATE properties SET date_added = NULL WHERE rowid = ?",
    [(rowid,) for rowid, mls_number, old_date in cleared],
)

answer = input("\nType 'confirm' to commit these changes (anything else rolls back): ").strip()
if answer == "confirm":
    con.commit()
    print(f"Committed. {len(real_updates)} rows given a real date, {len(cleared)} rows cleared.")
else:
    con.rollback()
    print("Rolled back. No changes made.")

con.close()
