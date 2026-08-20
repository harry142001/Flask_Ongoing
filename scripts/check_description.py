import sqlite3

conn = sqlite3.connect("data/Database1.db")
conn.row_factory = sqlite3.Row
row = conn.execute(
    "SELECT address, city, price, bedrooms, bathrooms, description FROM properties WHERE property_key = ?",
    ("bc0a78bcff982d74",),
).fetchone()
conn.close()

if row:
    print("Address:", row["address"], row["city"])
    print("Price:", row["price"], "| Beds:", row["bedrooms"], "| Baths:", row["bathrooms"])
    print()
    print("Generated description:")
    print(row["description"])
else:
    print("Row not found.")