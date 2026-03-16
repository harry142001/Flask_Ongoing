import sqlite3

db = input("Which db? (1=main, 2=details): ")
if db == "1":
    con = sqlite3.connect('data/Database1.db')
else:
    con = sqlite3.connect('data/property_details.db')

sql = input("Enter SQL: ")
con.execute(sql)
print(f"Done: {con.total_changes} rows affected")
con.commit()
con.close()
