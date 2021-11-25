from graper.db import DB

db = DB.create("sqlite://test.db")

# Create
db.cursor.execute(
    """
create table if not exists ttt
(
    id INTEGER,
    name TEXT
)
"""
)
# Insert
db.add({"id": 1, "name": "a"}, table_name="ttt")

# Select
print(db.query_all("select * from ttt"))

# Delete
db.delete(condition={"id": 1}, table_name="ttt")
print(db.query_all("select * from ttt"))

# Drop
db.cursor.execute("drop table ttt")

# Transaction

db.cursor.execute("begin")
db.cursor.execute(
    """
create table if not exists ttt
(
id INTEGER,
name TEXT
)
"""
)
# db.connection.commit()

# Close
db.close()
