import mysql.connector

connection = mysql.connector.connect(
    host="mysql.railway.internal",
    user="root",
    password="YOUR_PASSWORD_HERE",
    database="railway"
)

cursor = connection.cursor()

with open("partner_events.sql", "r", encoding="utf-8") as f:
    sql_script = f.read()

for statement in sql_script.split(";"):
    if statement.strip():
        cursor.execute(statement)

connection.commit()
cursor.close()
connection.close()

print("✅ SQL file imported successfully!")
