import psycopg2

# Connect to Supabase
conn = psycopg2.connect(
    host="db.prwzydmfrcbepgevmqmu.supabase.co",
    port=5432,
    database="postgres",
    user="etl_user1",
    password="cyuqD639?TT",
    sslmode="require"
)

cur = conn.cursor()

# Optional: print current timestamp
cur.execute("SELECT NOW();")
print("Current timestamp:", cur.fetchone()[0])

# Fetch and print first 5 rows from your table
cur.execute("SELECT * FROM policies LIMIT 5;")  # make sure table name matches exactly
rows = cur.fetchall()
for row in rows:
    print(row)

cur.close()
conn.close()
