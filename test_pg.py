import psycopg2
import os

try:
    db_url = os.getenv("DATABASE_URL")

    if not db_url:
        raise Exception("DATABASE_URL is not set")

    print("Connecting to PostgreSQL...")

    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    # simple query to verify connection
    cur.execute("SELECT version();")
    version = cur.fetchone()

    print("✅ Connection successful!")
    print("PostgreSQL version:", version[0])

    cur.close()
    conn.close()

except Exception as e:
    print("❌ Connection failed")
    print("Error:", str(e))
