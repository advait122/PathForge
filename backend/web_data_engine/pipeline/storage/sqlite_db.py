import sqlite3
from pathlib import Path
from datetime import datetime

DB_PATH = Path("opportunities.db")


def get_connection():
    return sqlite3.connect(DB_PATH)


def init_db():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS opportunities (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT,
        company TEXT,
        type TEXT,
        deadline TEXT,
        skills TEXT,
        url TEXT UNIQUE,
        source TEXT,
        content_hash TEXT,
        last_updated TEXT
    )
    """)

    conn.commit()
    conn.close()

    print("Database initialized")


def get_existing_hash(url: str):

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT content_hash FROM opportunities WHERE url = ?", (url,))
    row = cursor.fetchone()

    conn.close()

    return row[0] if row else None


def upsert_opportunity(data: dict, content_hash: str, source: str, url: str):

    conn = get_connection()
    cursor = conn.cursor()

    existing_hash = get_existing_hash(url)

    if existing_hash == content_hash:
        print("⏩ No change — skipping")
        conn.close()
        return

    now = datetime.utcnow().isoformat()

    if existing_hash is None:
        cursor.execute("""
        INSERT INTO opportunities
        (title, company, type, deadline, skills, url, source, content_hash, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data["title"],
            data["company"],
            data["type"],
            data["deadline"],
            str(data["skills"]),
            url,
            source,
            content_hash,
            now
        ))

        print("✅ Inserted:", data["title"])

    else:
        cursor.execute("""
        UPDATE opportunities
        SET title=?, company=?, type=?, deadline=?, skills=?, content_hash=?, last_updated=?
        WHERE url=?
        """, (
            data["title"],
            data["company"],
            data["type"],
            data["deadline"],
            str(data["skills"]),
            content_hash,
            now,
            url
        ))

        print("🔄 Updated:", data["title"])

    conn.commit()
    conn.close()


def delete_expired_opportunities():
    """Delete opportunities whose deadline has passed"""
    conn = get_connection()
    cursor = conn.cursor()
    
    now = datetime.utcnow().isoformat()
    
    # Find and delete opportunities with deadlines in the past
    cursor.execute("""
    DELETE FROM opportunities
    WHERE deadline IS NOT NULL AND deadline < ?
    """, (now,))
    
    deleted_count = cursor.rowcount
    conn.commit()
    conn.close()
    
    if deleted_count > 0:
        print(f"🗑️ Deleted {deleted_count} expired opportunity/opportunities")
    else:
        print("✅ No expired opportunities found")
    
    return deleted_count
