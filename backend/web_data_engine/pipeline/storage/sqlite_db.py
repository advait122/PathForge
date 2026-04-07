from datetime import datetime
import json

from backend.roadmap_engine.storage.database import get_connection as shared_get_connection


def get_connection():
    return shared_get_connection()


def init_db():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS opportunities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            company TEXT,
            type TEXT,
            audience_type TEXT,
            student_friendly INTEGER NOT NULL DEFAULT 0,
            experience_min INTEGER NOT NULL DEFAULT 0,
            experience_max INTEGER NOT NULL DEFAULT 0,
            deadline TEXT,
            skills TEXT,
            core_skills_json TEXT,
            secondary_skills_json TEXT,
            normalized_skills_json TEXT,
            location TEXT,
            cgpa_requirement REAL,
            backlog_allowed INTEGER,
            description_summary TEXT,
            url TEXT UNIQUE,
            application_url TEXT,
            source TEXT,
            source_url TEXT,
            content_hash TEXT,
            quality_score REAL NOT NULL DEFAULT 0,
            is_active INTEGER NOT NULL DEFAULT 1,
            agent_trace_json TEXT,
            fetched_at TEXT,
            last_validated_at TEXT,
            last_updated TEXT
        )
        """
    )

    existing_columns = {row[1] for row in cursor.execute("PRAGMA table_info(opportunities)").fetchall()}
    additions = {
        "audience_type": "TEXT",
        "student_friendly": "INTEGER NOT NULL DEFAULT 0",
        "experience_min": "INTEGER NOT NULL DEFAULT 0",
        "experience_max": "INTEGER NOT NULL DEFAULT 0",
        "core_skills_json": "TEXT",
        "secondary_skills_json": "TEXT",
        "normalized_skills_json": "TEXT",
        "location": "TEXT",
        "cgpa_requirement": "REAL",
        "backlog_allowed": "INTEGER",
        "description_summary": "TEXT",
        "application_url": "TEXT",
        "source_url": "TEXT",
        "quality_score": "REAL NOT NULL DEFAULT 0",
        "is_active": "INTEGER NOT NULL DEFAULT 1",
        "agent_trace_json": "TEXT",
        "fetched_at": "TEXT",
        "last_validated_at": "TEXT",
    }
    for column_name, definition in additions.items():
        if column_name not in existing_columns:
            cursor.execute(f"ALTER TABLE opportunities ADD COLUMN {column_name} {definition}")

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
        print("No change - skipping")
        conn.close()
        return

    now = datetime.utcnow().isoformat()
    insert_values = (
        data["title"],
        data["company"],
        data["type"],
        data.get("audience_type"),
        int(data.get("student_friendly") or 0),
        int(data.get("experience_min") or 0),
        int(data.get("experience_max") or 0),
        data.get("deadline"),
        str(data.get("skills", [])),
        json.dumps(data.get("core_skills", []), ensure_ascii=False),
        json.dumps(data.get("secondary_skills", []), ensure_ascii=False),
        json.dumps(data.get("normalized_skills", []), ensure_ascii=False),
        data.get("location"),
        data.get("cgpa_requirement"),
        data.get("backlog_allowed"),
        data.get("description_summary"),
        url,
        data.get("application_url", url),
        source,
        data.get("source_url", url),
        content_hash,
        float(data.get("quality_score") or 0.0),
        int(data.get("is_active", 1)),
        data.get("agent_trace_json"),
        data.get("fetched_at"),
        data.get("last_validated_at"),
        now,
    )

    if existing_hash is None:
        cursor.execute(
            """
            INSERT INTO opportunities (
                title, company, type, audience_type, student_friendly, experience_min, experience_max,
                deadline, skills, core_skills_json, secondary_skills_json, normalized_skills_json,
                location, cgpa_requirement, backlog_allowed, description_summary,
                url, application_url, source, source_url, content_hash,
                quality_score, is_active, agent_trace_json, fetched_at, last_validated_at, last_updated
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            insert_values,
        )
        print("Inserted:", data["title"])
    else:
        cursor.execute(
            """
            UPDATE opportunities
            SET title=?, company=?, type=?, audience_type=?, student_friendly=?, experience_min=?, experience_max=?,
                deadline=?, skills=?, core_skills_json=?, secondary_skills_json=?, normalized_skills_json=?,
                location=?, cgpa_requirement=?, backlog_allowed=?, description_summary=?,
                application_url=?, source=?, source_url=?, content_hash=?, quality_score=?, is_active=?, agent_trace_json=?, fetched_at=?, last_validated_at=?, last_updated=?
            WHERE url=?
            """,
            (
                data["title"],
                data["company"],
                data["type"],
                data.get("audience_type"),
                int(data.get("student_friendly") or 0),
                int(data.get("experience_min") or 0),
                int(data.get("experience_max") or 0),
                data.get("deadline"),
                str(data.get("skills", [])),
                json.dumps(data.get("core_skills", []), ensure_ascii=False),
                json.dumps(data.get("secondary_skills", []), ensure_ascii=False),
                json.dumps(data.get("normalized_skills", []), ensure_ascii=False),
                data.get("location"),
                data.get("cgpa_requirement"),
                data.get("backlog_allowed"),
                data.get("description_summary"),
                data.get("application_url", url),
                source,
                data.get("source_url", url),
                content_hash,
                float(data.get("quality_score") or 0.0),
                int(data.get("is_active", 1)),
                data.get("agent_trace_json"),
                data.get("fetched_at"),
                data.get("last_validated_at"),
                now,
                url,
            ),
        )
        print("Updated:", data["title"])

    conn.commit()
    conn.close()


def delete_expired_opportunities():
    conn = get_connection()
    cursor = conn.cursor()
    now = datetime.utcnow().isoformat()
    cursor.execute(
        """
        UPDATE opportunities
        SET is_active = 0, last_validated_at = ?, last_updated = ?
        WHERE is_active = 1 AND deadline IS NOT NULL AND deadline < ?
        """,
        (now, now, now),
    )
    affected = cursor.rowcount
    conn.commit()
    conn.close()
    if affected > 0:
        print(f"Deactivated {affected} expired opportunity/opportunities")
    else:
        print("No expired opportunities found")
    return affected
