from backend.roadmap_engine.storage.database import transaction


MENTOR_TABLE_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS mentor_profiles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        student_id INTEGER NOT NULL,
        normalized_skill TEXT NOT NULL,
        opted_in INTEGER NOT NULL DEFAULT 0,
        opted_in_at TEXT,
        people_helped INTEGER NOT NULL DEFAULT 0,
        badge_level TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE(student_id, normalized_skill),
        FOREIGN KEY(student_id) REFERENCES students(id) ON DELETE CASCADE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS mentor_sessions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        seeker_id INTEGER NOT NULL,
        mentor_id INTEGER NOT NULL,
        normalized_skill TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'open',
        created_at TEXT NOT NULL,
        closed_at TEXT,
        FOREIGN KEY(seeker_id) REFERENCES students(id) ON DELETE CASCADE,
        FOREIGN KEY(mentor_id) REFERENCES students(id) ON DELETE CASCADE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS mentor_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id INTEGER NOT NULL,
        sender_id INTEGER NOT NULL,
        message_text TEXT NOT NULL,
        sent_at TEXT NOT NULL,
        FOREIGN KEY(session_id) REFERENCES mentor_sessions(id) ON DELETE CASCADE,
        FOREIGN KEY(sender_id) REFERENCES students(id) ON DELETE CASCADE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS mentor_reviews (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id INTEGER NOT NULL UNIQUE,
        mentor_id INTEGER NOT NULL,
        seeker_id INTEGER NOT NULL,
        rating INTEGER NOT NULL,
        review_text TEXT,
        created_at TEXT NOT NULL,
        FOREIGN KEY(session_id) REFERENCES mentor_sessions(id) ON DELETE CASCADE
    );
    """,
]

MENTOR_INDEX_STATEMENTS = [
    "CREATE INDEX IF NOT EXISTS idx_mentor_profiles_student_skill ON mentor_profiles(student_id, normalized_skill);",
    "CREATE INDEX IF NOT EXISTS idx_mentor_sessions_seeker ON mentor_sessions(seeker_id);",
    "CREATE INDEX IF NOT EXISTS idx_mentor_sessions_mentor ON mentor_sessions(mentor_id);",
    "CREATE INDEX IF NOT EXISTS idx_mentor_messages_session ON mentor_messages(session_id);",
    "CREATE INDEX IF NOT EXISTS idx_mentor_reviews_mentor ON mentor_reviews(mentor_id);",
]


def init_mentor_schema() -> None:
    with transaction() as conn:
        cursor = conn.cursor()
        for stmt in MENTOR_TABLE_STATEMENTS:
            cursor.execute(stmt)
        for stmt in MENTOR_INDEX_STATEMENTS:
            cursor.execute(stmt)
