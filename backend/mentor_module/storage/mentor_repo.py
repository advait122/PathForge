from backend.roadmap_engine.storage.database import get_connection, transaction
from backend.roadmap_engine.utils import utc_now_iso


# ── mentor_profiles ────────────────────────────────────────────────────────────

def upsert_mentor_profile(student_id: int, normalized_skill: str) -> None:
    """Create a mentor_profiles row if it doesn't exist (opted_in defaults to 0)."""
    now = utc_now_iso()
    with transaction() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO mentor_profiles
                (student_id, normalized_skill, opted_in, people_helped, badge_level, created_at, updated_at)
            VALUES (?, ?, 0, 0, NULL, ?, ?)
            """,
            (student_id, normalized_skill, now, now),
        )


def set_opted_in(student_id: int, normalized_skill: str, opted_in: bool) -> None:
    now = utc_now_iso()
    opted_in_at = now if opted_in else None
    with transaction() as conn:
        conn.execute(
            """
            UPDATE mentor_profiles
            SET opted_in = ?, opted_in_at = ?, updated_at = ?
            WHERE student_id = ? AND normalized_skill = ?
            """,
            (1 if opted_in else 0, opted_in_at, now, student_id, normalized_skill),
        )


def get_mentor_profile(student_id: int, normalized_skill: str) -> dict | None:
    conn = get_connection()
    try:
        row = conn.execute(
            """
            SELECT id, student_id, normalized_skill, opted_in, opted_in_at,
                   people_helped, badge_level, created_at, updated_at
            FROM mentor_profiles
            WHERE student_id = ? AND normalized_skill = ?
            """,
            (student_id, normalized_skill),
        ).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def get_opted_in_mentors(normalized_skill: str, exclude_student_id: int) -> list[dict]:
    """All opted-in mentors for a skill, excluding the requesting student."""
    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT mp.student_id, mp.people_helped, mp.badge_level,
                   s.name AS student_name, s.branch, s.current_year, s.cgpa
            FROM mentor_profiles mp
            JOIN students s ON s.id = mp.student_id
            WHERE mp.normalized_skill = ? AND mp.opted_in = 1 AND mp.student_id != ?
            """,
            (normalized_skill, exclude_student_id),
        ).fetchall()
    finally:
        conn.close()
    return [dict(row) for row in rows]


def update_badge(student_id: int, normalized_skill: str, people_helped: int, badge_level: str | None) -> None:
    now = utc_now_iso()
    with transaction() as conn:
        conn.execute(
            """
            INSERT INTO mentor_profiles (
                student_id, normalized_skill, opted_in, opted_in_at,
                people_helped, badge_level, created_at, updated_at
            )
            VALUES (?, ?, 1, ?, ?, ?, ?, ?)
            ON CONFLICT(student_id, normalized_skill) DO UPDATE SET
                people_helped = excluded.people_helped,
                badge_level = excluded.badge_level,
                updated_at = excluded.updated_at
            """,
            (
                student_id,
                normalized_skill,
                now,
                people_helped,
                badge_level,
                now,
                now,
            ),
        )


def get_all_mentor_skills_for_student(student_id: int) -> list[dict]:
    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT normalized_skill, opted_in, people_helped, badge_level
            FROM mentor_profiles
            WHERE student_id = ?
            """,
            (student_id,),
        ).fetchall()
    finally:
        conn.close()
    return [dict(row) for row in rows]


# ── mentor_sessions ────────────────────────────────────────────────────────────

def get_open_session(seeker_id: int, mentor_id: int, normalized_skill: str) -> dict | None:
    conn = get_connection()
    try:
        row = conn.execute(
            """
            SELECT id, seeker_id, mentor_id, normalized_skill, status, created_at, closed_at
            FROM mentor_sessions
            WHERE seeker_id = ? AND mentor_id = ? AND normalized_skill = ? AND status = 'open'
            """,
            (seeker_id, mentor_id, normalized_skill),
        ).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def create_session(seeker_id: int, mentor_id: int, normalized_skill: str) -> int:
    now = utc_now_iso()
    with transaction() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO mentor_sessions (seeker_id, mentor_id, normalized_skill, status, created_at)
            VALUES (?, ?, ?, 'open', ?)
            """,
            (seeker_id, mentor_id, normalized_skill, now),
        )
        return int(cursor.lastrowid)


def get_session(session_id: int) -> dict | None:
    conn = get_connection()
    try:
        row = conn.execute(
            """
            SELECT s.id, s.seeker_id, s.mentor_id, s.normalized_skill, s.status,
                   s.created_at, s.closed_at,
                   seeker.name AS seeker_name,
                   mentor.name AS mentor_name
            FROM mentor_sessions s
            JOIN students seeker ON seeker.id = s.seeker_id
            JOIN students mentor ON mentor.id = s.mentor_id
            WHERE s.id = ?
            """,
            (session_id,),
        ).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def close_session(session_id: int) -> None:
    now = utc_now_iso()
    with transaction() as conn:
        conn.execute(
            "UPDATE mentor_sessions SET status = 'closed', closed_at = ? WHERE id = ?",
            (now, session_id),
        )


def cancel_session(session_id: int) -> None:
    now = utc_now_iso()
    with transaction() as conn:
        conn.execute(
            "UPDATE mentor_sessions SET status = 'cancelled', closed_at = ? WHERE id = ?",
            (now, session_id),
        )


def get_sessions_for_mentor(mentor_id: int) -> list[dict]:
    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT s.id, s.seeker_id, s.mentor_id, s.normalized_skill, s.status,
                   s.created_at, s.closed_at,
                   seeker.name AS seeker_name
            FROM mentor_sessions s
            JOIN students seeker ON seeker.id = s.seeker_id
            WHERE s.mentor_id = ?
            ORDER BY s.id DESC
            """,
            (mentor_id,),
        ).fetchall()
    finally:
        conn.close()
    return [dict(row) for row in rows]


def get_sessions_for_seeker(seeker_id: int) -> list[dict]:
    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT s.id, s.seeker_id, s.mentor_id, s.normalized_skill, s.status,
                   s.created_at, s.closed_at,
                   mentor.name AS mentor_name
            FROM mentor_sessions s
            JOIN students mentor ON mentor.id = s.mentor_id
            WHERE s.seeker_id = ?
            ORDER BY s.id DESC
            """,
            (seeker_id,),
        ).fetchall()
    finally:
        conn.close()
    return [dict(row) for row in rows]


def count_distinct_students_helped(mentor_id: int, normalized_skill: str) -> int:
    conn = get_connection()
    try:
        row = conn.execute(
            """
            SELECT COUNT(DISTINCT seeker_id) AS cnt
            FROM mentor_sessions
            WHERE mentor_id = ? AND normalized_skill = ? AND status = 'closed'
            """,
            (mentor_id, normalized_skill),
        ).fetchone()
    finally:
        conn.close()
    return int(row["cnt"]) if row else 0


# ── mentor_messages ────────────────────────────────────────────────────────────

def add_message(session_id: int, sender_id: int, message_text: str) -> int:
    now = utc_now_iso()
    with transaction() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO mentor_messages (session_id, sender_id, message_text, sent_at)
            VALUES (?, ?, ?, ?)
            """,
            (session_id, sender_id, message_text, now),
        )
        return int(cursor.lastrowid)


def get_messages(session_id: int) -> list[dict]:
    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT m.id, m.session_id, m.sender_id, m.message_text, m.sent_at,
                   s.name AS sender_name
            FROM mentor_messages m
            JOIN students s ON s.id = m.sender_id
            WHERE m.session_id = ?
            ORDER BY m.sent_at ASC, m.id ASC
            """,
            (session_id,),
        ).fetchall()
    finally:
        conn.close()
    return [dict(row) for row in rows]


# ── mentor_reviews ─────────────────────────────────────────────────────────────

def create_review(
    session_id: int,
    mentor_id: int,
    seeker_id: int,
    rating: int,
    review_text: str,
) -> None:
    now = utc_now_iso()
    with transaction() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO mentor_reviews
                (session_id, mentor_id, seeker_id, rating, review_text, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (session_id, mentor_id, seeker_id, rating, review_text or "", now),
        )


def get_review_for_session(session_id: int) -> dict | None:
    conn = get_connection()
    try:
        row = conn.execute(
            """
            SELECT id, session_id, mentor_id, seeker_id, rating, review_text, created_at
            FROM mentor_reviews
            WHERE session_id = ?
            """,
            (session_id,),
        ).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def get_avg_rating_for_mentor_skill(mentor_id: int, normalized_skill: str) -> float | None:
    conn = get_connection()
    try:
        row = conn.execute(
            """
            SELECT AVG(r.rating) AS avg_rating
            FROM mentor_reviews r
            JOIN mentor_sessions s ON s.id = r.session_id
            WHERE r.mentor_id = ? AND s.normalized_skill = ?
            """,
            (mentor_id, normalized_skill),
        ).fetchone()
    finally:
        conn.close()
    if row and row["avg_rating"] is not None:
        return float(row["avg_rating"])
    return None
