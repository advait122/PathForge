from backend.roadmap_engine.storage.database import get_connection, transaction
from backend.roadmap_engine.utils import utc_now_iso


def get_session(
    student_id: int,
    goal_id: int,
    goal_skill_id: int,
    playlist_recommendation_id: int,
) -> dict | None:
    connection = get_connection()
    try:
        row = connection.execute(
            """
            SELECT
                id,
                student_id,
                goal_id,
                goal_skill_id,
                playlist_recommendation_id,
                created_at,
                updated_at
            FROM skill_playlist_chat_sessions
            WHERE student_id = ?
              AND goal_id = ?
              AND goal_skill_id = ?
              AND playlist_recommendation_id = ?
            LIMIT 1
            """,
            (student_id, goal_id, goal_skill_id, playlist_recommendation_id),
        ).fetchone()
    finally:
        connection.close()

    return dict(row) if row else None


def get_or_create_session_id(
    student_id: int,
    goal_id: int,
    goal_skill_id: int,
    playlist_recommendation_id: int,
) -> int:
    now = utc_now_iso()
    with transaction() as connection:
        cursor = connection.cursor()
        existing = cursor.execute(
            """
            SELECT id
            FROM skill_playlist_chat_sessions
            WHERE student_id = ?
              AND goal_id = ?
              AND goal_skill_id = ?
              AND playlist_recommendation_id = ?
            LIMIT 1
            """,
            (student_id, goal_id, goal_skill_id, playlist_recommendation_id),
        ).fetchone()
        if existing is not None:
            session_id = int(existing["id"])
            cursor.execute(
                """
                UPDATE skill_playlist_chat_sessions
                SET updated_at = ?
                WHERE id = ?
                """,
                (now, session_id),
            )
            return session_id

        cursor.execute(
            """
            INSERT INTO skill_playlist_chat_sessions (
                student_id,
                goal_id,
                goal_skill_id,
                playlist_recommendation_id,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                student_id,
                goal_id,
                goal_skill_id,
                playlist_recommendation_id,
                now,
                now,
            ),
        )
        return int(cursor.lastrowid)


def add_message(session_id: int, role: str, message_text: str) -> int:
    now = utc_now_iso()
    with transaction() as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            INSERT INTO skill_playlist_chat_messages (
                session_id,
                role,
                message_text,
                created_at
            )
            VALUES (?, ?, ?, ?)
            """,
            (session_id, role, message_text, now),
        )
        cursor.execute(
            """
            UPDATE skill_playlist_chat_sessions
            SET updated_at = ?
            WHERE id = ?
            """,
            (now, session_id),
        )
        return int(cursor.lastrowid)


def list_messages(session_id: int, limit: int = 20) -> list[dict]:
    connection = get_connection()
    try:
        rows = connection.execute(
            """
            SELECT
                id,
                session_id,
                role,
                message_text,
                created_at
            FROM skill_playlist_chat_messages
            WHERE session_id = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (session_id, max(1, int(limit))),
        ).fetchall()
    finally:
        connection.close()

    ordered = [dict(row) for row in rows]
    ordered.reverse()
    return ordered
