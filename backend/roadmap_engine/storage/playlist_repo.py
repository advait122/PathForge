import json

from backend.roadmap_engine.storage.database import get_connection, transaction
from backend.roadmap_engine.utils import utc_now_iso


def replace_skill_recommendations(goal_id: int, goal_skill_id: int, recommendations: list[dict]) -> None:
    now = utc_now_iso()
    with transaction() as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            DELETE FROM playlist_recommendations
            WHERE goal_id = ? AND goal_skill_id = ?
            """,
            (goal_id, goal_skill_id),
        )
        cursor.executemany(
            """
            INSERT INTO playlist_recommendations (
                goal_id,
                goal_skill_id,
                playlist_id,
                title,
                channel_title,
                playlist_url,
                rank_score,
                summary_json,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    goal_id,
                    goal_skill_id,
                    item["playlist_id"],
                    item["title"],
                    item.get("channel_title", ""),
                    item["playlist_url"],
                    item.get("rank_score", 0.0),
                    json.dumps(item.get("summary", {}), ensure_ascii=False),
                    now,
                )
                for item in recommendations
            ],
        )


def list_skill_recommendations(goal_id: int, goal_skill_id: int) -> list[dict]:
    connection = get_connection()
    try:
        rows = connection.execute(
            """
            SELECT
                r.id,
                r.goal_id,
                r.goal_skill_id,
                r.playlist_id,
                r.title,
                r.channel_title,
                r.playlist_url,
                r.rank_score,
                r.summary_json,
                r.created_at,
                CASE WHEN s.id IS NOT NULL THEN 1 ELSE 0 END AS is_selected
            FROM playlist_recommendations r
            LEFT JOIN goal_skill_selected_playlists s
                ON s.playlist_recommendation_id = r.id
                AND s.goal_skill_id = r.goal_skill_id
            WHERE r.goal_id = ? AND r.goal_skill_id = ?
            ORDER BY r.rank_score DESC, r.id ASC
            """,
            (goal_id, goal_skill_id),
        ).fetchall()
    finally:
        connection.close()

    result: list[dict] = []
    for row in rows:
        row_dict = dict(row)
        row_dict["summary"] = json.loads(row_dict["summary_json"]) if row_dict["summary_json"] else {}
        result.append(row_dict)
    return result


def select_recommendation(goal_id: int, goal_skill_id: int, recommendation_id: int) -> None:
    now = utc_now_iso()
    with transaction() as connection:
        cursor = connection.cursor()
        row = cursor.execute(
            """
            SELECT id
            FROM playlist_recommendations
            WHERE id = ? AND goal_id = ? AND goal_skill_id = ?
            """,
            (recommendation_id, goal_id, goal_skill_id),
        ).fetchone()
        if row is None:
            raise ValueError("Selected playlist option is invalid for this skill.")

        cursor.execute(
            """
            DELETE FROM goal_skill_selected_playlists
            WHERE goal_id = ? AND goal_skill_id = ?
            """,
            (goal_id, goal_skill_id),
        )
        cursor.execute(
            """
            INSERT INTO goal_skill_selected_playlists (
                goal_id,
                goal_skill_id,
                playlist_recommendation_id,
                selected_at
            )
            VALUES (?, ?, ?, ?)
            """,
            (goal_id, goal_skill_id, recommendation_id, now),
        )


def get_selected_recommendation(goal_id: int, goal_skill_id: int) -> dict | None:
    connection = get_connection()
    try:
        row = connection.execute(
            """
            SELECT
                r.id,
                r.goal_id,
                r.goal_skill_id,
                r.playlist_id,
                r.title,
                r.channel_title,
                r.playlist_url,
                r.rank_score,
                r.summary_json,
                r.created_at,
                s.selected_at
            FROM goal_skill_selected_playlists s
            JOIN playlist_recommendations r ON r.id = s.playlist_recommendation_id
            WHERE s.goal_id = ? AND s.goal_skill_id = ?
            LIMIT 1
            """,
            (goal_id, goal_skill_id),
        ).fetchone()
    finally:
        connection.close()

    if row is None:
        return None

    result = dict(row)
    result["summary"] = json.loads(result["summary_json"]) if result["summary_json"] else {}
    result["is_selected"] = 1
    return result
