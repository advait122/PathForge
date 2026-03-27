import json

from backend.roadmap_engine.storage.database import get_connection, transaction
from backend.roadmap_engine.utils import utc_now_iso


def load_existing_matches(goal_id: int) -> dict[int, dict]:
    connection = get_connection()
    try:
        rows = connection.execute(
            """
            SELECT
                opportunity_id,
                bucket,
                eligible_now,
                last_evaluated_at
            FROM opportunity_match_cache
            WHERE goal_id = ?
            """,
            (goal_id,),
        ).fetchall()
    finally:
        connection.close()

    return {row["opportunity_id"]: dict(row) for row in rows}


def replace_goal_matches(goal_id: int, matches: list[dict]) -> None:
    now = utc_now_iso()
    with transaction() as connection:
        cursor = connection.cursor()
        cursor.execute("DELETE FROM opportunity_match_cache WHERE goal_id = ?", (goal_id,))
        cursor.executemany(
            """
            INSERT INTO opportunity_match_cache (
                goal_id,
                opportunity_id,
                bucket,
                match_score,
                required_skills_count,
                matched_skills_count,
                missing_skills_json,
                next_skills_json,
                eligible_now,
                last_evaluated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    goal_id,
                    match["opportunity_id"],
                    match["bucket"],
                    match["match_score"],
                    match["required_skills_count"],
                    match["matched_skills_count"],
                    json.dumps(match["missing_skills"], ensure_ascii=False),
                    json.dumps(match["next_skills"], ensure_ascii=False),
                    1 if match["eligible_now"] else 0,
                    now,
                )
                for match in matches
            ],
        )


def list_matches_with_opportunities(goal_id: int) -> list[dict]:
    connection = get_connection()
    try:
        rows = connection.execute(
            """
            SELECT
                m.id,
                m.goal_id,
                m.opportunity_id,
                m.bucket,
                m.match_score,
                m.required_skills_count,
                m.matched_skills_count,
                m.missing_skills_json,
                m.next_skills_json,
                m.eligible_now,
                m.last_evaluated_at,
                o.title,
                o.company,
                o.type,
                o.deadline,
                o.url
            FROM opportunity_match_cache m
            JOIN opportunities o ON o.id = m.opportunity_id
            WHERE m.goal_id = ?
            ORDER BY
                CASE m.bucket
                    WHEN 'eligible_now' THEN 0
                    WHEN 'almost_eligible' THEN 1
                    ELSE 2
                END,
                m.match_score DESC
            """,
            (goal_id,),
        ).fetchall()
    finally:
        connection.close()

    result: list[dict] = []
    for row in rows:
        row_dict = dict(row)
        row_dict["missing_skills"] = json.loads(row_dict["missing_skills_json"])
        row_dict["next_skills"] = json.loads(row_dict["next_skills_json"])
        result.append(row_dict)
    return result


def create_notification(
    *,
    student_id: int,
    goal_id: int | None,
    notification_type: str,
    title: str,
    body: str,
    related_opportunity_id: int | None = None,
) -> None:
    now = utc_now_iso()
    with transaction() as connection:
        connection.execute(
            """
            INSERT INTO user_notifications (
                student_id,
                goal_id,
                notification_type,
                title,
                body,
                related_opportunity_id,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                student_id,
                goal_id,
                notification_type,
                title,
                body,
                related_opportunity_id,
                now,
            ),
        )


def list_notifications(student_id: int, limit: int = 30) -> list[dict]:
    connection = get_connection()
    try:
        rows = connection.execute(
            """
            SELECT
                n.id,
                n.student_id,
                n.goal_id,
                n.notification_type,
                n.title,
                n.body,
                n.related_opportunity_id,
                n.is_read,
                n.created_at,
                o.title AS opportunity_title,
                o.company AS opportunity_company,
                o.url AS opportunity_url
            FROM user_notifications n
            LEFT JOIN opportunities o ON o.id = n.related_opportunity_id
            WHERE n.student_id = ?
            ORDER BY n.id DESC
            LIMIT ?
            """,
            (student_id, limit),
        ).fetchall()
    finally:
        connection.close()

    return [dict(row) for row in rows]

