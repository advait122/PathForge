import json

from backend.roadmap_engine.storage.database import get_connection, transaction
from backend.roadmap_engine.utils import utc_now_iso


def create_active_goal(
    *,
    student_id: int,
    goal_text: str,
    target_company: str | None,
    target_role_family: str | None,
    target_duration_months: int,
    start_date: str,
    target_end_date: str,
    llm_confidence: float | None,
    requirements: dict,
) -> int:
    now = utc_now_iso()
    requirements_json = json.dumps(requirements, ensure_ascii=False)

    with transaction() as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            UPDATE career_goals
            SET status = 'archived', updated_at = ?
            WHERE student_id = ? AND status = 'active'
            """,
            (now, student_id),
        )

        cursor.execute(
            """
            INSERT INTO career_goals (
                student_id,
                goal_text,
                target_company,
                target_role_family,
                target_duration_months,
                start_date,
                target_end_date,
                llm_confidence,
                requirements_json,
                status,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?)
            """,
            (
                student_id,
                goal_text,
                target_company,
                target_role_family,
                target_duration_months,
                start_date,
                target_end_date,
                llm_confidence,
                requirements_json,
                now,
                now,
            ),
        )
        return int(cursor.lastrowid)


def get_active_goal(student_id: int) -> dict | None:
    connection = get_connection()
    try:
        row = connection.execute(
            """
            SELECT
                id,
                student_id,
                goal_text,
                target_company,
                target_role_family,
                target_duration_months,
                start_date,
                target_end_date,
                llm_confidence,
                requirements_json,
                status,
                created_at,
                updated_at
            FROM career_goals
            WHERE student_id = ? AND status = 'active'
            ORDER BY id DESC
            LIMIT 1
            """,
            (student_id,),
        ).fetchone()
    finally:
        connection.close()

    if row is None:
        return None

    goal = dict(row)
    goal["requirements"] = json.loads(goal["requirements_json"]) if goal["requirements_json"] else {}
    return goal


def replace_goal_skills(goal_id: int, skills: list[dict]) -> None:
    now = utc_now_iso()
    with transaction() as connection:
        cursor = connection.cursor()
        cursor.execute("DELETE FROM career_goal_skills WHERE goal_id = ?", (goal_id,))

        for skill in skills:
            cursor.execute(
                """
                INSERT INTO career_goal_skills (
                    goal_id,
                    skill_name,
                    normalized_skill,
                    priority,
                    estimated_hours,
                    skill_source,
                    status,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)
                """,
                (
                    goal_id,
                    skill["skill_name"],
                    skill["normalized_skill"],
                    skill["priority"],
                    skill["estimated_hours"],
                    skill["skill_source"],
                    now,
                ),
            )


def list_goal_skills(goal_id: int) -> list[dict]:
    connection = get_connection()
    try:
        rows = connection.execute(
            """
            SELECT
                id,
                goal_id,
                skill_name,
                normalized_skill,
                priority,
                estimated_hours,
                skill_source,
                status,
                completed_at,
                created_at
            FROM career_goal_skills
            WHERE goal_id = ?
            ORDER BY priority ASC, id ASC
            """,
            (goal_id,),
        ).fetchall()
    finally:
        connection.close()

    return [dict(row) for row in rows]


def set_goal_skill_status(goal_skill_id: int, status: str, completed_at: str | None = None) -> None:
    with transaction() as connection:
        connection.execute(
            """
            UPDATE career_goal_skills
            SET status = ?, completed_at = ?
            WHERE id = ?
            """,
            (status, completed_at, goal_skill_id),
        )


def get_goal_skill(goal_skill_id: int) -> dict | None:
    connection = get_connection()
    try:
        row = connection.execute(
            """
            SELECT
                id,
                goal_id,
                skill_name,
                normalized_skill,
                priority,
                estimated_hours,
                skill_source,
                status,
                completed_at,
                created_at
            FROM career_goal_skills
            WHERE id = ?
            """,
            (goal_skill_id,),
        ).fetchone()
    finally:
        connection.close()

    return dict(row) if row else None

