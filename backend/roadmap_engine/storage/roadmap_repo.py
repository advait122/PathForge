from backend.roadmap_engine.storage.database import get_connection, transaction
from backend.roadmap_engine.utils import utc_now_iso


def create_or_replace_plan(goal_id: int, start_date: str, end_date: str) -> int:
    now = utc_now_iso()

    with transaction() as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            UPDATE roadmap_plans
            SET status = 'archived', updated_at = ?
            WHERE goal_id = ? AND status = 'active'
            """,
            (now, goal_id),
        )
        cursor.execute(
            """
            INSERT INTO roadmap_plans (
                goal_id, start_date, end_date, status, created_at, updated_at
            )
            VALUES (?, ?, ?, 'active', ?, ?)
            """,
            (goal_id, start_date, end_date, now, now),
        )
        return int(cursor.lastrowid)


def mark_plan_replanned(plan_id: int) -> None:
    now = utc_now_iso()
    with transaction() as connection:
        connection.execute(
            """
            UPDATE roadmap_plans
            SET last_replanned_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (now, now, plan_id),
        )


def bulk_insert_tasks(plan_id: int, tasks: list[dict]) -> None:
    now = utc_now_iso()
    with transaction() as connection:
        cursor = connection.cursor()
        cursor.execute("DELETE FROM roadmap_plan_tasks WHERE plan_id = ?", (plan_id,))
        cursor.executemany(
            """
            INSERT INTO roadmap_plan_tasks (
                plan_id,
                goal_skill_id,
                task_date,
                title,
                description,
                target_minutes,
                is_completed,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?)
            """,
            [
                (
                    plan_id,
                    task["goal_skill_id"],
                    task["task_date"],
                    task["title"],
                    task.get("description", ""),
                    task["target_minutes"],
                    now,
                    now,
                )
                for task in tasks
            ],
        )


def append_tasks(plan_id: int, tasks: list[dict]) -> None:
    if not tasks:
        return

    now = utc_now_iso()
    with transaction() as connection:
        connection.executemany(
            """
            INSERT INTO roadmap_plan_tasks (
                plan_id,
                goal_skill_id,
                task_date,
                title,
                description,
                target_minutes,
                is_completed,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?)
            """,
            [
                (
                    plan_id,
                    task["goal_skill_id"],
                    task["task_date"],
                    task["title"],
                    task.get("description", ""),
                    task["target_minutes"],
                    now,
                    now,
                )
                for task in tasks
            ],
        )


def get_active_plan(goal_id: int) -> dict | None:
    connection = get_connection()
    try:
        row = connection.execute(
            """
            SELECT id, goal_id, start_date, end_date, status, last_replanned_at, created_at, updated_at
            FROM roadmap_plans
            WHERE goal_id = ? AND status = 'active'
            ORDER BY id DESC
            LIMIT 1
            """,
            (goal_id,),
        ).fetchone()
    finally:
        connection.close()

    return dict(row) if row else None


def list_tasks(plan_id: int, date_from: str | None = None, date_to: str | None = None) -> list[dict]:
    where = ["plan_id = ?"]
    params: list = [plan_id]

    if date_from:
        where.append("task_date >= ?")
        params.append(date_from)
    if date_to:
        where.append("task_date <= ?")
        params.append(date_to)

    where_sql = " AND ".join(where)
    query = f"""
        SELECT
            t.id,
            t.plan_id,
            t.goal_skill_id,
            t.task_date,
            t.title,
            t.description,
            t.target_minutes,
            t.is_completed,
            t.completed_at,
            t.created_at,
            s.skill_name,
            s.normalized_skill
        FROM roadmap_plan_tasks t
        LEFT JOIN career_goal_skills s ON s.id = t.goal_skill_id
        WHERE {where_sql}
        ORDER BY t.task_date ASC, t.id ASC
    """

    connection = get_connection()
    try:
        rows = connection.execute(query, params).fetchall()
    finally:
        connection.close()

    return [dict(row) for row in rows]


def list_incomplete_tasks(plan_id: int) -> list[dict]:
    connection = get_connection()
    try:
        rows = connection.execute(
            """
            SELECT
                id,
                plan_id,
                goal_skill_id,
                task_date,
                title,
                description,
                target_minutes,
                is_completed,
                completed_at,
                created_at,
                updated_at
            FROM roadmap_plan_tasks
            WHERE plan_id = ? AND is_completed = 0
            ORDER BY task_date ASC, id ASC
            """,
            (plan_id,),
        ).fetchall()
    finally:
        connection.close()

    return [dict(row) for row in rows]


def count_overdue_incomplete(plan_id: int, today_iso: str) -> int:
    connection = get_connection()
    try:
        row = connection.execute(
            """
            SELECT COUNT(*) AS total
            FROM roadmap_plan_tasks
            WHERE plan_id = ? AND is_completed = 0 AND task_date < ?
            """,
            (plan_id, today_iso),
        ).fetchone()
    finally:
        connection.close()

    return int(row["total"]) if row else 0


def get_task(task_id: int) -> dict | None:
    connection = get_connection()
    try:
        row = connection.execute(
            """
            SELECT
                id,
                plan_id,
                goal_skill_id,
                task_date,
                title,
                description,
                target_minutes,
                is_completed,
                completed_at,
                created_at,
                updated_at
            FROM roadmap_plan_tasks
            WHERE id = ?
            """,
            (task_id,),
        ).fetchone()
    finally:
        connection.close()

    return dict(row) if row else None


def bulk_update_task_dates(date_updates: list[tuple[int, str]]) -> None:
    if not date_updates:
        return

    now = utc_now_iso()
    with transaction() as connection:
        connection.executemany(
            """
            UPDATE roadmap_plan_tasks
            SET task_date = ?, updated_at = ?
            WHERE id = ?
            """,
            [(new_date, now, task_id) for task_id, new_date in date_updates],
        )


def set_task_completed(task_id: int, is_completed: bool) -> None:
    now = utc_now_iso()
    completed_at = now if is_completed else None
    with transaction() as connection:
        connection.execute(
            """
            UPDATE roadmap_plan_tasks
            SET is_completed = ?, completed_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (1 if is_completed else 0, completed_at, now, task_id),
        )


def list_tasks_for_skill(plan_id: int, goal_skill_id: int) -> list[dict]:
    connection = get_connection()
    try:
        rows = connection.execute(
            """
            SELECT id, task_date, title, description, is_completed, target_minutes
            FROM roadmap_plan_tasks
            WHERE plan_id = ? AND goal_skill_id = ?
            ORDER BY task_date ASC, id ASC
            """,
            (plan_id, goal_skill_id),
        ).fetchall()
    finally:
        connection.close()

    return [dict(row) for row in rows]


def bulk_update_task_content(content_updates: list[tuple[int, str, str]]) -> None:
    if not content_updates:
        return

    now = utc_now_iso()
    with transaction() as connection:
        connection.executemany(
            """
            UPDATE roadmap_plan_tasks
            SET title = ?, description = ?, updated_at = ?
            WHERE id = ?
            """,
            [(title, description, now, task_id) for task_id, title, description in content_updates],
        )
