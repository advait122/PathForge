import json

from backend.roadmap_engine.storage.database import get_connection, transaction
from backend.roadmap_engine.utils import utc_now_iso


def _hydrate_job(row) -> dict | None:
    if row is None:
        return None
    job = dict(row)
    raw = job.get("required_skills_json") or "[]"
    try:
        job["required_skills"] = json.loads(raw)
    except json.JSONDecodeError:
        job["required_skills"] = []
    return job


def create_company_account(username: str, password_hash: str) -> int:
    now = utc_now_iso()
    with transaction() as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            INSERT INTO company_accounts (username, password_hash, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            """,
            (username, password_hash, now, now),
        )
        return int(cursor.lastrowid)


def get_company_account(company_id: int) -> dict | None:
    connection = get_connection()
    try:
        row = connection.execute(
            """
            SELECT id, username, password_hash, created_at, updated_at
            FROM company_accounts
            WHERE id = ?
            """,
            (company_id,),
        ).fetchone()
    finally:
        connection.close()

    return dict(row) if row else None


def get_company_by_username(username: str) -> dict | None:
    connection = get_connection()
    try:
        row = connection.execute(
            """
            SELECT id, username, password_hash, created_at, updated_at
            FROM company_accounts
            WHERE username = ?
            """,
            (username,),
        ).fetchone()
    finally:
        connection.close()

    return dict(row) if row else None


def create_job_post(
    *,
    company_id: int,
    title: str,
    job_description: str,
    required_skills: list[str],
    allow_active_backlog: bool,
    min_cgpa: float,
    shortlist_count: int,
    application_deadline: str,
) -> int:
    now = utc_now_iso()
    with transaction() as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            INSERT INTO company_job_posts (
                company_id,
                title,
                job_description,
                required_skills_json,
                allow_active_backlog,
                min_cgpa,
                shortlist_count,
                application_deadline,
                status,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'open', ?, ?)
            """,
            (
                company_id,
                title,
                job_description,
                json.dumps(required_skills, ensure_ascii=False),
                1 if allow_active_backlog else 0,
                float(min_cgpa),
                int(shortlist_count),
                application_deadline,
                now,
                now,
            ),
        )
        return int(cursor.lastrowid)


def get_job_post(job_id: int) -> dict | None:
    connection = get_connection()
    try:
        row = connection.execute(
            """
            SELECT
                j.id,
                j.company_id,
                j.title,
                j.job_description,
                j.required_skills_json,
                j.allow_active_backlog,
                j.min_cgpa,
                j.shortlist_count,
                j.application_deadline,
                j.status,
                j.created_at,
                j.updated_at,
                c.username AS company_username
            FROM company_job_posts j
            JOIN company_accounts c ON c.id = j.company_id
            WHERE j.id = ?
            """,
            (job_id,),
        ).fetchone()
    finally:
        connection.close()

    return _hydrate_job(row)


def list_company_jobs(company_id: int, limit: int = 20) -> list[dict]:
    connection = get_connection()
    try:
        rows = connection.execute(
            """
            SELECT
                j.id,
                j.company_id,
                j.title,
                j.job_description,
                j.required_skills_json,
                j.allow_active_backlog,
                j.min_cgpa,
                j.shortlist_count,
                j.application_deadline,
                j.status,
                j.created_at,
                j.updated_at
            FROM company_job_posts j
            WHERE j.company_id = ?
            ORDER BY j.id DESC
            LIMIT ?
            """,
            (company_id, int(limit)),
        ).fetchall()
    finally:
        connection.close()

    return [_hydrate_job(row) for row in rows]


def upsert_job_application(job_id: int, student_id: int, status: str) -> None:
    now = utc_now_iso()
    with transaction() as connection:
        connection.execute(
            """
            INSERT INTO company_job_applications (
                job_id,
                student_id,
                status,
                invited_at,
                acted_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, NULL, ?)
            ON CONFLICT(job_id, student_id)
            DO UPDATE SET
                status = excluded.status,
                updated_at = excluded.updated_at
            """,
            (job_id, student_id, status, now, now),
        )


def get_job_application(job_id: int, student_id: int) -> dict | None:
    connection = get_connection()
    try:
        row = connection.execute(
            """
            SELECT
                id,
                job_id,
                student_id,
                status,
                invited_at,
                acted_at,
                updated_at
            FROM company_job_applications
            WHERE job_id = ? AND student_id = ?
            """,
            (job_id, student_id),
        ).fetchone()
    finally:
        connection.close()

    return dict(row) if row else None


def set_job_application_status(job_id: int, student_id: int, status: str) -> None:
    now = utc_now_iso()
    acted_at = now if status in {"applied", "declined"} else None
    with transaction() as connection:
        connection.execute(
            """
            UPDATE company_job_applications
            SET status = ?, acted_at = ?, updated_at = ?
            WHERE job_id = ? AND student_id = ?
            """,
            (status, acted_at, now, job_id, student_id),
        )


def list_job_applications(job_id: int) -> list[dict]:
    connection = get_connection()
    try:
        rows = connection.execute(
            """
            SELECT
                a.id,
                a.job_id,
                a.student_id,
                a.status,
                a.invited_at,
                a.acted_at,
                a.updated_at,
                s.name AS student_name,
                s.branch,
                s.current_year
            FROM company_job_applications a
            JOIN students s ON s.id = a.student_id
            WHERE a.job_id = ?
            ORDER BY a.id DESC
            """,
            (job_id,),
        ).fetchall()
    finally:
        connection.close()

    return [dict(row) for row in rows]


def add_shortlist(job_id: int, student_id: int) -> None:
    now = utc_now_iso()
    with transaction() as connection:
        connection.execute(
            """
            INSERT OR IGNORE INTO company_job_shortlists (job_id, student_id, created_at)
            VALUES (?, ?, ?)
            """,
            (job_id, student_id, now),
        )


def list_shortlisted_students(job_id: int) -> list[dict]:
    connection = get_connection()
    try:
        rows = connection.execute(
            """
            SELECT
                s.id AS student_id,
                s.name AS student_name,
                s.branch,
                s.current_year,
                sl.created_at
            FROM company_job_shortlists sl
            JOIN students s ON s.id = sl.student_id
            WHERE sl.job_id = ?
            ORDER BY sl.id ASC
            """,
            (job_id,),
        ).fetchall()
    finally:
        connection.close()

    return [dict(row) for row in rows]


def list_pending_invites_for_student(student_id: int) -> list[dict]:
    connection = get_connection()
    try:
        rows = connection.execute(
            """
            SELECT
                a.id,
                a.job_id,
                a.student_id,
                a.status,
                a.invited_at,
                j.company_id,
                j.title,
                j.job_description,
                j.required_skills_json,
                j.allow_active_backlog,
                j.min_cgpa,
                j.shortlist_count,
                j.application_deadline,
                c.username AS company_username
            FROM company_job_applications a
            JOIN company_job_posts j ON j.id = a.job_id
            JOIN company_accounts c ON c.id = j.company_id
            WHERE a.student_id = ? AND a.status = 'pending' AND j.status = 'open'
            ORDER BY a.id DESC
            """,
            (student_id,),
        ).fetchall()
    finally:
        connection.close()

    return [_hydrate_job(row) for row in rows]


def list_student_skill_keys(student_id: int) -> set[str]:
    connection = get_connection()
    try:
        rows = connection.execute(
            """
            SELECT normalized_skill
            FROM student_skills
            WHERE student_id = ?
            """,
            (student_id,),
        ).fetchall()
    finally:
        connection.close()
    return {str(row["normalized_skill"]) for row in rows}


def get_latest_skill_score(student_id: int, normalized_skill: str) -> float | None:
    connection = get_connection()
    try:
        row = connection.execute(
            """
            SELECT sa.score_percent
            FROM career_goals g
            JOIN career_goal_skills gs
                ON gs.goal_id = g.id
                AND gs.normalized_skill = ?
            JOIN skill_assessments sa
                ON sa.goal_skill_id = gs.id
            WHERE g.student_id = ?
              AND sa.submitted_at IS NOT NULL
              AND sa.score_percent IS NOT NULL
            ORDER BY sa.submitted_at DESC, sa.id DESC
            LIMIT 1
            """,
            (normalized_skill, student_id),
        ).fetchone()
    finally:
        connection.close()

    if row is None:
        return None
    return float(row["score_percent"])


def count_replan_notifications(student_id: int) -> int:
    connection = get_connection()
    try:
        row = connection.execute(
            """
            SELECT COUNT(*) AS total
            FROM user_notifications
            WHERE student_id = ? AND notification_type = 'roadmap_replanned'
            """,
            (student_id,),
        ).fetchone()
    finally:
        connection.close()
    return int(row["total"]) if row else 0
