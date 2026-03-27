from backend.roadmap_engine.storage.database import get_connection, transaction
from backend.roadmap_engine.utils import utc_now_iso


def create_student(
    name: str,
    branch: str,
    current_year: int,
    weekly_study_hours: int,
    cgpa: float,
    has_active_backlog: bool,
) -> int:
    now = utc_now_iso()

    with transaction() as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            INSERT INTO students (
                name,
                branch,
                current_year,
                weekly_study_hours,
                cgpa,
                has_active_backlog,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                name,
                branch,
                current_year,
                weekly_study_hours,
                float(cgpa),
                1 if has_active_backlog else 0,
                now,
                now,
            ),
        )
        return int(cursor.lastrowid)


def get_student(student_id: int) -> dict | None:
    connection = get_connection()
    try:
        row = connection.execute(
            """
            SELECT
                id,
                name,
                branch,
                current_year,
                weekly_study_hours,
                cgpa,
                has_active_backlog,
                created_at,
                updated_at
            FROM students
            WHERE id = ?
            """,
            (student_id,),
        ).fetchone()
    finally:
        connection.close()

    if row is None:
        return None

    return dict(row)


def list_students() -> list[dict]:
    connection = get_connection()
    try:
        rows = connection.execute(
            """
            SELECT
                id,
                name,
                branch,
                current_year,
                weekly_study_hours,
                cgpa,
                has_active_backlog,
                created_at,
                updated_at
            FROM students
            ORDER BY id DESC
            """
        ).fetchall()
    finally:
        connection.close()

    return [dict(row) for row in rows]


def replace_student_skills(student_id: int, skills: list[dict]) -> None:
    now = utc_now_iso()

    with transaction() as connection:
        cursor = connection.cursor()
        cursor.execute(
            "DELETE FROM student_skills WHERE student_id = ?",
            (student_id,),
        )

        for skill in skills:
            cursor.execute(
                """
                INSERT INTO student_skills (
                    student_id,
                    skill_name,
                    normalized_skill,
                    skill_source,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    student_id,
                    skill["skill_name"],
                    skill["normalized_skill"],
                    skill["skill_source"],
                    now,
                ),
            )


def list_student_skills(student_id: int) -> list[dict]:
    connection = get_connection()
    try:
        rows = connection.execute(
            """
            SELECT id, skill_name, normalized_skill, skill_source, created_at
            FROM student_skills
            WHERE student_id = ?
            ORDER BY skill_name ASC
            """,
            (student_id,),
        ).fetchall()
    finally:
        connection.close()

    return [dict(row) for row in rows]


def add_student_skill(
    *,
    student_id: int,
    skill_name: str,
    normalized_skill: str,
    skill_source: str,
) -> None:
    now = utc_now_iso()
    with transaction() as connection:
        connection.execute(
            """
            INSERT OR IGNORE INTO student_skills (
                student_id, skill_name, normalized_skill, skill_source, created_at
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (student_id, skill_name, normalized_skill, skill_source, now),
        )
