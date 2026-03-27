from backend.roadmap_engine.storage.database import transaction


BASE_TABLE_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS students (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        branch TEXT NOT NULL,
        current_year INTEGER NOT NULL,
        weekly_study_hours INTEGER NOT NULL DEFAULT 8,
        cgpa REAL NOT NULL DEFAULT 7.0,
        has_active_backlog INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS student_skills (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        student_id INTEGER NOT NULL,
        skill_name TEXT NOT NULL,
        normalized_skill TEXT NOT NULL,
        skill_source TEXT NOT NULL,
        created_at TEXT NOT NULL,
        UNIQUE(student_id, normalized_skill),
        FOREIGN KEY(student_id) REFERENCES students(id) ON DELETE CASCADE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS career_goals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        student_id INTEGER NOT NULL,
        goal_text TEXT NOT NULL,
        target_company TEXT,
        target_role_family TEXT,
        target_duration_months INTEGER NOT NULL,
        start_date TEXT NOT NULL,
        target_end_date TEXT NOT NULL,
        llm_confidence REAL,
        requirements_json TEXT,
        status TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY(student_id) REFERENCES students(id) ON DELETE CASCADE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS career_goal_skills (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        goal_id INTEGER NOT NULL,
        skill_name TEXT NOT NULL,
        normalized_skill TEXT NOT NULL,
        priority INTEGER NOT NULL,
        estimated_hours REAL NOT NULL,
        skill_source TEXT NOT NULL,
        status TEXT NOT NULL,
        completed_at TEXT,
        created_at TEXT NOT NULL,
        UNIQUE(goal_id, normalized_skill),
        FOREIGN KEY(goal_id) REFERENCES career_goals(id) ON DELETE CASCADE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS roadmap_plans (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        goal_id INTEGER NOT NULL,
        start_date TEXT NOT NULL,
        end_date TEXT NOT NULL,
        status TEXT NOT NULL,
        last_replanned_at TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY(goal_id) REFERENCES career_goals(id) ON DELETE CASCADE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS roadmap_plan_tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        plan_id INTEGER NOT NULL,
        goal_skill_id INTEGER,
        task_date TEXT NOT NULL,
        title TEXT NOT NULL,
        description TEXT,
        target_minutes INTEGER NOT NULL,
        is_completed INTEGER NOT NULL DEFAULT 0,
        completed_at TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY(plan_id) REFERENCES roadmap_plans(id) ON DELETE CASCADE,
        FOREIGN KEY(goal_skill_id) REFERENCES career_goal_skills(id) ON DELETE SET NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS skill_assessments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        goal_id INTEGER NOT NULL,
        goal_skill_id INTEGER NOT NULL,
        attempt_no INTEGER NOT NULL,
        questions_json TEXT NOT NULL,
        answer_key_json TEXT NOT NULL,
        student_answers_json TEXT,
        score_percent REAL,
        passed INTEGER,
        feedback_text TEXT,
        created_at TEXT NOT NULL,
        submitted_at TEXT,
        FOREIGN KEY(goal_id) REFERENCES career_goals(id) ON DELETE CASCADE,
        FOREIGN KEY(goal_skill_id) REFERENCES career_goal_skills(id) ON DELETE CASCADE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS opportunity_match_cache (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        goal_id INTEGER NOT NULL,
        opportunity_id INTEGER NOT NULL,
        bucket TEXT NOT NULL,
        match_score REAL NOT NULL,
        required_skills_count INTEGER NOT NULL,
        matched_skills_count INTEGER NOT NULL,
        missing_skills_json TEXT NOT NULL,
        next_skills_json TEXT NOT NULL,
        eligible_now INTEGER NOT NULL DEFAULT 0,
        last_evaluated_at TEXT NOT NULL,
        UNIQUE(goal_id, opportunity_id),
        FOREIGN KEY(goal_id) REFERENCES career_goals(id) ON DELETE CASCADE,
        FOREIGN KEY(opportunity_id) REFERENCES opportunities(id) ON DELETE CASCADE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS user_notifications (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        student_id INTEGER NOT NULL,
        goal_id INTEGER,
        notification_type TEXT NOT NULL,
        title TEXT NOT NULL,
        body TEXT NOT NULL,
        related_opportunity_id INTEGER,
        is_read INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL,
        FOREIGN KEY(student_id) REFERENCES students(id) ON DELETE CASCADE,
        FOREIGN KEY(goal_id) REFERENCES career_goals(id) ON DELETE SET NULL,
        FOREIGN KEY(related_opportunity_id) REFERENCES opportunities(id) ON DELETE SET NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS company_accounts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL UNIQUE,
        password_hash TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS company_job_posts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_id INTEGER NOT NULL,
        title TEXT NOT NULL,
        job_description TEXT NOT NULL,
        required_skills_json TEXT NOT NULL,
        allow_active_backlog INTEGER NOT NULL DEFAULT 1,
        min_cgpa REAL NOT NULL,
        shortlist_count INTEGER NOT NULL,
        application_deadline TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'open',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY(company_id) REFERENCES company_accounts(id) ON DELETE CASCADE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS company_job_applications (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER NOT NULL,
        student_id INTEGER NOT NULL,
        status TEXT NOT NULL,
        invited_at TEXT NOT NULL,
        acted_at TEXT,
        updated_at TEXT NOT NULL,
        UNIQUE(job_id, student_id),
        FOREIGN KEY(job_id) REFERENCES company_job_posts(id) ON DELETE CASCADE,
        FOREIGN KEY(student_id) REFERENCES students(id) ON DELETE CASCADE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS company_job_shortlists (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER NOT NULL,
        student_id INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        UNIQUE(job_id, student_id),
        FOREIGN KEY(job_id) REFERENCES company_job_posts(id) ON DELETE CASCADE,
        FOREIGN KEY(student_id) REFERENCES students(id) ON DELETE CASCADE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS playlist_recommendations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        goal_id INTEGER NOT NULL,
        goal_skill_id INTEGER NOT NULL,
        playlist_id TEXT NOT NULL,
        title TEXT NOT NULL,
        channel_title TEXT,
        playlist_url TEXT NOT NULL,
        rank_score REAL NOT NULL,
        summary_json TEXT,
        created_at TEXT NOT NULL,
        FOREIGN KEY(goal_id) REFERENCES career_goals(id) ON DELETE CASCADE,
        FOREIGN KEY(goal_skill_id) REFERENCES career_goal_skills(id) ON DELETE CASCADE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS goal_skill_selected_playlists (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        goal_id INTEGER NOT NULL,
        goal_skill_id INTEGER NOT NULL,
        playlist_recommendation_id INTEGER NOT NULL,
        selected_at TEXT NOT NULL,
        UNIQUE(goal_skill_id),
        FOREIGN KEY(goal_id) REFERENCES career_goals(id) ON DELETE CASCADE,
        FOREIGN KEY(goal_skill_id) REFERENCES career_goal_skills(id) ON DELETE CASCADE,
        FOREIGN KEY(playlist_recommendation_id) REFERENCES playlist_recommendations(id) ON DELETE CASCADE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS skill_playlist_chat_sessions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        student_id INTEGER NOT NULL,
        goal_id INTEGER NOT NULL,
        goal_skill_id INTEGER NOT NULL,
        playlist_recommendation_id INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE(student_id, goal_skill_id, playlist_recommendation_id),
        FOREIGN KEY(student_id) REFERENCES students(id) ON DELETE CASCADE,
        FOREIGN KEY(goal_id) REFERENCES career_goals(id) ON DELETE CASCADE,
        FOREIGN KEY(goal_skill_id) REFERENCES career_goal_skills(id) ON DELETE CASCADE,
        FOREIGN KEY(playlist_recommendation_id) REFERENCES playlist_recommendations(id) ON DELETE CASCADE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS skill_playlist_chat_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id INTEGER NOT NULL,
        role TEXT NOT NULL,
        message_text TEXT NOT NULL,
        created_at TEXT NOT NULL,
        FOREIGN KEY(session_id) REFERENCES skill_playlist_chat_sessions(id) ON DELETE CASCADE
    );
    """,
]

LEGACY_TABLES_TO_DROP = [
    "selected_playlists",
    "roadmap_tasks",
    "skill_tests",
    "chat_messages",
    "chat_sessions",
    "roadmaps",
    "goal_skill_gaps",
    "student_goals",
]


INDEX_STATEMENTS = [
    "CREATE INDEX IF NOT EXISTS idx_student_skills_student ON student_skills(student_id);",
    "CREATE INDEX IF NOT EXISTS idx_career_goals_student_status ON career_goals(student_id, status);",
    "CREATE INDEX IF NOT EXISTS idx_goal_skills_goal_status ON career_goal_skills(goal_id, status);",
    "CREATE INDEX IF NOT EXISTS idx_plan_tasks_plan_date ON roadmap_plan_tasks(plan_id, task_date);",
    "CREATE INDEX IF NOT EXISTS idx_notifications_student_read ON user_notifications(student_id, is_read);",
    "CREATE INDEX IF NOT EXISTS idx_opportunity_match_goal_bucket ON opportunity_match_cache(goal_id, bucket);",
    "CREATE INDEX IF NOT EXISTS idx_company_jobs_company_status ON company_job_posts(company_id, status);",
    "CREATE INDEX IF NOT EXISTS idx_company_applications_job_status ON company_job_applications(job_id, status);",
    "CREATE INDEX IF NOT EXISTS idx_company_applications_student_status ON company_job_applications(student_id, status);",
    "CREATE INDEX IF NOT EXISTS idx_company_shortlists_job ON company_job_shortlists(job_id);",
    "CREATE INDEX IF NOT EXISTS idx_selected_playlist_skill ON goal_skill_selected_playlists(goal_skill_id);",
    "CREATE INDEX IF NOT EXISTS idx_chat_sessions_student_skill ON skill_playlist_chat_sessions(student_id, goal_skill_id);",
    "CREATE INDEX IF NOT EXISTS idx_chat_messages_session_id ON skill_playlist_chat_messages(session_id, id);",
]


def _table_columns(cursor, table_name: str) -> set[str]:
    rows = cursor.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {row[1] for row in rows}


def _ensure_legacy_compatibility(cursor) -> None:
    # Cleanup from any interrupted migration runs.
    cursor.execute("DROP TABLE IF EXISTS roadmap_plan_tasks_legacy")

    # Old `students` table may not contain the new weekly_study_hours column.
    student_columns = _table_columns(cursor, "students")
    if "weekly_study_hours" not in student_columns:
        cursor.execute(
            "ALTER TABLE students ADD COLUMN weekly_study_hours INTEGER NOT NULL DEFAULT 8"
        )
    if "cgpa" not in student_columns:
        cursor.execute(
            "ALTER TABLE students ADD COLUMN cgpa REAL NOT NULL DEFAULT 7.0"
        )
    if "has_active_backlog" not in student_columns:
        cursor.execute(
            "ALTER TABLE students ADD COLUMN has_active_backlog INTEGER NOT NULL DEFAULT 0"
        )

    # Old roadmap tasks table may contain minutes_spent. Rebuild without that column.
    task_columns = _table_columns(cursor, "roadmap_plan_tasks")
    if "minutes_spent" in task_columns:
        cursor.execute("ALTER TABLE roadmap_plan_tasks RENAME TO roadmap_plan_tasks_legacy")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS roadmap_plan_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                plan_id INTEGER NOT NULL,
                goal_skill_id INTEGER,
                task_date TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                target_minutes INTEGER NOT NULL,
                is_completed INTEGER NOT NULL DEFAULT 0,
                completed_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(plan_id) REFERENCES roadmap_plans(id) ON DELETE CASCADE,
                FOREIGN KEY(goal_skill_id) REFERENCES career_goal_skills(id) ON DELETE SET NULL
            );
            """
        )
        cursor.execute(
            """
            INSERT INTO roadmap_plan_tasks (
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
            )
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
            FROM roadmap_plan_tasks_legacy
            """
        )
        cursor.execute("DROP TABLE IF EXISTS roadmap_plan_tasks_legacy")

    # Recreate playlist recommendation table if it still has the old gap-based structure.
    playlist_columns = _table_columns(cursor, "playlist_recommendations")
    if "gap_id" in playlist_columns and "goal_skill_id" not in playlist_columns:
        cursor.execute("DROP TABLE IF EXISTS playlist_recommendations")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS playlist_recommendations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                goal_id INTEGER NOT NULL,
                goal_skill_id INTEGER NOT NULL,
                playlist_id TEXT NOT NULL,
                title TEXT NOT NULL,
                channel_title TEXT,
                playlist_url TEXT NOT NULL,
                rank_score REAL NOT NULL,
                summary_json TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(goal_id) REFERENCES career_goals(id) ON DELETE CASCADE,
                FOREIGN KEY(goal_skill_id) REFERENCES career_goal_skills(id) ON DELETE CASCADE
            );
            """
        )

    # Drop obsolete tables from the previous architecture.
    for table_name in LEGACY_TABLES_TO_DROP:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")


def init_roadmap_schema() -> None:
    with transaction() as connection:
        cursor = connection.cursor()
        for statement in BASE_TABLE_STATEMENTS:
            cursor.execute(statement)

        _ensure_legacy_compatibility(cursor)

        for statement in INDEX_STATEMENTS:
            cursor.execute(statement)
