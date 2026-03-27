from backend.mentor_module.storage import mentor_repo
from backend.roadmap_engine.storage.database import get_connection


# Badge thresholds by number of distinct students helped.
BADGE_THRESHOLDS = [
    (15, "gold"),
    (5, "silver"),
    (1, "bronze"),
]


def get_badge_level(people_helped: int) -> str | None:
    for threshold, badge in BADGE_THRESHOLDS:
        if people_helped >= threshold:
            return badge
    return None


def _get_test_score_for_skill(student_id: int, normalized_skill: str) -> float | None:
    conn = get_connection()
    try:
        row = conn.execute(
            """
            SELECT sa.score_percent
            FROM skill_assessments sa
            JOIN career_goal_skills gs ON gs.id = sa.goal_skill_id
            JOIN career_goals g ON g.id = sa.goal_id
            WHERE g.student_id = ?
              AND gs.normalized_skill = ?
              AND sa.passed = 1
              AND sa.submitted_at IS NOT NULL
            ORDER BY sa.submitted_at DESC
            LIMIT 1
            """,
            (student_id, normalized_skill),
        ).fetchone()
    finally:
        conn.close()
    return float(row["score_percent"]) if row else None


def _get_replan_count(student_id: int) -> int:
    conn = get_connection()
    try:
        row = conn.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM user_notifications
            WHERE student_id = ? AND notification_type = 'roadmap_replanned'
            """,
            (student_id,),
        ).fetchone()
    finally:
        conn.close()
    return int(row["cnt"]) if row else 0


def compute_mentor_grade(
    test_score: float,
    replan_count: int,
    people_helped: int,
) -> float:
    """
    Grade formula:
      50% test score
      30% consistency (fewer replans)
      20% help score (students helped)
    """
    consistency = max(40.0, 99.0 - replan_count * 9.0)
    help_score = min(100.0, people_helped * 6.67)
    return round(test_score * 0.50 + consistency * 0.30 + help_score * 0.20, 2)


def is_eligible_to_mentor(student_id: int, normalized_skill: str) -> bool:
    """
    Eligible only if the student completed the skill through the app:
    - career_goal_skills.status = 'completed'
    - skill_assessments.passed = 1 for that skill
    """
    conn = get_connection()
    try:
        row = conn.execute(
            """
            SELECT 1
            FROM skill_assessments sa
            JOIN career_goal_skills gs ON gs.id = sa.goal_skill_id
            JOIN career_goals g ON g.id = sa.goal_id
            WHERE g.student_id = ?
              AND gs.normalized_skill = ?
              AND gs.status = 'completed'
              AND sa.passed = 1
              AND sa.submitted_at IS NOT NULL
            LIMIT 1
            """,
            (student_id, normalized_skill),
        ).fetchone()
    finally:
        conn.close()
    return row is not None


def get_mentor_opt_in_status(student_id: int, normalized_skill: str) -> dict:
    eligible = is_eligible_to_mentor(student_id, normalized_skill)
    profile = mentor_repo.get_mentor_profile(student_id, normalized_skill)
    opted_in = bool(profile and profile.get("opted_in") == 1)
    return {
        "eligible": eligible,
        "opted_in": opted_in,
        "normalized_skill": normalized_skill,
    }


def list_mentor_skill_toggle_states(student_id: int) -> list[dict]:
    """
    Skills the student is eligible to mentor (completed + passed through app),
    along with current opt-in state and badge/help metrics.
    """
    conn = get_connection()
    try:
        rows = conn.execute(
            """
            WITH completed_skills AS (
                SELECT DISTINCT gs.normalized_skill
                FROM skill_assessments sa
                JOIN career_goal_skills gs ON gs.id = sa.goal_skill_id
                JOIN career_goals g ON g.id = sa.goal_id
                WHERE g.student_id = ?
                  AND gs.status = 'completed'
                  AND sa.passed = 1
                  AND sa.submitted_at IS NOT NULL
            )
            SELECT
                cs.normalized_skill,
                COALESCE(mp.opted_in, 0) AS opted_in,
                COALESCE(mp.people_helped, 0) AS people_helped,
                mp.badge_level
            FROM completed_skills cs
            LEFT JOIN mentor_profiles mp
                ON mp.student_id = ?
               AND mp.normalized_skill = cs.normalized_skill
            ORDER BY cs.normalized_skill ASC
            """,
            (student_id, student_id),
        ).fetchall()
    finally:
        conn.close()

    return [dict(row) for row in rows]


def opt_in(student_id: int, normalized_skill: str) -> None:
    if not is_eligible_to_mentor(student_id, normalized_skill):
        raise ValueError("You must complete this skill with a passing test to become a mentor.")
    profile = mentor_repo.get_mentor_profile(student_id, normalized_skill)
    if profile is None:
        mentor_repo.upsert_mentor_profile(student_id, normalized_skill)
    mentor_repo.set_opted_in(student_id, normalized_skill, True)


def opt_out(student_id: int, normalized_skill: str) -> None:
    profile = mentor_repo.get_mentor_profile(student_id, normalized_skill)
    if profile is None:
        mentor_repo.upsert_mentor_profile(student_id, normalized_skill)
    mentor_repo.set_opted_in(student_id, normalized_skill, False)


def get_ranked_mentors(normalized_skill: str, requesting_student_id: int) -> list[dict]:
    raw_mentors = mentor_repo.get_opted_in_mentors(
        normalized_skill,
        exclude_student_id=requesting_student_id,
    )

    enriched: list[dict] = []
    for mentor in raw_mentors:
        mentor_id = int(mentor["student_id"])
        test_score = _get_test_score_for_skill(mentor_id, normalized_skill)
        if test_score is None:
            continue

        replan_count = _get_replan_count(mentor_id)
        people_helped = int(mentor.get("people_helped") or 0)
        avg_rating = mentor_repo.get_avg_rating_for_mentor_skill(mentor_id, normalized_skill)
        grade = compute_mentor_grade(test_score, replan_count, people_helped)

        enriched.append(
            {
                **mentor,
                "test_score": round(test_score, 1),
                "replan_count": replan_count,
                "avg_rating": round(avg_rating, 1) if avg_rating is not None else None,
                "grade": grade,
            }
        )

    enriched.sort(
        key=lambda item: (item["grade"], item["test_score"], item["people_helped"]),
        reverse=True,
    )
    return enriched


def after_session_close(mentor_id: int, normalized_skill: str) -> dict:
    people_helped = mentor_repo.count_distinct_students_helped(mentor_id, normalized_skill)
    new_badge = get_badge_level(people_helped)

    profile = mentor_repo.get_mentor_profile(mentor_id, normalized_skill)
    old_badge = profile.get("badge_level") if profile else None

    mentor_repo.update_badge(mentor_id, normalized_skill, people_helped, new_badge)

    badge_upgraded = new_badge is not None and new_badge != old_badge
    return {
        "people_helped": people_helped,
        "new_badge": new_badge,
        "old_badge": old_badge,
        "badge_upgraded": badge_upgraded,
    }
