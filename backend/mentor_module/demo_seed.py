from datetime import timedelta

from backend.mentor_module.schema import init_mentor_schema
from backend.mentor_module.storage import mentor_repo
from backend.roadmap_engine.storage import assessment_repo, goals_repo, roadmap_repo, students_repo
from backend.roadmap_engine.storage.schema import init_roadmap_schema
from backend.roadmap_engine.utils import utc_now_iso, utc_today


def _create_mentor_demo_student() -> tuple[int, int]:
    today = utc_today()
    now_iso = utc_now_iso()

    student_id = students_repo.create_student(
        name="Mentor Demo Student",
        branch="CSE",
        current_year=3,
        weekly_study_hours=14,
        cgpa=8.6,
        has_active_backlog=False,
    )

    goal_id = goals_repo.create_active_goal(
        student_id=student_id,
        goal_text="Crack backend internship interviews",
        target_company="DemoCorp",
        target_role_family="Backend Engineering",
        target_duration_months=6,
        start_date=today.isoformat(),
        target_end_date=(today + timedelta(days=180)).isoformat(),
        llm_confidence=0.92,
        requirements={"required_skills": ["DSA", "SQL"]},
    )

    goals_repo.replace_goal_skills(
        goal_id,
        [
            {
                "skill_name": "Data Structures and Algorithms",
                "normalized_skill": "dsa",
                "priority": 1,
                "estimated_hours": 80,
                "skill_source": "roadmap",
            },
            {
                "skill_name": "SQL",
                "normalized_skill": "sql",
                "priority": 2,
                "estimated_hours": 30,
                "skill_source": "roadmap",
            },
        ],
    )

    goal_skills = goals_repo.list_goal_skills(goal_id)
    dsa_skill = next(skill for skill in goal_skills if skill["normalized_skill"] == "dsa")
    sql_skill = next(skill for skill in goal_skills if skill["normalized_skill"] == "sql")

    goals_repo.set_goal_skill_status(int(dsa_skill["id"]), "completed", now_iso)
    students_repo.add_student_skill(
        student_id=student_id,
        skill_name="Data Structures and Algorithms",
        normalized_skill="dsa",
        skill_source="roadmap_mastered",
    )

    plan_id = roadmap_repo.create_or_replace_plan(
        goal_id,
        start_date=today.isoformat(),
        end_date=(today + timedelta(days=180)).isoformat(),
    )
    roadmap_repo.bulk_insert_tasks(
        plan_id,
        [
            {
                "goal_skill_id": int(sql_skill["id"]),
                "task_date": (today + timedelta(days=1)).isoformat(),
                "title": "SQL joins and indexing",
                "description": "Complete SQL revision tasks.",
                "target_minutes": 90,
            }
        ],
    )

    questions = [
        {
            "topic": "Arrays",
            "difficulty": "basic",
            "question": "What is the time complexity of binary search on a sorted array?",
            "options": ["O(log n)", "O(n)", "O(n log n)", "O(1)"],
        },
        {
            "topic": "Trees",
            "difficulty": "basic",
            "question": "Which traversal visits Left, Root, Right?",
            "options": ["Inorder", "Preorder", "Postorder", "Level order"],
        },
    ]
    answer_key = [0, 0]

    assessment_id = assessment_repo.create_assessment(
        goal_id=goal_id,
        goal_skill_id=int(dsa_skill["id"]),
        questions=questions,
        answer_key=answer_key,
    )
    assessment_repo.submit_assessment(
        assessment_id=assessment_id,
        student_answers=[0, 0],
        score_percent=100.0,
        passed=True,
        feedback_text="Excellent DSA fundamentals.",
    )

    mentor_repo.upsert_mentor_profile(student_id, "dsa")
    mentor_repo.set_opted_in(student_id, "dsa", False)

    return student_id, assessment_id


def _create_seeker_demo_student() -> int:
    today = utc_today()

    student_id = students_repo.create_student(
        name="Seeker Demo Student",
        branch="IT",
        current_year=2,
        weekly_study_hours=10,
        cgpa=7.9,
        has_active_backlog=False,
    )

    goal_id = goals_repo.create_active_goal(
        student_id=student_id,
        goal_text="Prepare for software internship",
        target_company="DemoCorp",
        target_role_family="Software Engineering",
        target_duration_months=8,
        start_date=today.isoformat(),
        target_end_date=(today + timedelta(days=240)).isoformat(),
        llm_confidence=0.88,
        requirements={"required_skills": ["DSA", "Python"]},
    )

    goals_repo.replace_goal_skills(
        goal_id,
        [
            {
                "skill_name": "Data Structures and Algorithms",
                "normalized_skill": "dsa",
                "priority": 1,
                "estimated_hours": 80,
                "skill_source": "roadmap",
            },
            {
                "skill_name": "Python",
                "normalized_skill": "python",
                "priority": 2,
                "estimated_hours": 40,
                "skill_source": "roadmap",
            },
        ],
    )

    goal_skills = goals_repo.list_goal_skills(goal_id)
    dsa_skill = next(skill for skill in goal_skills if skill["normalized_skill"] == "dsa")

    plan_id = roadmap_repo.create_or_replace_plan(
        goal_id,
        start_date=today.isoformat(),
        end_date=(today + timedelta(days=240)).isoformat(),
    )
    roadmap_repo.bulk_insert_tasks(
        plan_id,
        [
            {
                "goal_skill_id": int(dsa_skill["id"]),
                "task_date": today.isoformat(),
                "title": "Solve 5 array problems",
                "description": "Practice beginner DSA problems.",
                "target_minutes": 75,
            }
        ],
    )

    return student_id


def main() -> None:
    init_roadmap_schema()
    init_mentor_schema()

    mentor_id, mentor_assessment_id = _create_mentor_demo_student()
    seeker_id = _create_seeker_demo_student()

    print("mentor_student_id=", mentor_id)
    print("mentor_assessment_id=", mentor_assessment_id)
    print("seeker_student_id=", seeker_id)
    print(
        "mentor_opt_in_url=",
        f"/students/{mentor_id}/skills/tests/{mentor_assessment_id}/result",
    )
    print("seeker_request_url=", f"/mentor/mentors?skill=dsa&student_id={seeker_id}")


if __name__ == "__main__":
    main()
