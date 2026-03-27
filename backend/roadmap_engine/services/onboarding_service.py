import math
from datetime import timedelta

from backend.roadmap_engine.constants import (
    BRANCH_OPTIONS,
    DEFAULT_SKILL_EFFORT_HOURS,
    DEFAULT_WEEKLY_STUDY_HOURS,
    MAX_WEEKLY_STUDY_HOURS,
    MIN_WEEKLY_STUDY_HOURS,
    PREDEFINED_SKILLS,
    SKILL_EFFORT_ESTIMATE_HOURS,
    TIMELINE_MONTH_OPTIONS,
    YEAR_OPTIONS,
)
from backend.roadmap_engine.services.goal_intelligence_service import parse_goal_text, synthesize_required_skills
from backend.roadmap_engine.services.skill_normalizer import deduplicate_skills, normalize_skill
from backend.roadmap_engine.storage import goals_repo, roadmap_repo, students_repo
from backend.roadmap_engine.utils import end_date_from_months, parse_custom_skills, utc_today


def _estimate_skill_hours(normalized_skill: str) -> float:
    return float(SKILL_EFFORT_ESTIMATE_HOURS.get(normalized_skill, DEFAULT_SKILL_EFFORT_HOURS))


def _normalize_required_skills(required_skills: list[str]) -> list[dict]:
    normalized = []
    seen = set()
    for priority, skill in enumerate(required_skills, start=1):
        cleaned = skill.strip()
        key = normalize_skill(cleaned)
        if not key or key in seen:
            continue
        seen.add(key)
        normalized.append(
            {
                "skill_name": cleaned,
                "normalized_skill": key,
                "priority": priority,
                "estimated_hours": _estimate_skill_hours(key),
                "skill_source": "goal_requirements",
            }
        )
    return normalized


def _build_tasks(
    *,
    skills_to_learn: list[dict],
    start_date,
    end_date,
    weekly_study_hours: int,
) -> list[dict]:
    total_days = max((end_date - start_date).days + 1, 1)
    total_minutes = int(sum(skill["estimated_hours"] for skill in skills_to_learn) * 60)
    if total_minutes <= 0:
        return []

    average_minutes_per_day = max(1, math.ceil(total_minutes / total_days))
    capacity_per_day = max(1, int((weekly_study_hours * 60) / 7))
    target_minutes_per_day = max(average_minutes_per_day, capacity_per_day)

    tasks: list[dict] = []
    day_offset = 0

    for skill in skills_to_learn:
        skill_minutes = int(skill["estimated_hours"] * 60)
        remaining = skill_minutes

        while remaining > 0 and day_offset < total_days:
            current_date = start_date + timedelta(days=day_offset)
            todays_minutes = min(target_minutes_per_day, remaining)
            tasks.append(
                {
                    "goal_skill_id": skill["id"],
                    "task_date": current_date.isoformat(),
                    "title": f"Learn {skill['skill_name']}",
                    "description": (
                        f"Roadmap practice for {skill['skill_name']}. "
                        "Watch the suggested playlist and complete notes/problems."
                    ),
                    "target_minutes": todays_minutes,
                }
            )
            remaining -= todays_minutes
            day_offset += 1

        if remaining > 0 and tasks:
            tasks[-1]["target_minutes"] += remaining

    return tasks


def create_student_goal_plan(
    *,
    name: str,
    branch: str,
    current_year: int,
    weekly_study_hours: int,
    cgpa: float,
    active_backlog: bool,
    selected_skills: list[str],
    custom_skills_text: str,
    goal_text: str,
    target_duration_months: int,
) -> dict:
    cleaned_name = name.strip()
    cleaned_goal_text = goal_text.strip()

    if not cleaned_name:
        raise ValueError("Name is required.")
    if branch not in BRANCH_OPTIONS:
        raise ValueError("Please select a valid branch.")
    if current_year not in YEAR_OPTIONS:
        raise ValueError("Please select a valid year.")
    if target_duration_months not in TIMELINE_MONTH_OPTIONS:
        raise ValueError("Please select a valid target timeline.")
    if not cleaned_goal_text:
        raise ValueError("Goal is required.")
    if weekly_study_hours < MIN_WEEKLY_STUDY_HOURS or weekly_study_hours > MAX_WEEKLY_STUDY_HOURS:
        raise ValueError(
            f"Weekly study hours must be between {MIN_WEEKLY_STUDY_HOURS} and {MAX_WEEKLY_STUDY_HOURS}."
        )
    try:
        cgpa_value = float(cgpa)
    except (TypeError, ValueError) as error:
        raise ValueError("CGPA must be a valid number between 0 and 10.") from error
    if cgpa_value < 0 or cgpa_value > 10:
        raise ValueError("CGPA must be between 0 and 10.")

    custom_skills = parse_custom_skills(custom_skills_text)
    all_known_skills = deduplicate_skills(selected_skills + custom_skills)
    if not all_known_skills:
        raise ValueError("Add at least one current skill.")

    student_id = students_repo.create_student(
        name=cleaned_name,
        branch=branch,
        current_year=current_year,
        weekly_study_hours=weekly_study_hours or DEFAULT_WEEKLY_STUDY_HOURS,
        cgpa=cgpa_value,
        has_active_backlog=bool(active_backlog),
    )

    predefined_normalized = {normalize_skill(skill) for skill in PREDEFINED_SKILLS}
    skill_rows = []
    for skill in all_known_skills:
        normalized = normalize_skill(skill)
        if not normalized:
            continue
        skill_rows.append(
            {
                "skill_name": skill.strip(),
                "normalized_skill": normalized,
                "skill_source": "predefined" if normalized in predefined_normalized else "custom",
            }
        )
    students_repo.replace_student_skills(student_id, skill_rows)

    goal_parse = parse_goal_text(cleaned_goal_text)
    requirements = synthesize_required_skills(
        goal_text=cleaned_goal_text,
        target_company=goal_parse.get("target_company"),
    )
    required_skills = _normalize_required_skills(requirements.get("required_skills", []))
    known_skill_keys = {row["normalized_skill"] for row in skill_rows}
    missing_skill_specs = [
        skill for skill in required_skills if skill["normalized_skill"] not in known_skill_keys
    ]

    start = utc_today()
    end = end_date_from_months(start, target_duration_months)
    goal_id = goals_repo.create_active_goal(
        student_id=student_id,
        goal_text=cleaned_goal_text,
        target_company=goal_parse.get("target_company"),
        target_role_family=goal_parse.get("target_role_family"),
        target_duration_months=target_duration_months,
        start_date=start.isoformat(),
        target_end_date=end.isoformat(),
        llm_confidence=goal_parse.get("confidence"),
        requirements={
            "goal_parse": goal_parse,
            "requirements_source": requirements.get("source"),
            "required_skills": [item["skill_name"] for item in required_skills],
            "source_opportunity_count": requirements.get("source_opportunity_count", 0),
            "rationale": requirements.get("rationale", ""),
        },
    )
    goals_repo.replace_goal_skills(goal_id, missing_skill_specs)

    goal_skills = goals_repo.list_goal_skills(goal_id)
    plan_id = roadmap_repo.create_or_replace_plan(goal_id, start.isoformat(), end.isoformat())
    roadmap_tasks = _build_tasks(
        skills_to_learn=goal_skills,
        start_date=start,
        end_date=end,
        weekly_study_hours=weekly_study_hours,
    )
    roadmap_repo.bulk_insert_tasks(plan_id, roadmap_tasks)

    student = students_repo.get_student(student_id)
    return {
        "student": student,
        "goal": goals_repo.get_active_goal(student_id),
        "known_skills": [row["skill_name"] for row in skill_rows],
        "required_skills": [row["skill_name"] for row in required_skills],
        "missing_skills": [row["skill_name"] for row in goal_skills],
        "plan_id": plan_id,
        "task_count": len(roadmap_tasks),
    }
