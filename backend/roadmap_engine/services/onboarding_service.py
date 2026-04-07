import hashlib
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
from backend.roadmap_engine.services.agent_orchestrator_service import generate_verified_roadmap
from backend.roadmap_engine.services.skill_normalizer import deduplicate_skills, display_skill, normalize_skill
from backend.roadmap_engine.storage import goals_repo, roadmap_repo, students_repo
from backend.roadmap_engine.utils import end_date_from_months, parse_custom_skills, utc_today


def _normalize_login_name(name: str) -> str:
    return (name or "").strip().lower()


def _hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


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
    validation_result: dict | None = None,
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
    validation_result = validation_result or {}

    for skill in skills_to_learn:
        skill_minutes = int(skill["estimated_hours"] * 60)
        remaining = skill_minutes
        skill_task_count = max(1, math.ceil(skill_minutes / target_minutes_per_day))
        skill_task_index = 0

        while remaining > 0 and day_offset < total_days:
            current_date = start_date + timedelta(days=day_offset)
            todays_minutes = min(target_minutes_per_day, remaining)
            title, description = _task_content_for_skill(
                skill=skill,
                task_index=skill_task_index,
                task_count=skill_task_count,
                validation_result=validation_result,
            )
            tasks.append(
                {
                    "goal_skill_id": skill["id"],
                    "task_date": current_date.isoformat(),
                    "title": title,
                    "description": description,
                    "target_minutes": todays_minutes,
                }
            )
            remaining -= todays_minutes
            day_offset += 1
            skill_task_index += 1

        if remaining > 0 and tasks:
            tasks[-1]["target_minutes"] += remaining

    return tasks


def _skill_in_list(skill_name: str, items: list[str]) -> bool:
    target = normalize_skill(skill_name)
    return any(normalize_skill(item) == target for item in items)


def _task_content_for_skill(
    *,
    skill: dict,
    task_index: int,
    task_count: int,
    validation_result: dict,
) -> tuple[str, str]:
    skill_name = skill["skill_name"]
    normalized_skill = skill.get("normalized_skill", "")
    interview_topics = [str(item) for item in validation_result.get("interview_topics", [])]
    project_recommendations = [str(item) for item in validation_result.get("project_recommendations", [])]
    weak_skills = [str(item) for item in validation_result.get("weak_skills", [])]
    sequence_adjustments = [str(item) for item in validation_result.get("sequence_adjustments", [])]

    is_early_foundation = any(skill_name in note for note in sequence_adjustments)
    needs_extra_interview_focus = _skill_in_list(skill_name, weak_skills) or any(
        normalized_skill and normalize_skill(topic) == normalized_skill for topic in interview_topics
    )
    supports_backend_project = (
        "Backend API Project" in project_recommendations
        and normalized_skill in {"python", "java", "sql", "api", "git", "linux"}
    )

    if task_count == 1:
        title = f"Learn {skill_name}"
        description = (
            f"Build a strong base in {skill_name}. Watch the suggested playlist, take notes, "
            "and solve a few focused practice problems."
        )
        return title, description

    if task_index == 0:
        title = f"Build {skill_name} Foundations"
        description = (
            f"Cover the core concepts of {skill_name} first. Focus on understanding fundamentals, "
            "keeping notes, and identifying the ideas you should revise later."
        )
        if is_early_foundation:
            description += " This skill should be treated as an early roadmap priority."
        return title, description

    if task_index == task_count - 1:
        if needs_extra_interview_focus:
            title = f"Practice {skill_name} Interview Questions"
            description = (
                f"Revise {skill_name} with interview-style questions and timed practice. "
                "Summarize mistakes and convert weak areas into revision notes."
            )
        else:
            title = f"Revise {skill_name}"
            description = (
                f"Consolidate {skill_name} with recap notes, targeted practice, and a short self-check "
                "to confirm retention."
            )
        return title, description

    midpoint = max(1, task_count // 2)
    if task_index == midpoint and supports_backend_project:
        title = f"Apply {skill_name} in a Project"
        description = (
            f"Use {skill_name} in a small backend-focused project task. Build something practical, "
            "connect it to your ongoing roadmap work, and document what you implemented."
        )
        return title, description

    if needs_extra_interview_focus and task_index >= max(1, task_count - 2):
        title = f"Strengthen {skill_name} Problem Solving"
        description = (
            f"Spend this session on deeper practice in {skill_name}. Mix conceptual revision with "
            "interview-style questions and write down common pitfalls."
        )
        return title, description

    title = f"Practice {skill_name}"
    description = (
        f"Continue hands-on practice for {skill_name}. Combine learning from the playlist with notes, "
        "small exercises, and examples that improve confidence."
    )
    return title, description


def create_student_goal_plan(
    *,
    name: str,
    password: str,
    confirm_password: str,
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
    login_name = _normalize_login_name(cleaned_name)
    cleaned_goal_text = goal_text.strip()

    if not cleaned_name:
        raise ValueError("Name is required.")
    if not login_name:
        raise ValueError("Name is required.")
    if not password:
        raise ValueError("Password is required.")
    if len(password) < 6:
        raise ValueError("Password must be at least 6 characters.")
    if password != confirm_password:
        raise ValueError("Passwords do not match.")
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

    if students_repo.get_student_account_by_username(login_name):
        raise ValueError("Name already exists. Please login instead.")

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

    requirements = generate_verified_roadmap(
        goal_text=cleaned_goal_text,
        target_duration_months=target_duration_months,
        known_skills=[row["skill_name"] for row in skill_rows],
        weekly_study_hours=weekly_study_hours,
    )
    goal_parse = requirements.get("goal_parse", {})
    if not bool(goal_parse.get("is_valid", True)):
        raise ValueError(goal_parse.get("rejection_reason") or "Please enter a clearer goal.")

    student_id = students_repo.create_student(
        name=cleaned_name,
        branch=branch,
        current_year=current_year,
        weekly_study_hours=weekly_study_hours or DEFAULT_WEEKLY_STUDY_HOURS,
        cgpa=cgpa_value,
        has_active_backlog=bool(active_backlog),
    )
    students_repo.create_student_account(
        student_id=student_id,
        username=login_name,
        password_hash=_hash_password(password),
    )
    students_repo.replace_student_skills(student_id, skill_rows)
    selected_skill_details = requirements.get("selected_skill_details", [])
    if selected_skill_details:
        required_skills = []
        seen: set[str] = set()
        for skill in selected_skill_details:
            normalized = normalize_skill(str(skill.get("normalized_skill") or skill.get("skill_name") or ""))
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            required_skills.append(
                {
                    "skill_name": str(skill.get("skill_name") or display_skill(normalized)),
                    "normalized_skill": normalized,
                    "priority": int(skill.get("priority") or (len(required_skills) + 1)),
                    "estimated_hours": float(skill.get("estimated_hours") or _estimate_skill_hours(normalized)),
                    "skill_source": str(skill.get("skill_source") or "goal_requirements"),
                }
            )
    else:
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
            "role_intent": requirements.get("role_intent", {}),
            "evidence_summary": requirements.get("evidence_summary", {}),
            "evidence_highlights": requirements.get("evidence_highlights", []),
            "validation_result": requirements.get("validation_result", {}),
            "verification_result": requirements.get("verification_result", {}),
            "agent_trace_id": requirements.get("agent_trace_id"),
            "selected_skill_details": required_skills,
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
        validation_result=requirements.get("validation_result", {}),
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


def login_student(*, name: str, password: str) -> dict:
    login_name = _normalize_login_name(name)
    if not login_name or not password:
        raise ValueError("Name and password are required.")

    account = students_repo.get_student_account_by_username(login_name)
    if account is None:
        raise ValueError("Invalid name or password.")
    if account["password_hash"] != _hash_password(password):
        raise ValueError("Invalid name or password.")

    student = students_repo.get_student(int(account["student_id"]))
    if student is None:
        raise ValueError("Student account is invalid. Please sign up again.")
    return student
