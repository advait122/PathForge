import ast
import json
import re

from backend.roadmap_engine.storage import assessment_repo, goals_repo, roadmap_repo, students_repo
from backend.roadmap_engine.utils import parse_iso_deadline, utc_today


def _assert_student(student_id: int) -> dict:
    student = students_repo.get_student(student_id)
    if student is None:
        raise ValueError("Student not found.")
    return student


def _active_goal_and_plan(student_id: int) -> tuple[dict, dict]:
    goal = goals_repo.get_active_goal(student_id)
    if goal is None:
        raise ValueError("No active goal found for this student.")

    plan = roadmap_repo.get_active_plan(goal["id"])
    if plan is None:
        raise ValueError("No active roadmap plan found.")
    return goal, plan


def _task_progress(tasks: list[dict]) -> dict:
    if not tasks:
        return {
            "completed_tasks": 0,
            "total_tasks": 0,
            "completion_percent": 0.0,
        }

    total_tasks = len(tasks)
    completed_tasks = sum(1 for task in tasks if task["is_completed"] == 1)
    completion_percent = (completed_tasks / total_tasks) * 100

    return {
        "completed_tasks": completed_tasks,
        "total_tasks": total_tasks,
        "completion_percent": completion_percent,
    }


def _active_skill(goal_skills: list[dict]) -> dict | None:
    pending = [item for item in goal_skills if item["status"] != "completed"]
    return pending[0] if pending else None


def _goal_months_remaining(target_end_date: str | None, today) -> int | None:
    target_end = parse_iso_deadline(target_end_date)
    if target_end is None:
        return None
    days_remaining = max((target_end - today).days, 0)
    if days_remaining == 0:
        return 0
    return max(1, round(days_remaining / 30))


def _format_goal_target_date(target_end_date: str | None) -> str:
    target_end = parse_iso_deadline(target_end_date)
    if target_end is None:
        return (target_end_date or "Not set").strip() or "Not set"
    return target_end.strftime(f"%B {target_end.day}, %Y")


def _humanize_summary_value(value: object) -> str:
    if value is None:
        return ""

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return ""

        parsed: object | None = None
        if text.startswith("{") or text.startswith("["):
            try:
                parsed = json.loads(text)
            except Exception:
                try:
                    parsed = ast.literal_eval(text)
                except Exception:
                    parsed = None
        if parsed is not None:
            return _humanize_summary_value(parsed)
        return text

    if isinstance(value, dict):
        lines: list[str] = []
        for key, val in value.items():
            cleaned = _humanize_summary_value(val)
            if not cleaned:
                continue
            key_label = str(key).replace("_", " ").strip().title()
            lines.append(f"{key_label}: {cleaned}")
        return "\n".join(lines)

    if isinstance(value, (list, tuple, set)):
        lines: list[str] = []
        for item in value:
            cleaned = _humanize_summary_value(item)
            if not cleaned:
                continue
            for segment in cleaned.splitlines():
                segment = segment.strip()
                if segment:
                    lines.append(f"- {segment}")
        return "\n".join(lines)

    return str(value).strip()


def _clean_recommendation_summaries(recommendations: list[dict]) -> list[dict]:
    cleaned: list[dict] = []
    for item in recommendations:
        normalized_item = dict(item)
        summary = normalized_item.get("summary", {}) or {}
        normalized_item["summary_human"] = {
            "topic_overview": _humanize_summary_value(summary.get("topic_overview")) or "Not available.",
            "learning_experience": (
                _humanize_summary_value(summary.get("learning_experience")) or "Not available."
            ),
            "topics_covered_summary": (
                _humanize_summary_value(summary.get("topics_covered_summary")) or "Not available."
            ),
        }
        cleaned.append(normalized_item)
    return cleaned


def _clean_notification_text(text: str) -> str:
    cleaned = " ".join(str(text or "").split())

    def pluralize(match: re.Match[str]) -> str:
        count = int(match.group(1))
        noun = match.group(2)
        if count == 1:
            return f"{count} {noun}"
        return f"{count} {noun}s"

    cleaned = re.sub(r"\b(\d+)\s+([A-Za-z]+)\(s\)", pluralize, cleaned)
    return cleaned.strip()


def _humanize_notification(note: dict) -> dict:
    item = dict(note)
    note_type = str(item.get("notification_type", "")).strip()
    title = str(item.get("title", "Notification")).strip() or "Notification"
    detail = _clean_notification_text(str(item.get("body", "")).strip())

    opportunity_title = str(item.get("opportunity_title", "")).strip()
    opportunity_company = str(item.get("opportunity_company", "")).strip()
    opportunity_url = str(item.get("opportunity_url", "")).strip()

    item["ui_link_text"] = ""
    item["ui_link_url"] = ""
    item["ui_detail_prefix"] = ""
    item["ui_detail_suffix"] = ""

    if note_type == "newly_eligible":
        if opportunity_title:
            title = f"Now eligible: {opportunity_title}"
        else:
            title = "You are now eligible"
        if opportunity_title:
            item["ui_link_text"] = opportunity_title
            if opportunity_url:
                item["ui_link_url"] = opportunity_url
            item["ui_detail_prefix"] = "You are now eligible to apply for "
            if opportunity_company:
                item["ui_detail_suffix"] = f" at {opportunity_company}."
            else:
                item["ui_detail_suffix"] = "."
            detail = (
                f"{item['ui_detail_prefix']}{opportunity_title}{item['ui_detail_suffix']}"
            )

    elif note_type == "deadline_alert":
        if opportunity_title:
            title = f"Deadline soon: {opportunity_title}"
        else:
            title = "Application deadline approaching"

        days_match = re.search(r"closes in (\d+)\s+day", detail, flags=re.IGNORECASE)
        status_match = re.search(r"Status:\s*([^\.]+)", detail, flags=re.IGNORECASE)

        detail_segments: list[str] = []
        if opportunity_title:
            item["ui_link_text"] = opportunity_title
            if opportunity_url:
                item["ui_link_url"] = opportunity_url
            if opportunity_company:
                detail_segments.append(f"at {opportunity_company}")

        if days_match:
            days = int(days_match.group(1))
            day_word = "day" if days == 1 else "days"
            detail_segments.append(f"closes in {days} {day_word}")

        if detail_segments:
            item["ui_detail_suffix"] = " " + " ".join(detail_segments).strip() + "."
            detail = (
                f"{opportunity_title}{item['ui_detail_suffix']}"
                if opportunity_title
                else " ".join(detail_segments).strip() + "."
            )

        if status_match:
            status = status_match.group(1).strip().replace("_", " ")
            if status:
                detail = f"{detail} Current eligibility: {status}.".strip()
                if opportunity_title:
                    item["ui_detail_suffix"] = (
                        f"{item['ui_detail_suffix']} Current eligibility: {status}."
                    )

    elif note_type == "skill_test_passed":
        title = "Skill test passed"
        detail = detail or "Great job. Your skill has been marked as completed."

    elif note_type == "skill_test_failed":
        title = "Skill test retry needed"
        detail = detail or "Review the suggested topics and try the test again."

    elif note_type == "roadmap_replanned":
        title = "Roadmap updated"
        detail = detail or "Your pending tasks were rescheduled to keep your plan on track."

    item["ui_title"] = title
    item["ui_detail"] = detail or "More details are not available."
    return item


def _humanize_notifications(notifications: list[dict]) -> list[dict]:
    return [_humanize_notification(item) for item in notifications]


_COMPANY_LOGO_DOMAIN_MAP: dict[str, str] = {
    "amazon": "amazon.com",
    "google": "google.com",
    "microsoft": "microsoft.com",
    "meta": "meta.com",
    "netflix": "netflix.com",
    "apple": "apple.com",
    "openai": "openai.com",
    "cohere": "cohere.com",
    "nvidia": "nvidia.com",
    "intel": "intel.com",
    "adobe": "adobe.com",
    "salesforce": "salesforce.com",
    "oracle": "oracle.com",
    "ibm": "ibm.com",
    "uber": "uber.com",
    "airbnb": "airbnb.com",
    "atlassian": "atlassian.com",
    "twitter": "x.com",
    "x": "x.com",
    "x twitter": "x.com",
}

_COMPANY_BASE_LOCATION_MAP: dict[str, tuple[str, str, str]] = {
    "google": ("United States", "California", "Mountain View"),
    "amazon": ("United States", "Washington", "Seattle"),
    "microsoft": ("United States", "Washington", "Redmond"),
    "meta": ("United States", "California", "Menlo Park"),
    "netflix": ("United States", "California", "Los Gatos"),
    "apple": ("United States", "California", "Cupertino"),
    "openai": ("United States", "California", "San Francisco"),
    "cohere": ("Canada", "Ontario", "Toronto"),
    "nvidia": ("United States", "California", "Santa Clara"),
    "intel": ("United States", "California", "Santa Clara"),
    "adobe": ("United States", "California", "San Jose"),
    "salesforce": ("United States", "California", "San Francisco"),
    "oracle": ("United States", "Texas", "Austin"),
    "ibm": ("United States", "New York", "Armonk"),
    "uber": ("United States", "California", "San Francisco"),
    "airbnb": ("United States", "California", "San Francisco"),
    "atlassian": ("Australia", "New South Wales", "Sydney"),
    "x": ("United States", "California", "San Francisco"),
    "twitter": ("United States", "California", "San Francisco"),
    "verily life sciences": ("United States", "California", "South San Francisco"),
}

_LOCATION_KEYWORDS: list[tuple[str, tuple[str, str, str]]] = [
    ("bengaluru", ("India", "Karnataka", "Bengaluru")),
    ("bangalore", ("India", "Karnataka", "Bengaluru")),
    ("hyderabad", ("India", "Telangana", "Hyderabad")),
    ("pune", ("India", "Maharashtra", "Pune")),
    ("mumbai", ("India", "Maharashtra", "Mumbai")),
    ("delhi", ("India", "Delhi", "New Delhi")),
    ("gurugram", ("India", "Haryana", "Gurugram")),
    ("noida", ("India", "Uttar Pradesh", "Noida")),
    ("chennai", ("India", "Tamil Nadu", "Chennai")),
    ("remote", ("Global", "Remote", "Remote")),
    ("london", ("United Kingdom", "England", "London")),
    ("new york", ("United States", "New York", "New York")),
    ("san francisco", ("United States", "California", "San Francisco")),
    ("seattle", ("United States", "Washington", "Seattle")),
]


def _normalize_company_key(company_name: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", company_name.lower()).strip()


def _company_logo_domain(company_name: str) -> str:
    normalized = _normalize_company_key(company_name)
    if not normalized:
        return ""

    mapped = _COMPANY_LOGO_DOMAIN_MAP.get(normalized)
    if mapped:
        return mapped

    tokens = normalized.split()
    if not tokens:
        return ""

    first_token = tokens[0]
    mapped_first = _COMPANY_LOGO_DOMAIN_MAP.get(first_token)
    if mapped_first:
        return mapped_first

    if first_token.isdigit():
        return ""
    return f"{first_token}.com"


def _company_logo_url(company_name: str) -> str:
    domain = _company_logo_domain(company_name)
    if not domain:
        return ""
    return f"https://www.google.com/s2/favicons?sz=128&domain={domain}"


def _derive_work_mode(title: str, opportunity_type: str) -> str:
    haystack = f"{title} {opportunity_type}".lower()
    if any(token in haystack for token in ("remote", "work from home", "wfh", "any location")):
        return "Remote"
    if "hybrid" in haystack:
        return "Hybrid"
    if any(token in haystack for token in ("onsite", "on-site", "in office", "office")):
        return "On-site"
    if str(opportunity_type).strip().lower() == "hackathon":
        return "Remote"
    return "Any"


def _derive_location(company_name: str, title: str) -> tuple[str, str, str]:
    haystack = f"{company_name} {title}".lower()
    for keyword, location_tuple in _LOCATION_KEYWORDS:
        if keyword in haystack:
            return location_tuple

    normalized_company = _normalize_company_key(company_name)
    mapped = _COMPANY_BASE_LOCATION_MAP.get(normalized_company)
    if mapped:
        return mapped

    first_token = normalized_company.split()[0] if normalized_company else ""
    if first_token:
        mapped_first = _COMPANY_BASE_LOCATION_MAP.get(first_token)
        if mapped_first:
            return mapped_first

    return ("Global", "Any", "Any")


def _attach_company_logos(items: list[dict]) -> list[dict]:
    enriched: list[dict] = []
    for row in items:
        company_name = str(row.get("company", "")).strip()
        title_text = str(row.get("title", "")).strip()
        opportunity_type = str(row.get("type", "")).strip()
        country, state, city = _derive_location(company_name, title_text)
        enriched.append(
            {
                **row,
                "company_logo_url": _company_logo_url(company_name),
                "work_mode": _derive_work_mode(title_text, opportunity_type),
                "country": country,
                "state": state,
                "city": city,
            }
        )
    return enriched


def _attach_company_logos_by_bucket(bucketed: dict[str, list[dict]]) -> dict[str, list[dict]]:
    return {
        bucket: _attach_company_logos(rows or [])
        for bucket, rows in bucketed.items()
    }


def _test_history(goal_id: int) -> list[dict]:
    attempts = assessment_repo.list_assessments_for_goal(goal_id, submitted_only=True, limit=1500)
    history: list[dict] = []
    per_skill_attempt_counter: dict[int, int] = {}
    for row in attempts:
        score = row.get("score_percent")
        try:
            score_value = float(score) if score is not None else None
        except (TypeError, ValueError):
            score_value = None

        goal_skill_id = int(row.get("goal_skill_id") or 0)
        current_counter = per_skill_attempt_counter.get(goal_skill_id, 0) + 1
        per_skill_attempt_counter[goal_skill_id] = current_counter

        passed_value = row.get("passed")
        if passed_value == 1:
            result_label = "Passed"
        elif passed_value == 0:
            result_label = "Failed"
        else:
            result_label = "Pending"

        history.append(
            {
                **row,
                "score_percent": score_value,
                "result_label": result_label,
                "score_display": f"{score_value:.1f}%" if score_value is not None else "-",
                "display_attempt_no": current_counter,
            }
        )
    return history


def get_dashboard(student_id: int) -> dict:
    student = _assert_student(student_id)
    goal, plan = _active_goal_and_plan(student_id)
    profile_skills = students_repo.list_student_skills(student_id)

    from backend.roadmap_engine.services import roadmap_adjustment_service

    replan_info = roadmap_adjustment_service.auto_replan_if_behind(student_id)
    if replan_info.get("applied"):
        plan = roadmap_repo.get_active_plan(goal["id"]) or plan

    today = utc_today()
    months_remaining = _goal_months_remaining(goal.get("target_end_date"), today)
    goal_target_date_display = _format_goal_target_date(goal.get("target_end_date"))
    all_tasks = roadmap_repo.list_tasks(plan["id"])
    all_window_tasks = roadmap_repo.list_tasks(plan["id"], today.isoformat(), None)
    goal_skills = goals_repo.list_goal_skills(goal["id"])
    active_skill = _active_skill(goal_skills)

    # lazy imports to avoid circular references
    from backend.roadmap_engine.services import (
        chatbot_service,
        company_service,
        matching_service,
        youtube_learning_service,
    )

    matches = _attach_company_logos_by_bucket(
        matching_service.refresh_opportunity_matches(student_id)
    )
    forecast_7_days = _attach_company_logos(
        matching_service.forecast_eligible_in_days(student_id, days=7)
    )
    notifications = _humanize_notifications(matching_service.list_notifications(student_id))

    selected_playlist = None
    recommendations = []
    playlist_recommendation_error = ""
    ready_for_test_ids: set[int] = set()

    if active_skill:
        recommendations, playlist_recommendation_error = youtube_learning_service.get_or_create_recommendations(
            goal_id=goal["id"],
            goal_skill_id=active_skill["id"],
            skill_name=active_skill["skill_name"],
        )
        recommendations = _clean_recommendation_summaries(recommendations)
        selected_playlist = youtube_learning_service.get_selected_playlist(
            goal_id=goal["id"],
            goal_skill_id=active_skill["id"],
        )
        active_tasks = roadmap_repo.list_tasks_for_skill(plan["id"], active_skill["id"])
        if selected_playlist and active_tasks and all(task["is_completed"] == 1 for task in active_tasks):
            ready_for_test_ids.add(active_skill["id"])

    chatbot_panel = chatbot_service.get_chat_panel(student_id)
    company_job_invites = company_service.list_student_pending_company_jobs(student_id)
    test_history = _test_history(goal["id"])

    today_tasks = [
        task for task in all_window_tasks
        if task["task_date"] == today.isoformat() and (active_skill is None or task["goal_skill_id"] == active_skill["id"])
    ]
    upcoming_tasks = [
        task for task in all_window_tasks
        if active_skill is None or task["goal_skill_id"] == active_skill["id"]
    ]

    return {
        "student": student,
        "goal": goal,
        "goal_months_remaining": months_remaining,
        "goal_target_date_display": goal_target_date_display,
        "plan": plan,
        "today": today.isoformat(),
        "known_skills": [item["skill_name"] for item in profile_skills],
        "required_skills": goal.get("requirements", {}).get("required_skills", []),
        "goal_skills": [
            {
                **skill,
                "ready_for_test": skill["id"] in ready_for_test_ids,
                "is_active": bool(active_skill and skill["id"] == active_skill["id"]),
                "is_locked": bool(active_skill and skill["id"] != active_skill["id"] and skill["status"] != "completed"),
            }
            for skill in goal_skills
        ],
        "replan_info": replan_info,
        "progress": _task_progress(all_tasks),
        "today_tasks": today_tasks,
        "upcoming_tasks": upcoming_tasks,
        "opportunities": matches,
        "opportunity_forecast_7_days": forecast_7_days,
        "notifications": notifications,
        "active_skill": active_skill,
        "active_skill_recommendations": recommendations,
        "playlist_recommendation_error": playlist_recommendation_error,
        "selected_playlist": selected_playlist,
        "chatbot": chatbot_panel,
        "company_job_invites": company_job_invites,
        "test_history": test_history,
    }


def set_task_completion(student_id: int, task_id: int, completed: bool) -> None:
    _assert_student(student_id)
    goal, plan = _active_goal_and_plan(student_id)

    goal_skills = goals_repo.list_goal_skills(goal["id"])
    active_skill = _active_skill(goal_skills)
    if active_skill is None:
        raise ValueError("All skills are already completed.")

    from backend.roadmap_engine.services import youtube_learning_service

    selected_playlist = youtube_learning_service.get_selected_playlist(goal["id"], active_skill["id"])
    if selected_playlist is None:
        raise ValueError(
            f"Select one of the top 3 playlists for {active_skill['skill_name']} before marking tasks."
        )

    task = roadmap_repo.get_task(task_id)
    if task is None or task["plan_id"] != plan["id"]:
        raise ValueError("Task not found for this student.")
    if task.get("goal_skill_id") != active_skill["id"]:
        raise ValueError(
            f"Only {active_skill['skill_name']} tasks are unlocked right now. Complete this skill first."
        )

    roadmap_repo.set_task_completed(task_id, completed)

    if task["goal_skill_id"]:
        skill_tasks = roadmap_repo.list_tasks_for_skill(plan["id"], task["goal_skill_id"])
        if skill_tasks and all(item["is_completed"] == 1 for item in skill_tasks):
            goals_repo.set_goal_skill_status(task["goal_skill_id"], "in_progress", None)

    from backend.roadmap_engine.services import matching_service

    matching_service.refresh_opportunity_matches(student_id)
