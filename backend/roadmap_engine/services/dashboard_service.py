import ast
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
import json
import logging
import os
import re
import time

from backend.roadmap_engine.enhanced_assessment import coding_repo
from backend.roadmap_engine.enhanced_assessment.skill_gate import requires_coding_test
from backend.roadmap_engine.services.skill_normalizer import display_skill
from backend.roadmap_engine.storage.database import get_query_count, reset_query_count
from backend.roadmap_engine.storage import assessment_repo, goals_repo, matching_repo, roadmap_repo, students_repo
from backend.roadmap_engine.utils import parse_iso_deadline, utc_today

logger = logging.getLogger(__name__)


def _dashboard_perf_enabled() -> bool:
    raw = os.getenv("DASHBOARD_PERF_LOG", "")
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _dashboard_perf_log(message: str) -> None:
    if _dashboard_perf_enabled():
        text = f"[perf][dashboard] {message}"
        print(text)
        logger.info(text)


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


def _coding_test_items(
    goal_id: int,
    goal_skills: list[dict],
    ready_for_test_ids: set[int],
) -> list[dict]:
    if not ready_for_test_ids:
        return []

    attempts = assessment_repo.list_assessments_for_goal(goal_id, submitted_only=True, limit=2000)
    latest_by_skill: dict[int, dict] = {}
    for row in attempts:
        skill_id = int(row.get("goal_skill_id") or 0)
        if skill_id <= 0:
            continue
        latest_by_skill[skill_id] = row

    relevant_assessment_ids: list[int] = []
    for skill in goal_skills:
        skill_id = int(skill.get("id") or 0)
        if skill_id <= 0 or skill_id not in ready_for_test_ids:
            continue
        if str(skill.get("status", "")).strip().lower() == "completed":
            continue
        skill_name = str(skill.get("skill_name", "")).strip()
        if not requires_coding_test(skill_name):
            continue
        latest_mcq = latest_by_skill.get(skill_id)
        if latest_mcq is None:
            continue
        assessment_id = int(latest_mcq.get("id") or 0)
        if assessment_id > 0:
            relevant_assessment_ids.append(assessment_id)

    coding_by_assessment: dict[int, dict] = {}
    if relevant_assessment_ids:
        try:
            coding_by_assessment = coding_repo.list_coding_assessments_by_assessment_ids(
                relevant_assessment_ids
            )
        except Exception:
            coding_by_assessment = {}

    items: list[dict] = []
    for skill in goal_skills:
        skill_id = int(skill.get("id") or 0)
        if skill_id <= 0:
            continue
        if skill_id not in ready_for_test_ids:
            continue
        if str(skill.get("status", "")).strip().lower() == "completed":
            continue
        skill_name = str(skill.get("skill_name", "")).strip()
        if not requires_coding_test(skill_name):
            continue

        latest_mcq = latest_by_skill.get(skill_id)
        mcq_passed = bool(latest_mcq and latest_mcq.get("passed") == 1)

        coding_record = None
        if latest_mcq is not None:
            coding_record = coding_by_assessment.get(int(latest_mcq.get("id") or 0))

        coding_passed = bool(coding_record and coding_record.get("passed") == 1)
        if coding_passed:
            continue

        items.append(
            {
                "goal_skill_id": skill_id,
                "skill_name": skill_name,
                "label": f"Coding Test - {skill_name}",
                "can_take": mcq_passed,
                "lock_reason": "" if mcq_passed else "Pass MCQ test first.",
            }
        )

    return items


def _bucket_matches(matches: list[dict]) -> dict[str, list[dict]]:
    return {
        "eligible_now": [row for row in matches if row.get("bucket") == "eligible_now"],
        "almost_eligible": [row for row in matches if row.get("bucket") == "almost_eligible"],
        "coming_soon": [row for row in matches if row.get("bucket") == "coming_soon"],
    }


def _goal_skill_completion_forecast_from_tasks(
    goal_skills: list[dict],
    all_tasks: list[dict],
    *,
    today,
    days: int,
) -> dict[str, str]:
    horizon = today + timedelta(days=days)

    tasks_by_skill: dict[int, list[dict]] = {}
    for task in all_tasks:
        skill_id = int(task.get("goal_skill_id") or 0)
        if skill_id <= 0:
            continue
        tasks_by_skill.setdefault(skill_id, []).append(task)

    forecast: dict[str, str] = {}
    for skill in goal_skills:
        normalized_skill = str(skill.get("normalized_skill") or "").strip()
        if not normalized_skill:
            continue

        if str(skill.get("status") or "").strip().lower() == "completed":
            forecast[normalized_skill] = today.isoformat()
            continue

        skill_id = int(skill.get("id") or 0)
        if skill_id <= 0:
            continue

        tasks = tasks_by_skill.get(skill_id, [])
        if not tasks:
            continue

        incomplete_dates = []
        for task in tasks:
            if task.get("is_completed") == 1:
                continue
            parsed = parse_iso_deadline(task.get("task_date"))
            if parsed is not None:
                incomplete_dates.append(parsed)

        if not incomplete_dates:
            forecast[normalized_skill] = today.isoformat()
            continue

        latest_incomplete = max(incomplete_dates)
        if latest_incomplete <= horizon:
            forecast[normalized_skill] = latest_incomplete.isoformat()

    return forecast


def _forecast_eligible_from_cached_matches(
    matches: list[dict],
    *,
    current_keys: set[str],
    goal_skills: list[dict],
    all_tasks: list[dict],
    days: int,
) -> list[dict]:
    today = utc_today()
    completion_forecast = _goal_skill_completion_forecast_from_tasks(
        goal_skills,
        all_tasks,
        today=today,
        days=days,
    )
    projected_keys = set(current_keys) | set(completion_forecast.keys())

    display_lookup = {
        str(row.get("normalized_skill") or "").strip(): str(row.get("skill_name") or "").strip()
        for row in goal_skills
    }

    forecasted: list[dict] = []
    for item in matches:
        if item.get("bucket") == "eligible_now":
            continue

        missing = item.get("missing_skills", [])
        if not isinstance(missing, list) or not missing:
            continue

        if not all(skill in projected_keys for skill in missing):
            continue

        unlock_dates = [completion_forecast.get(skill, today.isoformat()) for skill in missing]
        predicted_date = max(unlock_dates) if unlock_dates else today.isoformat()

        forecasted.append(
            {
                "opportunity_id": item.get("opportunity_id"),
                "title": item.get("title"),
                "company": item.get("company"),
                "type": item.get("type"),
                "deadline": item.get("deadline"),
                "url": item.get("url"),
                "match_score": item.get("match_score", 0.0),
                "predicted_eligible_date": predicted_date,
                "skills_to_unlock": [
                    display_lookup.get(skill, display_skill(skill))
                    for skill in missing
                ],
            }
        )

    forecasted.sort(
        key=lambda row: (
            row["predicted_eligible_date"],
            -(row.get("match_score", 0.0) or 0.0),
        )
    )
    return forecasted[:25]


def get_dashboard(student_id: int, student: dict | None = None) -> dict:
    dashboard_started = time.perf_counter()
    reset_query_count()
    _dashboard_perf_log(f"START student_id={student_id}")
    threaded_query_count = 0

    def effective_query_count() -> int:
        return int(get_query_count() + threaded_query_count)

    def run_step(step_name: str, fn, *args, **kwargs):
        step_started = time.perf_counter()
        query_before = effective_query_count()
        _dashboard_perf_log(f"{step_name} start q={query_before}")
        try:
            result = fn(*args, **kwargs)
        except Exception as error:
            elapsed = time.perf_counter() - step_started
            query_after = effective_query_count()
            _dashboard_perf_log(
                f"{step_name} error {elapsed:.4f}s q+{query_after - query_before} total_q={query_after} err={error}"
            )
            raise
        elapsed = time.perf_counter() - step_started
        query_after = effective_query_count()
        _dashboard_perf_log(
            f"{step_name} done {elapsed:.4f}s q+{query_after - query_before} total_q={query_after}"
        )
        return result

    def _threaded_db_call(fn, args: tuple, kwargs: dict):
        # Each worker maintains its own query counter context.
        reset_query_count()
        result = fn(*args, **kwargs)
        return result, get_query_count()

    def run_parallel(
        step_name: str,
        calls: dict[str, tuple],
    ) -> dict[str, object]:
        nonlocal threaded_query_count

        step_started = time.perf_counter()
        query_before = effective_query_count()
        _dashboard_perf_log(f"{step_name} start q={query_before}")

        if not calls:
            _dashboard_perf_log(f"{step_name} done 0.0000s q+0 total_q={query_before}")
            return {}

        results: dict[str, object] = {}
        try:
            with ThreadPoolExecutor(max_workers=len(calls)) as executor:
                futures = {
                    key: executor.submit(_threaded_db_call, fn, args, kwargs)
                    for key, (fn, args, kwargs) in calls.items()
                }
                for key, future in futures.items():
                    value, thread_queries = future.result()
                    results[key] = value
                    query_delta = int(thread_queries or 0)
                    threaded_query_count += query_delta
                    _dashboard_perf_log(f"{step_name}:{key} done q+{query_delta}")
        except Exception as error:
            elapsed = time.perf_counter() - step_started
            query_after = effective_query_count()
            _dashboard_perf_log(
                f"{step_name} error {elapsed:.4f}s q+{query_after - query_before} total_q={query_after} err={error}"
            )
            raise

        elapsed = time.perf_counter() - step_started
        query_after = effective_query_count()
        _dashboard_perf_log(
            f"{step_name} done {elapsed:.4f}s q+{query_after - query_before} total_q={query_after}"
        )
        return results

    if student is None:
        student = run_step("_assert_student", _assert_student, student_id)
    goal, plan = run_step("_active_goal_and_plan", _active_goal_and_plan, student_id)
    today = utc_today()
    initial_reads = run_parallel(
        "parallel_initial_reads",
        {
            "profile_skills": (students_repo.list_student_skills, (student_id,), {}),
            "all_tasks": (roadmap_repo.list_tasks, (plan["id"],), {}),
        },
    )
    profile_skills = initial_reads["profile_skills"]
    all_tasks = initial_reads["all_tasks"]

    from backend.roadmap_engine.services import roadmap_adjustment_service

    incomplete_tasks = [task for task in all_tasks if task.get("is_completed") != 1]
    has_overdue_incomplete = False
    first_incomplete_date = None
    for task in incomplete_tasks:
        parsed_date = parse_iso_deadline(task.get("task_date"))
        if parsed_date is None:
            continue
        if first_incomplete_date is None:
            first_incomplete_date = parsed_date
        if parsed_date < today:
            has_overdue_incomplete = True
            break

    if not incomplete_tasks:
        replan_info: dict = {"applied": False, "reason": "no_incomplete_tasks"}
    elif has_overdue_incomplete:
        replan_info = run_step(
            "auto_replan_if_behind",
            roadmap_adjustment_service.auto_replan_if_behind,
            student_id,
        )
    elif first_incomplete_date is not None and first_incomplete_date > today:
        replan_info = run_step(
            "auto_pull_tasks_forward_if_ready",
            roadmap_adjustment_service.auto_pull_tasks_forward_if_ready,
            student_id,
        )
    else:
        replan_info = {"applied": False, "reason": "already_available"}

    if replan_info.get("applied"):
        plan = run_step("reload_active_plan", roadmap_repo.get_active_plan, goal["id"]) or plan
        all_tasks = run_step("reload_tasks_all", roadmap_repo.list_tasks, plan["id"])

    all_window_tasks = [task for task in all_tasks if str(task.get("task_date") or "") >= today.isoformat()]

    # lazy imports to avoid circular references
    from backend.roadmap_engine.services import (
        chatbot_service,
        company_service,
        matching_service,
        youtube_learning_service,
    )

    parallel_dashboard_reads = run_parallel(
        "parallel_dashboard_reads",
        {
            "goal_skills": (goals_repo.list_goal_skills, (goal["id"],), {}),
            "cached_matches": (matching_repo.list_matches_with_opportunities, (goal["id"],), {}),
            "notifications_raw": (matching_service.list_notifications, (student_id,), {}),
            "company_job_invites": (company_service.list_student_pending_company_jobs, (student_id,), {}),
            "test_history": (_test_history, (goal["id"],), {}),
        },
    )
    goal_skills = parallel_dashboard_reads["goal_skills"]
    cached_matches = parallel_dashboard_reads["cached_matches"]
    notifications_raw = parallel_dashboard_reads["notifications_raw"]
    company_job_invites = parallel_dashboard_reads["company_job_invites"]
    test_history = parallel_dashboard_reads["test_history"]

    active_skill = _active_skill(goal_skills)
    months_remaining = _goal_months_remaining(goal.get("target_end_date"), today)
    goal_target_date_display = _format_goal_target_date(goal.get("target_end_date"))

    # Dashboard request path stays read-only: use cached matches only.
    bucketed_matches = _bucket_matches(cached_matches)
    matches_for_forecast = cached_matches

    matches = run_step(
        "_attach_company_logos_by_bucket",
        _attach_company_logos_by_bucket,
        bucketed_matches,
    )

    current_keys = {
        str(item.get("normalized_skill") or "").strip()
        for item in profile_skills
        if str(item.get("normalized_skill") or "").strip()
    }
    for row in goal_skills:
        if str(row.get("status") or "").strip().lower() == "completed":
            normalized = str(row.get("normalized_skill") or "").strip()
            if normalized:
                current_keys.add(normalized)

    raw_forecast = run_step(
        "forecast_eligible_from_cached_matches",
        _forecast_eligible_from_cached_matches,
        matches_for_forecast,
        current_keys=current_keys,
        goal_skills=goal_skills,
        all_tasks=all_tasks,
        days=7,
    )
    forecast_7_days = run_step(
        "_attach_company_logos",
        _attach_company_logos,
        raw_forecast,
    )
    notifications = run_step(
        "_humanize_notifications",
        _humanize_notifications,
        notifications_raw,
    )

    selected_playlist = None
    recommendations = []
    playlist_recommendation_error = ""
    ready_for_test_ids: set[int] = set()

    if active_skill:
        recommendations, playlist_recommendation_error = run_step(
            "get_or_create_recommendations",
            youtube_learning_service.get_or_create_recommendations,
            goal_id=goal["id"],
            goal_skill_id=active_skill["id"],
            skill_name=active_skill["skill_name"],
        )
        recommendations = run_step(
            "_clean_recommendation_summaries",
            _clean_recommendation_summaries,
            recommendations,
        )
        selected_playlist = run_step(
            "get_selected_playlist",
            youtube_learning_service.get_selected_playlist,
            goal_id=goal["id"],
            goal_skill_id=active_skill["id"],
        )
        active_tasks = [
            task for task in all_tasks
            if task.get("goal_skill_id") == active_skill["id"]
        ]
        if selected_playlist and active_tasks and all(task["is_completed"] == 1 for task in active_tasks):
            ready_for_test_ids.add(active_skill["id"])

    chatbot_panel = run_step(
        "get_chat_panel_from_preloaded",
        chatbot_service.get_chat_panel_from_preloaded,
        student_id=student_id,
        goal=goal,
        active_skill=active_skill,
        selected_playlist=selected_playlist,
    )
    coding_tests = run_step("_coding_test_items", _coding_test_items, goal["id"], goal_skills, ready_for_test_ids)

    today_tasks = run_step(
        "build_today_tasks",
        lambda: [
            task
            for task in all_window_tasks
            if task["task_date"] == today.isoformat()
            and (active_skill is None or task["goal_skill_id"] == active_skill["id"])
        ],
    )
    upcoming_tasks = run_step(
        "build_upcoming_tasks",
        lambda: [
            task for task in all_window_tasks
            if active_skill is None or task["goal_skill_id"] == active_skill["id"]
        ],
    )

    payload = {
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
        "coding_tests": coding_tests,
    }

    _dashboard_perf_log(
        f"TOTAL {time.perf_counter() - dashboard_started:.4f}s total_q={effective_query_count()}"
    )
    return payload


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
