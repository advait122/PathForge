from datetime import timedelta

from backend.roadmap_engine.services.skill_normalizer import display_skill, normalize_skill
from backend.roadmap_engine.storage import goals_repo, matching_repo, opportunities_repo, roadmap_repo, students_repo
from backend.roadmap_engine.utils import parse_iso_deadline, utc_today


def _goal_skill_completion_forecast(goal_id: int, days: int) -> dict[str, str]:
    plan = roadmap_repo.get_active_plan(goal_id)
    if not plan:
        return {}

    today = utc_today()
    horizon = today + timedelta(days=days)
    forecast: dict[str, str] = {}
    goal_skills = goals_repo.list_goal_skills(goal_id)
    all_tasks = roadmap_repo.list_tasks(plan["id"])
    tasks_by_skill: dict[int, list[dict]] = {}
    for task in all_tasks:
        skill_id = int(task.get("goal_skill_id") or 0)
        if skill_id <= 0:
            continue
        tasks_by_skill.setdefault(skill_id, []).append(task)

    for skill in goal_skills:
        if skill["status"] == "completed":
            forecast[skill["normalized_skill"]] = today.isoformat()
            continue
        tasks = tasks_by_skill.get(int(skill["id"]), [])
        if not tasks:
            continue
        incomplete_dates = []
        for task in tasks:
            if task["is_completed"] == 1:
                continue
            parsed = parse_iso_deadline(task.get("task_date"))
            if parsed is not None:
                incomplete_dates.append(parsed)
        if not incomplete_dates:
            forecast[skill["normalized_skill"]] = today.isoformat()
            continue
        latest_incomplete = max(incomplete_dates)
        if latest_incomplete <= horizon:
            forecast[skill["normalized_skill"]] = latest_incomplete.isoformat()
    return forecast


def forecast_eligible_in_days(student_id: int, days: int = 7) -> list[dict]:
    goal = goals_repo.get_active_goal(student_id)
    if goal is None:
        return []

    current_keys, _ = _current_skill_state(student_id, goal["id"])
    completion_forecast = _goal_skill_completion_forecast(goal["id"], days)
    projected_keys = set(current_keys) | set(completion_forecast.keys())
    goal_skill_rows = goals_repo.list_goal_skills(goal["id"])
    display_lookup = {row["normalized_skill"]: row["skill_name"] for row in goal_skill_rows}

    matches = matching_repo.list_matches_with_opportunities(goal["id"])
    forecasted: list[dict] = []
    for item in matches:
        if item["bucket"] == "eligible_now":
            continue
        missing = item.get("missing_skills", [])
        if not missing or not all(skill in projected_keys for skill in missing):
            continue
        unlock_dates = [completion_forecast.get(skill, utc_today().isoformat()) for skill in missing]
        predicted_date = max(unlock_dates) if unlock_dates else utc_today().isoformat()
        forecasted.append(
            {
                "opportunity_id": item["opportunity_id"],
                "title": item["title"],
                "company": item["company"],
                "type": item.get("type"),
                "deadline": item.get("deadline"),
                "url": item.get("url"),
                "match_score": item.get("match_score", 0.0),
                "predicted_eligible_date": predicted_date,
                "skills_to_unlock": [display_lookup.get(skill, display_skill(skill)) for skill in missing],
            }
        )

    forecasted.sort(key=lambda row: (row["predicted_eligible_date"], -(row.get("match_score", 0.0))))
    return forecasted[:25]


def _current_skill_state(student_id: int, goal_id: int) -> tuple[set[str], list[dict]]:
    profile_skills = students_repo.list_student_skills(student_id)
    skill_keys = {item["normalized_skill"] for item in profile_skills}
    goal_skills = goals_repo.list_goal_skills(goal_id)
    for row in goal_skills:
        if row["status"] == "completed":
            skill_keys.add(row["normalized_skill"])
    return skill_keys, goal_skills


def _canonical_cache_row(row: dict) -> dict:
    return {
        "bucket": row.get("bucket"),
        "match_score": round(float(row.get("match_score") or 0.0), 8),
        "required_skills_count": int(row.get("required_skills_count") or 0),
        "matched_skills_count": int(row.get("matched_skills_count") or 0),
        "missing_skills": list(row.get("missing_skills") or []),
        "next_skills": list(row.get("next_skills") or []),
        "eligible_now": 1 if row.get("eligible_now") else 0,
    }


def _cache_payload_changed(previous: dict[int, dict], computed: list[dict]) -> bool:
    if len(previous) != len(computed):
        return True
    computed_map = {int(item.get("opportunity_id") or 0): _canonical_cache_row(item) for item in computed}
    if len(computed_map) != len(computed):
        return True
    previous_map = {int(opportunity_id): _canonical_cache_row(row) for opportunity_id, row in previous.items()}
    return previous_map != computed_map


def _student_constraints(student_id: int) -> dict:
    student = students_repo.get_student(student_id)
    if student is None:
        return {"cgpa": 0.0, "has_active_backlog": 0}
    return {
        "cgpa": float(student.get("cgpa") or 0.0),
        "has_active_backlog": int(student.get("has_active_backlog") or 0),
    }


def _weighted_match(
    *,
    opportunity: dict,
    current_keys: set[str],
    next_keys: set[str],
    projected_keys: set[str],
    student_constraints: dict,
    target_company: str,
) -> dict:
    core_keys = [normalize_skill(skill) for skill in opportunity.get("core_skills", []) if normalize_skill(skill)]
    secondary_keys = [normalize_skill(skill) for skill in opportunity.get("secondary_skills", []) if normalize_skill(skill)]
    if not core_keys:
        core_keys = [normalize_skill(skill) for skill in opportunity.get("skills_list", []) if normalize_skill(skill)]
    core_keys = list(dict.fromkeys(core_keys))
    secondary_keys = [skill for skill in dict.fromkeys(secondary_keys) if skill not in core_keys]
    if not core_keys:
        return {"bucket": "not_for_you", "missing_skills": [], "match_score": 0.0, "matched_skills_count": 0, "required_skills_count": 0}

    audience_type = str(opportunity.get("audience_type") or "")
    student_friendly = int(opportunity.get("student_friendly") or 0) == 1
    if audience_type == "experienced" or not student_friendly or int(opportunity.get("experience_min") or 0) >= 2:
        return {
            "bucket": "not_for_you",
            "missing_skills": core_keys + secondary_keys,
            "match_score": 0.0,
            "matched_skills_count": 0,
            "required_skills_count": len(core_keys) + len(secondary_keys),
        }

    core_matched = [skill for skill in core_keys if skill in current_keys]
    core_missing = [skill for skill in core_keys if skill not in current_keys]
    secondary_matched = [skill for skill in secondary_keys if skill in current_keys]
    secondary_missing = [skill for skill in secondary_keys if skill not in current_keys]
    roadmap_reachable_core = [skill for skill in core_missing if skill in projected_keys]
    roadmap_reachable_secondary = [skill for skill in secondary_missing if skill in projected_keys]

    core_score = len(core_matched) / max(len(core_keys), 1)
    secondary_score = len(secondary_matched) / max(len(secondary_keys), 1) if secondary_keys else 1.0
    profile_score = 1.0
    cgpa_requirement = opportunity.get("cgpa_requirement")
    if cgpa_requirement is not None:
        profile_score *= 1.0 if float(student_constraints.get("cgpa") or 0.0) >= float(cgpa_requirement) else 0.0
    backlog_allowed = opportunity.get("backlog_allowed")
    if backlog_allowed is not None and int(backlog_allowed) == 0 and int(student_constraints.get("has_active_backlog") or 0) == 1:
        profile_score = 0.0
    company_bonus = 1.0 if target_company and str(opportunity.get("company") or "").strip().lower() == target_company else 0.0
    roadmap_score = (len(roadmap_reachable_core) + (len(roadmap_reachable_secondary) * 0.4)) / max(len(core_keys) + len(secondary_keys), 1)
    deadline_score = 1.0
    deadline = parse_iso_deadline(opportunity.get("deadline"))
    if deadline is not None:
        days_left = (deadline - utc_today()).days
        if days_left < 0:
            return {
                "bucket": "not_for_you",
                "missing_skills": core_missing + secondary_missing,
                "match_score": 0.0,
                "matched_skills_count": len(core_matched) + len(secondary_matched),
                "required_skills_count": len(core_keys) + len(secondary_keys),
            }
        if days_left <= 5:
            deadline_score = 0.75
        elif days_left <= 15:
            deadline_score = 0.9

    match_score = round(
        min(
            1.0,
            (core_score * 0.45)
            + (secondary_score * 0.15)
            + (profile_score * 0.15)
            + (company_bonus * 0.10)
            + (roadmap_score * 0.10)
            + (deadline_score * 0.05),
        ),
        4,
    )
    missing_skills = core_missing + secondary_missing

    if profile_score <= 0:
        bucket = "not_for_you"
    elif not core_missing:
        bucket = "eligible_now" if len(secondary_missing) <= 2 else "almost_eligible"
    elif len(core_missing) == 1 and core_missing[0] in projected_keys:
        bucket = "almost_eligible"
    elif len(core_missing) <= 2 and any(skill in next_keys for skill in core_missing):
        bucket = "almost_eligible"
    elif any(skill in projected_keys for skill in core_missing):
        bucket = "coming_soon"
    elif student_friendly and str(opportunity.get("type") or "").strip().lower() in {"hackathon", "internship"}:
        bucket = "coming_soon"
    elif student_friendly and match_score >= 0.12:
        bucket = "coming_soon"
    else:
        bucket = "not_for_you"

    return {
        "bucket": bucket,
        "missing_skills": missing_skills,
        "match_score": match_score,
        "matched_skills_count": len(core_matched) + len(secondary_matched),
        "required_skills_count": len(core_keys) + len(secondary_keys),
    }


def refresh_opportunity_matches(student_id: int) -> dict:
    goal = goals_repo.get_active_goal(student_id)
    if goal is None:
        return {"eligible_now": [], "almost_eligible": [], "coming_soon": []}

    current_keys, goal_skills = _current_skill_state(student_id, goal["id"])
    pending_goal_skills = [row for row in goal_skills if row["status"] != "completed"]
    next_keys = {row["normalized_skill"] for row in pending_goal_skills[:2]}
    next_skill_names = [row["skill_name"] for row in pending_goal_skills[:2]]
    projected_keys = set(current_keys) | {row["normalized_skill"] for row in pending_goal_skills[:4]}
    opportunities = opportunities_repo.list_recent(limit=250)
    previous = matching_repo.load_existing_matches(goal["id"])
    constraints = _student_constraints(student_id)
    computed: list[dict] = []
    target_company = (goal.get("target_company") or "").strip().lower()

    for item in opportunities:
        company_name = str(item.get("company") or "").strip()
        match = _weighted_match(
            opportunity=item,
            current_keys=current_keys,
            next_keys=next_keys,
            projected_keys=projected_keys,
            student_constraints=constraints,
            target_company=target_company,
        )
        if match["required_skills_count"] <= 0 or match["bucket"] == "not_for_you":
            continue

        computed.append(
            {
                "opportunity_id": item["id"],
                "bucket": match["bucket"],
                "match_score": match["match_score"],
                "required_skills_count": match["required_skills_count"],
                "matched_skills_count": match["matched_skills_count"],
                "missing_skills": match["missing_skills"],
                "next_skills": next_skill_names,
                "eligible_now": match["bucket"] == "eligible_now",
                "deadline": item.get("deadline"),
                "title": item.get("title"),
                "company": company_name,
            }
        )

    computed.sort(
        key=lambda row: (
            {"eligible_now": 0, "almost_eligible": 1, "coming_soon": 2}.get(row["bucket"], 3),
            -row["match_score"],
        )
    )
    computed = computed[:120]

    for match in computed:
        previous_row = previous.get(match["opportunity_id"])
        became_eligible = match["eligible_now"] and (previous_row is None or previous_row["eligible_now"] == 0)
        if became_eligible:
            company_label = match["company"] or "Unknown company"
            matching_repo.create_notification(
                student_id=student_id,
                goal_id=goal["id"],
                notification_type="newly_eligible",
                title="Newly Eligible Opportunity",
                body=f"You are now eligible for {match['title']} at {company_label}.",
                related_opportunity_id=match["opportunity_id"],
            )

        deadline = parse_iso_deadline(match.get("deadline"))
        if deadline is not None:
            days_left = (deadline - utc_today()).days
            if 0 <= days_left <= 10 and match["bucket"] in {"eligible_now", "almost_eligible"}:
                if previous_row is None or previous_row["bucket"] != match["bucket"]:
                    company_label = match["company"] or "Unknown company"
                    matching_repo.create_notification(
                        student_id=student_id,
                        goal_id=goal["id"],
                        notification_type="deadline_alert",
                        title="Opportunity Deadline Soon",
                        body=(
                            f"{match['title']} ({company_label}) closes in {days_left} day(s). "
                            f"Status: {match['bucket'].replace('_', ' ')}."
                        ),
                        related_opportunity_id=match["opportunity_id"],
                    )

    stripped = [
        {
            "opportunity_id": item["opportunity_id"],
            "bucket": item["bucket"],
            "match_score": item["match_score"],
            "required_skills_count": item["required_skills_count"],
            "matched_skills_count": item["matched_skills_count"],
            "missing_skills": item["missing_skills"],
            "next_skills": item["next_skills"],
            "eligible_now": item["eligible_now"],
        }
        for item in computed
    ]
    if _cache_payload_changed(previous, stripped):
        matching_repo.replace_goal_matches(goal["id"], stripped)

    return bucketed_matches_for_student(student_id)


def bucketed_matches_for_student(student_id: int) -> dict:
    goal = goals_repo.get_active_goal(student_id)
    if goal is None:
        return {"eligible_now": [], "almost_eligible": [], "coming_soon": []}
    matches = matching_repo.list_matches_with_opportunities(goal["id"])
    return {
        "eligible_now": [row for row in matches if row["bucket"] == "eligible_now"],
        "almost_eligible": [row for row in matches if row["bucket"] == "almost_eligible"],
        "coming_soon": [row for row in matches if row["bucket"] == "coming_soon"],
    }


def list_notifications(student_id: int) -> list[dict]:
    return matching_repo.list_notifications(student_id)
