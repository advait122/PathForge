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
    for skill in goal_skills:
        if skill["status"] == "completed":
            forecast[skill["normalized_skill"]] = today.isoformat()
            continue

        tasks = roadmap_repo.list_tasks_for_skill(plan["id"], skill["id"])
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
        if not missing:
            continue

        if not all(skill in projected_keys for skill in missing):
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
                "skills_to_unlock": [
                    display_lookup.get(skill, display_skill(skill))
                    for skill in missing
                ],
            }
        )

    forecasted.sort(
        key=lambda row: (
            row["predicted_eligible_date"],
            -(row.get("match_score", 0.0)),
        )
    )
    return forecasted[:25]


def _current_skill_state(student_id: int, goal_id: int) -> tuple[set[str], list[dict]]:
    profile_skills = students_repo.list_student_skills(student_id)
    skill_keys = {item["normalized_skill"] for item in profile_skills}

    goal_skills = goals_repo.list_goal_skills(goal_id)
    for row in goal_skills:
        if row["status"] == "completed":
            skill_keys.add(row["normalized_skill"])

    return skill_keys, goal_skills


def _classify_match(required_keys: list[str], current_keys: set[str], next_keys: set[str]) -> tuple[str, list[str]]:
    missing = [skill for skill in required_keys if skill not in current_keys]
    if len(missing) == 0:
        return "eligible_now", missing

    if len(missing) <= 2 or any(skill in next_keys for skill in missing):
        return "almost_eligible", missing

    return "coming_soon", missing


def refresh_opportunity_matches(student_id: int) -> dict:
    goal = goals_repo.get_active_goal(student_id)
    if goal is None:
        return {"eligible_now": [], "almost_eligible": [], "coming_soon": []}

    current_keys, goal_skills = _current_skill_state(student_id, goal["id"])
    pending_goal_skills = [row for row in goal_skills if row["status"] != "completed"]
    next_keys = {row["normalized_skill"] for row in pending_goal_skills[:2]}
    next_skill_names = [row["skill_name"] for row in pending_goal_skills[:2]]

    opportunities = opportunities_repo.list_recent(limit=250)
    previous = matching_repo.load_existing_matches(goal["id"])
    computed: list[dict] = []

    target_company = (goal.get("target_company") or "").strip().lower()

    for item in opportunities:
        required = item.get("skills_list") or []
        required_keys = []
        seen = set()
        for skill in required:
            key = normalize_skill(skill)
            if not key or key in seen:
                continue
            seen.add(key)
            required_keys.append(key)

        if not required_keys:
            continue

        bucket, missing = _classify_match(required_keys, current_keys, next_keys)
        matched_count = len(required_keys) - len(missing)
        base_score = matched_count / len(required_keys)
        company_bonus = 0.15 if target_company and item["company"].lower() == target_company else 0.0
        score = min(1.0, base_score + company_bonus)

        computed.append(
            {
                "opportunity_id": item["id"],
                "bucket": bucket,
                "match_score": score,
                "required_skills_count": len(required_keys),
                "matched_skills_count": matched_count,
                "missing_skills": missing,
                "next_skills": next_skill_names,
                "eligible_now": bucket == "eligible_now",
                "deadline": item.get("deadline"),
                "title": item.get("title"),
                "company": item.get("company"),
            }
        )

    computed.sort(
        key=lambda row: (
            {"eligible_now": 0, "almost_eligible": 1, "coming_soon": 2}[row["bucket"]],
            -row["match_score"],
        )
    )
    computed = computed[:120]

    for match in computed:
        previous_row = previous.get(match["opportunity_id"])
        became_eligible = match["eligible_now"] and (
            previous_row is None or previous_row["eligible_now"] == 0
        )
        if became_eligible:
            matching_repo.create_notification(
                student_id=student_id,
                goal_id=goal["id"],
                notification_type="newly_eligible",
                title="Newly Eligible Opportunity",
                body=f"You are now eligible for {match['title']} at {match['company']}.",
                related_opportunity_id=match["opportunity_id"],
            )

        deadline = parse_iso_deadline(match.get("deadline"))
        if deadline is not None:
            days_left = (deadline - utc_today()).days
            if 0 <= days_left <= 10 and match["bucket"] in {"eligible_now", "almost_eligible"}:
                if previous_row is None or previous_row["bucket"] != match["bucket"]:
                    matching_repo.create_notification(
                        student_id=student_id,
                        goal_id=goal["id"],
                        notification_type="deadline_alert",
                        title="Opportunity Deadline Soon",
                        body=(
                            f"{match['title']} ({match['company']}) closes in {days_left} day(s). "
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
