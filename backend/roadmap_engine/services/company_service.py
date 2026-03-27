import hashlib
from datetime import date

from backend.roadmap_engine.services.skill_normalizer import deduplicate_skills, display_skill, normalize_skill
from backend.roadmap_engine.storage import company_repo, goals_repo, matching_repo, students_repo
from backend.roadmap_engine.utils import parse_custom_skills, utc_today


TOP_FILTER_OPTIONS = [10, 20, 50, 100]


def _hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def _normalize_username(username: str) -> str:
    return username.strip().lower()


def get_company(company_id: int) -> dict | None:
    return company_repo.get_company_account(company_id)


def signup_company(*, username: str, password: str, confirm_password: str) -> dict:
    cleaned_username = _normalize_username(username)
    if not cleaned_username:
        raise ValueError("Username is required.")
    if len(cleaned_username) < 3 or len(cleaned_username) > 40:
        raise ValueError("Username must be between 3 and 40 characters.")
    if not password:
        raise ValueError("Password is required.")
    if len(password) < 6:
        raise ValueError("Password must be at least 6 characters.")
    if password != confirm_password:
        raise ValueError("Passwords do not match.")

    existing = company_repo.get_company_by_username(cleaned_username)
    if existing:
        raise ValueError("Username already exists. Please login instead.")

    company_id = company_repo.create_company_account(
        cleaned_username,
        _hash_password(password),
    )
    company = company_repo.get_company_account(company_id)
    if company is None:
        raise ValueError("Failed to create company account.")
    return company


def login_company(*, username: str, password: str) -> dict:
    cleaned_username = _normalize_username(username)
    if not cleaned_username or not password:
        raise ValueError("Username and password are required.")

    company = company_repo.get_company_by_username(cleaned_username)
    if company is None:
        raise ValueError("Invalid username or password.")
    if company["password_hash"] != _hash_password(password):
        raise ValueError("Invalid username or password.")
    return company


def parse_required_skills(selected_skills: list[str], custom_skills_text: str) -> list[str]:
    custom_skills = parse_custom_skills(custom_skills_text)
    combined = deduplicate_skills((selected_skills or []) + custom_skills)

    normalized: list[str] = []
    seen: set[str] = set()
    for skill in combined:
        key = normalize_skill(skill)
        if not key or key in seen:
            continue
        seen.add(key)
        normalized.append(key)

    if not normalized:
        raise ValueError("Add at least one required skill.")
    return normalized


def build_job_title(job_description: str) -> str:
    compact = " ".join((job_description or "").split()).strip()
    if not compact:
        return "Company Role Opening"
    if len(compact) <= 64:
        return compact
    return compact[:61].rstrip() + "..."


def _synthetic_skill_score(student_id: int, normalized_skill: str) -> float:
    signature = sum(ord(ch) for ch in f"{student_id}:{normalized_skill}")
    return float(62 + (signature % 34))


def _regularity_rating(student_id: int, replan_count: int) -> float:
    jitter = student_id % 6
    rating = 95 - (replan_count * 9) - jitter
    return float(max(40, min(99, rating)))


def _score_for_student_skill(student_id: int, normalized_skill: str) -> tuple[float, str]:
    score = company_repo.get_latest_skill_score(student_id, normalized_skill)
    if score is not None:
        return float(max(0.0, min(100.0, score))), "assessment"
    return _synthetic_skill_score(student_id, normalized_skill), "simulated"


def _rank_candidates_for_job(job: dict) -> list[dict]:
    required = [str(skill) for skill in (job.get("required_skills") or []) if str(skill).strip()]
    required_set = set(required)
    if not required_set:
        return []

    applications = company_repo.list_job_applications(job["id"])
    application_by_student = {int(item["student_id"]): item for item in applications}

    shortlisted = {
        int(item["student_id"])
        for item in company_repo.list_shortlisted_students(job["id"])
    }

    students = students_repo.list_students()
    ranked: list[dict] = []

    for student in students:
        student_id = int(student["id"])
        skill_rows = students_repo.list_student_skills(student_id)
        skill_keys = {
            str(row.get("normalized_skill") or "").strip()
            for row in skill_rows
            if str(row.get("normalized_skill") or "").strip()
        }
        if not required_set.issubset(skill_keys):
            continue

        try:
            cgpa = round(float(student.get("cgpa", 0.0)), 2)
        except (TypeError, ValueError):
            cgpa = 0.0
        if cgpa < float(job["min_cgpa"]):
            continue

        try:
            has_backlog = bool(int(student.get("has_active_backlog", 0)))
        except (TypeError, ValueError):
            has_backlog = False
        if int(job["allow_active_backlog"]) == 0 and has_backlog:
            continue

        all_skill_labels: list[str] = []
        seen_labels: set[str] = set()
        for row in skill_rows:
            normalized = str(row.get("normalized_skill") or "").strip()
            raw_name = str(row.get("skill_name") or "").strip()
            source = normalized or raw_name
            if not source:
                continue
            label = display_skill(source)
            label_key = label.lower()
            if label_key in seen_labels:
                continue
            seen_labels.add(label_key)
            all_skill_labels.append(label)

        per_skill_scores: list[float] = []
        sources: set[str] = set()
        for skill_key in required:
            score, source = _score_for_student_skill(student_id, skill_key)
            per_skill_scores.append(score)
            sources.add(source)

        cumulative_test_score = (
            round(sum(per_skill_scores) / len(per_skill_scores), 2) if per_skill_scores else 0.0
        )
        replan_count = company_repo.count_replan_notifications(student_id)
        regularity = round(_regularity_rating(student_id, replan_count), 2)
        cgpa_score = min(100.0, max(0.0, cgpa * 10.0))
        final_score = round(
            (cumulative_test_score * 0.62) + (regularity * 0.23) + (cgpa_score * 0.15),
            2,
        )

        app = application_by_student.get(student_id)
        application_status = app["status"] if app else "pending"

        ranked.append(
            {
                "student_id": student_id,
                "student_name": student["name"],
                "branch": student["branch"],
                "current_year": student["current_year"],
                "matched_skills": [display_skill(key) for key in required],
                "all_skills_display": all_skill_labels,
                "cumulative_test_score": cumulative_test_score,
                "regularity_rating": regularity,
                "replan_count": replan_count,
                "final_score": final_score,
                "cgpa": cgpa,
                "has_active_backlog": has_backlog,
                "application_status": application_status,
                "is_shortlisted": student_id in shortlisted,
                "metrics_source": "simulated" if "simulated" in sources else "assessment",
            }
        )

    ranked.sort(
        key=lambda item: (
            item["final_score"],
            item["cumulative_test_score"],
            item["regularity_rating"],
            item["cgpa"],
        ),
        reverse=True,
    )
    return ranked


def _build_demo_candidates(job: dict, count: int) -> list[dict]:
    required = [display_skill(str(skill)) for skill in (job.get("required_skills") or [])]
    demos: list[dict] = []
    for idx in range(1, count + 1):
        test_score = float(68 + ((idx * 7) % 28))
        regularity = float(72 + ((idx * 5) % 24))
        cgpa = round(7.1 + (idx % 10) * 0.2, 2)
        cgpa_score = min(100.0, max(0.0, cgpa * 10.0))
        final_score = round(
            (test_score * 0.62) + (regularity * 0.23) + (cgpa_score * 0.15),
            2,
        )
        demos.append(
            {
                "student_id": -idx,
                "student_name": f"Demo Student {idx}",
                "branch": "CSE",
                "current_year": 3 + (idx % 2),
                "matched_skills": required,
                "all_skills_display": required,
                "cumulative_test_score": round(test_score, 2),
                "regularity_rating": round(regularity, 2),
                "replan_count": idx % 3,
                "final_score": final_score,
                "cgpa": cgpa,
                "has_active_backlog": False,
                "application_status": "demo",
                "is_shortlisted": False,
                "metrics_source": "simulated-demo",
            }
        )
    demos.sort(key=lambda item: item["final_score"], reverse=True)
    return demos


def _notify_student_job_invite(*, student_id: int, job: dict) -> None:
    goal = goals_repo.get_active_goal(student_id)
    goal_id = int(goal["id"]) if goal else None
    company_name = str(job.get("company_username") or "Company").strip()
    title = f"New Company Invite: {company_name}"
    body = (
        f"You are eligible for '{job['title']}'. "
        "Open your dashboard to apply or decline this invitation."
    )
    matching_repo.create_notification(
        student_id=student_id,
        goal_id=goal_id,
        notification_type="company_job_invite",
        title=title,
        body=body,
    )


def create_company_job(
    *,
    company_id: int,
    job_description: str,
    required_skills: list[str],
    allow_active_backlog: bool,
    min_cgpa: float,
    shortlist_count: int,
    application_deadline: str,
) -> dict:
    company = company_repo.get_company_account(company_id)
    if company is None:
        raise ValueError("Company account not found.")

    description = " ".join((job_description or "").split()).strip()
    if not description:
        raise ValueError("Job description is required.")

    try:
        deadline = date.fromisoformat(str(application_deadline))
    except ValueError as error:
        raise ValueError("Please provide a valid application deadline.") from error

    if deadline < utc_today():
        raise ValueError("Application deadline cannot be in the past.")

    min_cgpa_value = float(min_cgpa)
    if min_cgpa_value < 0 or min_cgpa_value > 10:
        raise ValueError("Min CGPA must be between 0 and 10.")

    shortlist_limit = int(shortlist_count)
    if shortlist_limit < 1 or shortlist_limit > 500:
        raise ValueError("Students to shortlist must be between 1 and 500.")

    title = build_job_title(description)
    job_id = company_repo.create_job_post(
        company_id=company_id,
        title=title,
        job_description=description,
        required_skills=required_skills,
        allow_active_backlog=allow_active_backlog,
        min_cgpa=min_cgpa_value,
        shortlist_count=shortlist_limit,
        application_deadline=deadline.isoformat(),
    )

    job = company_repo.get_job_post(job_id)
    if job is None:
        raise ValueError("Failed to create job post.")

    candidates = _rank_candidates_for_job(job)
    for candidate in candidates:
        company_repo.upsert_job_application(job_id, int(candidate["student_id"]), "pending")
        _notify_student_job_invite(student_id=int(candidate["student_id"]), job=job)

    return job


def get_company_dashboard(company_id: int, job_id: int | None, top_n: int | None) -> dict:
    company = company_repo.get_company_account(company_id)
    if company is None:
        raise ValueError("Company account not found.")

    jobs = company_repo.list_company_jobs(company_id)
    if not jobs:
        raise ValueError("No jobs created yet. Create a job to view the dashboard.")

    selected_job: dict | None = None
    if job_id is not None:
        selected_job = company_repo.get_job_post(job_id)
        if selected_job is None or int(selected_job["company_id"]) != company_id:
            raise ValueError("Requested job was not found for this company.")
    if selected_job is None:
        selected_job = jobs[0]

    default_top = int(selected_job.get("shortlist_count") or 20)
    if default_top < 1:
        default_top = 20

    if top_n is None:
        top_value = default_top
    else:
        try:
            top_value = int(top_n)
        except (TypeError, ValueError):
            top_value = default_top
        if top_value < 1:
            top_value = default_top
    top_value = min(top_value, 500)

    ranked = _rank_candidates_for_job(selected_job)
    using_demo_data = False
    if not ranked:
        using_demo_data = True
        ranked = _build_demo_candidates(selected_job, max(top_value, 10))
    top_candidates = ranked[:top_value]
    applied_candidates = [item for item in ranked if item["application_status"] == "applied"][:top_value]

    selected_students_raw = company_repo.list_shortlisted_students(selected_job["id"])
    ranked_by_id = {int(item["student_id"]): item for item in ranked}
    selected_students: list[dict] = []
    for item in selected_students_raw:
        student_id = int(item["student_id"])
        metrics = ranked_by_id.get(student_id, {})
        selected_students.append(
            {
                **item,
                "final_score": metrics.get("final_score"),
                "cumulative_test_score": metrics.get("cumulative_test_score"),
                "regularity_rating": metrics.get("regularity_rating"),
            }
        )

    status_counts = {"pending": 0, "applied": 0, "declined": 0}
    for row in company_repo.list_job_applications(selected_job["id"]):
        status = str(row.get("status") or "").strip().lower()
        if status in status_counts:
            status_counts[status] += 1

    selected_job["required_skill_labels"] = [
        display_skill(str(skill))
        for skill in (selected_job.get("required_skills") or [])
    ]

    return {
        "company": company,
        "jobs": jobs,
        "active_job": selected_job,
        "top_filter": top_value,
        "top_options": sorted({*TOP_FILTER_OPTIONS, default_top}),
        "top_candidates": top_candidates,
        "applied_candidates": applied_candidates,
        "selected_students": selected_students,
        "status_counts": status_counts,
        "eligible_count": len(ranked),
        "using_demo_data": using_demo_data,
    }


def shortlist_students(
    *,
    company_id: int,
    job_id: int,
    student_ids: list[int],
) -> dict:
    job = company_repo.get_job_post(job_id)
    if job is None or int(job["company_id"]) != company_id:
        raise ValueError("Job not found for this company.")

    eligible_for_shortlist = {
        int(item["student_id"])
        for item in company_repo.list_job_applications(job_id)
        if item.get("status") == "applied"
    }
    existing = {
        int(item["student_id"])
        for item in company_repo.list_shortlisted_students(job_id)
    }

    remaining_slots = max(0, int(job["shortlist_count"]) - len(existing))
    if remaining_slots == 0:
        return {"added": 0, "limit_reached": True}

    added = 0
    seen: set[int] = set()
    for raw_id in student_ids:
        student_id = int(raw_id)
        if student_id in seen:
            continue
        seen.add(student_id)
        if student_id in existing:
            continue
        if student_id not in eligible_for_shortlist:
            continue
        company_repo.add_shortlist(job_id, student_id)
        added += 1
        if added >= remaining_slots:
            break

    return {"added": added, "limit_reached": added < len(seen)}


def list_student_pending_company_jobs(student_id: int) -> list[dict]:
    invites = company_repo.list_pending_invites_for_student(student_id)
    today = utc_today()
    normalized: list[dict] = []

    for item in invites:
        required = [display_skill(str(skill)) for skill in (item.get("required_skills") or [])]
        deadline_text = str(item.get("application_deadline") or "")
        deadline_passed = False
        try:
            deadline_passed = date.fromisoformat(deadline_text) < today
        except ValueError:
            deadline_passed = False

        normalized.append(
            {
                **item,
                "required_skill_labels": required,
                "deadline_passed": deadline_passed,
            }
        )
    return normalized


def respond_to_company_job(*, student_id: int, job_id: int, decision: str) -> None:
    job = company_repo.get_job_post(job_id)
    if job is None:
        raise ValueError("Company job not found.")

    app = company_repo.get_job_application(job_id, student_id)
    if app is None:
        raise ValueError("No pending invitation found for this job.")
    if app["status"] != "pending":
        raise ValueError("You already responded to this invitation.")

    try:
        deadline = date.fromisoformat(str(job["application_deadline"]))
    except ValueError:
        deadline = utc_today()
    if deadline < utc_today():
        raise ValueError("This job invitation has expired.")

    choice = str(decision or "").strip().lower()
    if choice not in {"apply", "decline"}:
        raise ValueError("Invalid decision.")

    next_status = "applied" if choice == "apply" else "declined"
    company_repo.set_job_application_status(job_id, student_id, next_status)
