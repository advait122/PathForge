from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import time
from urllib.parse import quote_plus

from fastapi import APIRouter, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from backend.roadmap_engine.constants import (
    BRANCH_OPTIONS,
    DEFAULT_WEEKLY_STUDY_HOURS,
    PREDEFINED_SKILLS,
    TIMELINE_MONTH_OPTIONS,
    YEAR_OPTIONS,
)
from backend.roadmap_engine.enhanced_assessment import coding_repo
from backend.roadmap_engine.enhanced_assessment import service as enhanced_assessment_service
from backend.roadmap_engine.enhanced_assessment.skill_gate import requires_coding_test
from backend.roadmap_engine.services import (
    assessment_service,
    chatbot_service,
    company_service,
    dashboard_service,
    location_catalog_service,
    matching_service,
    onboarding_service,
)
from backend.roadmap_engine.services.skill_normalizer import display_skill
from backend.roadmap_engine.storage import students_repo
from backend.roadmap_engine.utils import utc_now_iso


router = APIRouter()

TEMPLATES_DIR = Path(__file__).resolve().parents[3] / "frontend" / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
STUDENT_COOKIE_KEY = "student_session_id"
COMPANY_COOKIE_KEY = "company_session_id"
COMPANY_DRAFT_COOKIE_KEY = "company_job_draft"
CODING_TEST_DURATION_MINUTES = 150

ALLOWED_DASHBOARD_SECTIONS = {
    "roadmap",
    "tasks",
    "tests",
    "doubtbot",
    "mentor",
    "opportunities",
}
ALLOWED_COMPANY_DASHBOARD_SECTIONS = {
    "dashboard",
    "eligible",
    "applied",
}


def _asset_version() -> str:
    # Force fresh CSS fetch on each request across devices/browsers.
    return str(int(time.time()))


def _normalize_dashboard_section(section: str, default: str = "roadmap") -> str:
    normalized = (section or "").lower().strip()
    if normalized in ALLOWED_DASHBOARD_SECTIONS:
        return normalized
    return default


def _normalize_company_section(section: str, default: str = "dashboard") -> str:
    normalized = (section or "").lower().strip()
    if normalized in ALLOWED_COMPANY_DASHBOARD_SECTIONS:
        return normalized
    return default


def _student_or_404(student_id: int) -> dict:
    student = students_repo.get_student(student_id)
    if student is None:
        raise HTTPException(status_code=404, detail="Student not found.")
    return student


def _assessment_for_student_or_404(student_id: int, assessment_id: int) -> dict:
    from backend.roadmap_engine.storage import assessment_repo, goals_repo

    goal = goals_repo.get_active_goal(student_id)
    if goal is None:
        raise HTTPException(status_code=404, detail="Active goal not found.")

    assessment = assessment_repo.get_assessment(assessment_id)
    if assessment is None or assessment["goal_id"] != goal["id"]:
        raise HTTPException(status_code=404, detail="Assessment not found.")
    return enhanced_assessment_service.attach_existing_coding_assessment(assessment)


def _assessment_review(assessment: dict) -> dict:
    questions = assessment.get("questions", []) or []
    answer_key = assessment.get("answer_key", []) or []
    student_answers = assessment.get("student_answers", []) or []

    reviewed_questions: list[dict] = []
    correct_count = 0

    for idx, question in enumerate(questions):
        expected = answer_key[idx] if idx < len(answer_key) else None
        selected = student_answers[idx] if idx < len(student_answers) else None
        is_correct = expected is not None and selected is not None and selected == expected
        if is_correct:
            correct_count += 1

        reviewed_options = []
        for option_idx, option_text in enumerate(question.get("options", [])):
            reviewed_options.append(
                {
                    "text": str(option_text),
                    "is_selected": selected is not None and option_idx == selected,
                    "is_correct": expected is not None and option_idx == expected,
                }
            )

        reviewed_questions.append(
            {
                "topic": str(question.get("topic", "General")),
                "difficulty": str(question.get("difficulty", "basic")),
                "question": str(question.get("question", "")),
                "options": reviewed_options,
                "is_correct": is_correct,
            }
        )

    total_questions = len(answer_key)
    wrong_count = max(total_questions - correct_count, 0)
    return {
        "questions": reviewed_questions,
        "total_questions": total_questions,
        "correct_count": correct_count,
        "wrong_count": wrong_count,
    }


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _coding_deadline_iso(coding_assessment: dict | None) -> str | None:
    if not coding_assessment:
        return None
    created = _parse_iso_datetime(coding_assessment.get("created_at"))
    if created is None:
        return None
    deadline = (created + timedelta(minutes=CODING_TEST_DURATION_MINUTES)).replace(microsecond=0)
    return deadline.isoformat().replace("+00:00", "Z")


def _coding_deadline_utc(coding_assessment: dict | None) -> datetime | None:
    if not coding_assessment:
        return None
    created = _parse_iso_datetime(coding_assessment.get("created_at"))
    if created is None:
        return None
    return created + timedelta(minutes=CODING_TEST_DURATION_MINUTES)


def _current_company(request: Request) -> dict | None:
    raw_company_id = request.cookies.get(COMPANY_COOKIE_KEY)
    if not raw_company_id:
        return None
    try:
        company_id = int(raw_company_id)
    except ValueError:
        return None
    return company_service.get_company(company_id)


def _current_student(request: Request) -> dict | None:
    raw_student_id = request.cookies.get(STUDENT_COOKIE_KEY)
    if not raw_student_id:
        return None
    try:
        student_id = int(raw_student_id)
    except ValueError:
        return None
    return students_repo.get_student(student_id)


def _load_company_draft(request: Request) -> dict:
    raw = request.cookies.get(COMPANY_DRAFT_COOKIE_KEY)
    if not raw:
        return {}
    try:
        loaded = json.loads(raw)
        if isinstance(loaded, dict):
            return loaded
    except json.JSONDecodeError:
        return {}
    return {}


@router.get("/", response_class=HTMLResponse, include_in_schema=False)
def home(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        "about.html",
        {
            "request": request,
            "asset_version": _asset_version(),
        },
    )


@router.get("/onboarding", response_class=HTMLResponse)
def onboarding_page(request: Request, error: str = "", mode: str = "signup") -> HTMLResponse:
    auth_mode = "login" if (mode or "").strip().lower() == "login" else "signup"
    return templates.TemplateResponse(
        request,
        "onboarding.html",
        {
            "request": request,
            "asset_version": _asset_version(),
            "error": error,
            "branch_options": BRANCH_OPTIONS,
            "year_options": YEAR_OPTIONS,
            "timeline_options": TIMELINE_MONTH_OPTIONS,
            "predefined_skills": PREDEFINED_SKILLS,
            "default_weekly_hours": DEFAULT_WEEKLY_STUDY_HOURS,
            "auth_mode": auth_mode,
        },
    )


@router.post("/onboarding")
def onboarding_submit(
    name: str = Form(...),
    password: str = Form(...),
    confirm_password: str = Form(...),
    branch: str = Form(...),
    current_year: int = Form(...),
    weekly_study_hours: int = Form(DEFAULT_WEEKLY_STUDY_HOURS),
    cgpa: float = Form(...),
    active_backlog: str = Form(default="no"),
    selected_skills: list[str] = Form(default=[]),
    custom_skills: str = Form(default=""),
    goal_text: str = Form(...),
    target_duration_months: int = Form(...),
) -> RedirectResponse:
    try:
        result = onboarding_service.create_student_goal_plan(
            name=name,
            password=password,
            confirm_password=confirm_password,
            branch=branch,
            current_year=current_year,
            weekly_study_hours=weekly_study_hours,
            cgpa=cgpa,
            active_backlog=str(active_backlog).strip().lower() == "yes",
            selected_skills=selected_skills,
            custom_skills_text=custom_skills,
            goal_text=goal_text,
            target_duration_months=target_duration_months,
        )
        student_id = result["student"]["id"]
        matching_service.refresh_opportunity_matches(student_id)
    except ValueError as error:
        escaped = quote_plus(str(error))
        return RedirectResponse(f"/onboarding?mode=signup&error={escaped}", status_code=303)

    response = RedirectResponse(url=f"/students/{student_id}/dashboard", status_code=303)
    response.set_cookie(
        STUDENT_COOKIE_KEY,
        str(student_id),
        httponly=True,
        samesite="lax",
    )
    return response


@router.post("/student/login")
def student_login(
    name: str = Form(...),
    password: str = Form(...),
) -> RedirectResponse:
    try:
        student = onboarding_service.login_student(name=name, password=password)
    except ValueError as error:
        escaped = quote_plus(str(error))
        return RedirectResponse(f"/onboarding?mode=login&error={escaped}", status_code=303)

    response = RedirectResponse(url=f"/students/{student['id']}/dashboard", status_code=303)
    response.set_cookie(
        STUDENT_COOKIE_KEY,
        str(student["id"]),
        httponly=True,
        samesite="lax",
    )
    return response


@router.get("/company/auth", response_class=HTMLResponse)
def company_auth_page(request: Request, error: str = "") -> HTMLResponse:
    company = _current_company(request)
    if company is not None:
        return RedirectResponse(url="/company/dashboard", status_code=303)

    return templates.TemplateResponse(
        request,
        "company_auth.html",
        {
            "request": request,
            "asset_version": _asset_version(),
            "error": error,
        },
    )


@router.get("/company/signup", response_class=HTMLResponse)
def company_signup_page(request: Request, error: str = "") -> HTMLResponse:
    company = _current_company(request)
    if company is not None:
        return RedirectResponse(url="/company/dashboard", status_code=303)

    return templates.TemplateResponse(
        request,
        "company_signup.html",
        {
            "request": request,
            "asset_version": _asset_version(),
            "error": error,
        },
    )


@router.post("/company/signup")
def company_signup(
    username: str = Form(...),
    password: str = Form(...),
    confirm_password: str = Form(...),
) -> RedirectResponse:
    try:
        company = company_service.signup_company(
            username=username,
            password=password,
            confirm_password=confirm_password,
        )
    except ValueError as error:
        escaped = quote_plus(str(error))
        return RedirectResponse(url=f"/company/signup?error={escaped}", status_code=303)

    response = RedirectResponse(url="/company/job/create/step1", status_code=303)
    response.set_cookie(
        COMPANY_COOKIE_KEY,
        str(company["id"]),
        httponly=True,
        samesite="lax",
    )
    return response


@router.post("/company/login")
def company_login(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
):
    is_ajax = request.headers.get("X-Requested-With", "").lower() == "xmlhttprequest"
    try:
        company = company_service.login_company(username=username, password=password)
    except ValueError as error:
        if is_ajax:
            return JSONResponse({"ok": False, "error": str(error)}, status_code=400)
        escaped = quote_plus(str(error))
        return RedirectResponse(url=f"/company/auth?error={escaped}", status_code=303)

    if is_ajax:
        response: RedirectResponse | JSONResponse = JSONResponse(
            {"ok": True, "redirect_url": "/company/dashboard"},
            status_code=200,
        )
    else:
        response = RedirectResponse(url="/company/dashboard", status_code=303)
    response.set_cookie(
        COMPANY_COOKIE_KEY,
        str(company["id"]),
        httponly=True,
        samesite="lax",
    )
    return response


@router.post("/company/logout")
def company_logout() -> RedirectResponse:
    response = RedirectResponse(url="/company/auth", status_code=303)
    response.delete_cookie(COMPANY_COOKIE_KEY)
    response.delete_cookie(COMPANY_DRAFT_COOKIE_KEY)
    return response


@router.get("/company/job/create/step1", response_class=HTMLResponse)
def company_job_step1_page(request: Request, error: str = "") -> HTMLResponse:
    company = _current_company(request)
    if company is None:
        escaped = quote_plus("Please login as a company first.")
        return RedirectResponse(url=f"/company/auth?error={escaped}", status_code=303)

    draft = _load_company_draft(request)
    return templates.TemplateResponse(
        request,
        "company_job_step1.html",
        {
            "request": request,
            "asset_version": _asset_version(),
            "error": error,
            "company": company,
            "predefined_skills": PREDEFINED_SKILLS,
            "draft": draft,
        },
    )


@router.post("/company/job/create/step1")
def company_job_step1_submit(
    request: Request,
    selected_skills: list[str] = Form(default=[]),
    custom_required_skills: str = Form(default=""),
    job_description: str = Form(...),
    active_backlog: str = Form(default="yes"),
) -> RedirectResponse:
    company = _current_company(request)
    if company is None:
        escaped = quote_plus("Please login as a company first.")
        return RedirectResponse(url=f"/company/auth?error={escaped}", status_code=303)

    try:
        required_skills = company_service.parse_required_skills(selected_skills, custom_required_skills)
        clean_description = " ".join((job_description or "").split()).strip()
        if not clean_description:
            raise ValueError("Job description is required.")
        allow_active_backlog = str(active_backlog).strip().lower() == "yes"

        draft = {
            "required_skills": required_skills,
            "job_description": clean_description,
            "allow_active_backlog": allow_active_backlog,
        }
    except ValueError as error:
        escaped = quote_plus(str(error))
        return RedirectResponse(url=f"/company/job/create/step1?error={escaped}", status_code=303)

    response = RedirectResponse(url="/company/job/create/step2", status_code=303)
    response.set_cookie(
        COMPANY_DRAFT_COOKIE_KEY,
        json.dumps(draft),
        httponly=True,
        samesite="lax",
        max_age=1800,
    )
    return response


@router.get("/company/job/create/step2", response_class=HTMLResponse)
def company_job_step2_page(request: Request, error: str = "") -> HTMLResponse:
    company = _current_company(request)
    if company is None:
        escaped = quote_plus("Please login as a company first.")
        return RedirectResponse(url=f"/company/auth?error={escaped}", status_code=303)

    draft = _load_company_draft(request)
    if not draft:
        escaped = quote_plus("Please complete step 1 first.")
        return RedirectResponse(url=f"/company/job/create/step1?error={escaped}", status_code=303)

    required = [display_skill(str(item)) for item in draft.get("required_skills", [])]

    return templates.TemplateResponse(
        request,
        "company_job_step2.html",
        {
            "request": request,
            "asset_version": _asset_version(),
            "error": error,
            "company": company,
            "draft": draft,
            "required_skill_labels": required,
        },
    )


@router.post("/company/job/create")
def company_job_create(
    request: Request,
    min_cgpa: float = Form(...),
    shortlist_count: int = Form(20),
    application_deadline: str = Form(...),
) -> RedirectResponse:
    company = _current_company(request)
    if company is None:
        escaped = quote_plus("Please login as a company first.")
        return RedirectResponse(url=f"/company/auth?error={escaped}", status_code=303)

    draft = _load_company_draft(request)
    if not draft:
        escaped = quote_plus("Please complete step 1 first.")
        return RedirectResponse(url=f"/company/job/create/step1?error={escaped}", status_code=303)

    try:
        job = company_service.create_company_job(
            company_id=int(company["id"]),
            job_description=str(draft.get("job_description", "")),
            required_skills=[str(item) for item in draft.get("required_skills", [])],
            allow_active_backlog=bool(draft.get("allow_active_backlog", True)),
            min_cgpa=float(min_cgpa),
            shortlist_count=int(shortlist_count),
            application_deadline=application_deadline,
        )
    except ValueError as error:
        escaped = quote_plus(str(error))
        return RedirectResponse(url=f"/company/job/create/step2?error={escaped}", status_code=303)

    response = RedirectResponse(
        url=f"/company/dashboard?job_id={job['id']}&top={int(job['shortlist_count'])}",
        status_code=303,
    )
    response.delete_cookie(COMPANY_DRAFT_COOKIE_KEY)
    return response


@router.get("/company/dashboard", response_class=HTMLResponse)
def company_dashboard_page(
    request: Request,
    job_id: int | None = None,
    top: int | None = None,
    section: str = "dashboard",
    error: str = "",
) -> HTMLResponse:
    company = _current_company(request)
    if company is None:
        escaped = quote_plus("Please login as a company first.")
        return RedirectResponse(url=f"/company/auth?error={escaped}", status_code=303)
    active_company_section = _normalize_company_section(section, "dashboard")

    try:
        dashboard = company_service.get_company_dashboard(
            int(company["id"]),
            job_id=job_id,
            top_n=top,
        )
    except ValueError as exc:
        escaped = quote_plus(str(exc))
        return RedirectResponse(url=f"/company/job/create/step1?error={escaped}", status_code=303)

    return templates.TemplateResponse(
        request,
        "company_dashboard.html",
        {
            "request": request,
            "asset_version": _asset_version(),
            "company": company,
            "company_dashboard": dashboard,
            "active_company_section": active_company_section,
            "error": error,
        },
    )


@router.post("/company/jobs/{job_id}/shortlist")
def company_shortlist_students(
    request: Request,
    job_id: int,
    top: int | None = None,
    section: str = "applied",
    selected_student_ids: list[int] = Form(default=[]),
) -> RedirectResponse:
    company = _current_company(request)
    if company is None:
        escaped = quote_plus("Please login as a company first.")
        return RedirectResponse(url=f"/company/auth?error={escaped}", status_code=303)
    active_company_section = _normalize_company_section(section, "applied")

    try:
        company_service.shortlist_students(
            company_id=int(company["id"]),
            job_id=job_id,
            student_ids=[int(item) for item in selected_student_ids],
        )
    except ValueError as error:
        escaped = quote_plus(str(error))
        top_query = f"&top={top}" if top is not None else ""
        return RedirectResponse(
            url=f"/company/dashboard?job_id={job_id}{top_query}&section={active_company_section}&error={escaped}",
            status_code=303,
        )

    top_query = f"&top={top}" if top is not None else ""
    return RedirectResponse(
        url=f"/company/dashboard?job_id={job_id}{top_query}&section={active_company_section}",
        status_code=303,
    )


@router.post("/students/{student_id}/roadmap/replan")
def manual_replan(student_id: int) -> RedirectResponse:
    _student_or_404(student_id)
    try:
        from backend.roadmap_engine.services import roadmap_adjustment_service

        result = roadmap_adjustment_service.auto_replan_if_behind(student_id)
        if result.get("applied"):
            msg = quote_plus(
                f"Roadmap replanned. {result['updated_task_count']} task(s) rescheduled."
            )
        else:
            msg = quote_plus("No replan needed right now.")
    except ValueError as error:
        msg = quote_plus(str(error))

    return RedirectResponse(
        url=f"/students/{student_id}/dashboard?error={msg}",
        status_code=303,
    )


@router.get("/students/{student_id}/dashboard", response_class=HTMLResponse)
def dashboard_page(
    request: Request,
    student_id: int,
    error: str = "",
    section: str = "roadmap",
) -> HTMLResponse:
    student = _student_or_404(student_id)
    active_section = _normalize_dashboard_section(section, "roadmap")

    try:
        dashboard = dashboard_service.get_dashboard(student_id)
    except ValueError as exc:
        escaped = quote_plus(str(exc))
        return RedirectResponse(url=f"/onboarding?error={escaped}", status_code=303)

    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "request": request,
            "asset_version": _asset_version(),
            "student": student,
            "dashboard": dashboard,
            "chatbot_context": dashboard.get("chatbot"),
            "error": error,
            "active_section": active_section,
        },
    )


@router.get("/students/{student_id}/locations/countries", response_class=JSONResponse)
def country_location_suggestions(
    student_id: int,
    q: str = "",
    limit: int = Query(default=500, ge=1, le=10000),
) -> JSONResponse:
    _student_or_404(student_id)
    items = location_catalog_service.search_countries(q=q, limit=limit)
    return JSONResponse({"items": items})


@router.get("/students/{student_id}/locations/states", response_class=JSONResponse)
def state_location_suggestions(
    student_id: int,
    country: str = "",
    q: str = "",
    limit: int = Query(default=500, ge=1, le=10000),
) -> JSONResponse:
    _student_or_404(student_id)
    items = location_catalog_service.search_states(country=country, q=q, limit=limit)
    return JSONResponse({"items": items})


@router.get("/students/{student_id}/locations/cities", response_class=JSONResponse)
def city_location_suggestions(
    student_id: int,
    country: str = "",
    state: str = "",
    q: str = "",
    limit: int = Query(default=500, ge=1, le=10000),
) -> JSONResponse:
    _student_or_404(student_id)
    items = location_catalog_service.search_cities(
        country=country,
        state=state,
        q=q,
        limit=limit,
    )
    return JSONResponse({"items": items})


@router.post("/students/{student_id}/tasks/{task_id}/completion")
def update_task_completion(
    student_id: int,
    task_id: int,
    is_completed: int = Form(...),
    section: str = "tasks",
) -> RedirectResponse:
    _student_or_404(student_id)
    active_section = _normalize_dashboard_section(section, "tasks")
    try:
        dashboard_service.set_task_completion(student_id, task_id, completed=bool(is_completed))
    except ValueError as error:
        escaped = quote_plus(str(error))
        return RedirectResponse(
            url=f"/students/{student_id}/dashboard?section={active_section}&error={escaped}",
            status_code=303,
        )

    return RedirectResponse(
        url=f"/students/{student_id}/dashboard?section={active_section}",
        status_code=303,
    )


@router.post("/students/{student_id}/company-jobs/{job_id}/respond")
def respond_company_job_invite(
    student_id: int,
    job_id: int,
    decision: str = Form(...),
    section: str = "roadmap",
) -> RedirectResponse:
    _student_or_404(student_id)
    active_section = _normalize_dashboard_section(section, "roadmap")
    try:
        company_service.respond_to_company_job(
            student_id=student_id,
            job_id=job_id,
            decision=decision,
        )
    except ValueError as error:
        escaped = quote_plus(str(error))
        return RedirectResponse(
            url=f"/students/{student_id}/dashboard?section={active_section}&error={escaped}",
            status_code=303,
        )

    return RedirectResponse(
        url=f"/students/{student_id}/dashboard?section={active_section}",
        status_code=303,
    )


@router.post("/students/{student_id}/skills/{goal_skill_id}/playlist/select")
def select_playlist(
    student_id: int,
    goal_skill_id: int,
    recommendation_id: str = Form(default=""),
    section: str = "tasks",
) -> RedirectResponse:
    _student_or_404(student_id)
    active_section = _normalize_dashboard_section(section, "tasks")
    try:
        from backend.roadmap_engine.storage import goals_repo
        from backend.roadmap_engine.services import youtube_learning_service

        goal = goals_repo.get_active_goal(student_id)
        if goal is None:
            raise ValueError("No active goal found.")

        goal_skill = goals_repo.get_goal_skill(goal_skill_id)
        if goal_skill is None or goal_skill["goal_id"] != goal["id"]:
            raise ValueError("Skill not found for active goal.")

        goal_skills = goals_repo.list_goal_skills(goal["id"])
        active_skill = next((item for item in goal_skills if item["status"] != "completed"), None)
        if active_skill is None:
            raise ValueError("All skills are already completed.")
        if active_skill["id"] != goal_skill_id:
            raise ValueError(
                f"Playlist selection is currently open for {active_skill['skill_name']} only."
            )
        recommendation_id_clean = recommendation_id.strip()
        if not recommendation_id_clean:
            raise ValueError("Playlist option is missing. Refresh the dashboard and try selecting again.")
        try:
            recommendation_id_int = int(recommendation_id_clean)
        except ValueError as error:
            raise ValueError("Invalid playlist option. Refresh the dashboard and try again.") from error

        currently_selected = youtube_learning_service.get_selected_playlist(goal["id"], goal_skill_id)
        if currently_selected and int(currently_selected.get("id", 0)) == recommendation_id_int:
            youtube_learning_service.clear_selected_playlist(goal["id"], goal_skill_id)
        else:
            youtube_learning_service.select_playlist(
                goal["id"],
                goal_skill_id,
                recommendation_id_int,
                goal_skill["skill_name"],
            )
    except ValueError as error:
        escaped = quote_plus(str(error))
        return RedirectResponse(
            url=f"/students/{student_id}/dashboard?section={active_section}&error={escaped}",
            status_code=303,
        )

    return RedirectResponse(
        url=f"/students/{student_id}/dashboard?section={active_section}",
        status_code=303,
    )


@router.post("/students/{student_id}/chat/send")
def chatbot_send(
    student_id: int,
    question: str = Form(...),
    section: str = "doubtbot",
) -> RedirectResponse:
    _student_or_404(student_id)
    active_section = _normalize_dashboard_section(section, "doubtbot")
    chat_anchor = "doubtbot-widget"
    try:
        chatbot_service.ask_question(student_id, question)
    except ValueError as error:
        escaped = quote_plus(str(error))
        return RedirectResponse(
            url=f"/students/{student_id}/dashboard?section={active_section}&error={escaped}#{chat_anchor}",
            status_code=303,
        )

    return RedirectResponse(
        url=f"/students/{student_id}/dashboard?section={active_section}#{chat_anchor}",
        status_code=303,
    )


@router.get("/students/{student_id}/skills/{goal_skill_id}/test", response_class=HTMLResponse)
def skill_test_page(request: Request, student_id: int, goal_skill_id: int) -> HTMLResponse:
    student = _student_or_404(student_id)
    try:
        assessment = assessment_service.generate_assessment(student_id, goal_skill_id)
    except ValueError as error:
        escaped = quote_plus(str(error))
        return RedirectResponse(
            url=f"/students/{student_id}/dashboard?section=tests&error={escaped}",
            status_code=303,
        )

    try:
        chatbot_context = chatbot_service.get_chat_panel(student_id)
    except ValueError:
        chatbot_context = None

    mentor_prompt = None
    if assessment.get("submitted_at") and assessment.get("passed") == 1:
        from backend.mentor_module.services import mentor_service
        from backend.roadmap_engine.services.skill_normalizer import normalize_skill
        from backend.roadmap_engine.storage import goals_repo

        goal_skill = goals_repo.get_goal_skill(int(assessment["goal_skill_id"]))
        if goal_skill is not None:
            normalized_skill = str(goal_skill.get("normalized_skill") or "").strip() or normalize_skill(
                str(goal_skill.get("skill_name") or "")
            )
            opt_in_status = mentor_service.get_mentor_opt_in_status(student_id, normalized_skill)
            mentor_prompt = {
                "skill_name": str(goal_skill.get("skill_name") or normalized_skill),
                "normalized_skill": normalized_skill,
                "skill_display": display_skill(normalized_skill),
                "eligible": bool(opt_in_status["eligible"]),
                "opted_in": bool(opt_in_status["opted_in"]),
            }

    return templates.TemplateResponse(
        request,
        "skill_test.html",
        {
            "request": request,
            "asset_version": _asset_version(),
            "student": student,
            "assessment": assessment,
            "show_results": bool(assessment.get("submitted_at")),
            "assessment_review": _assessment_review(assessment) if assessment.get("submitted_at") else None,
            "test_duration_minutes": assessment_service.TEST_DURATION_MINUTES,
            "test_deadline_iso": assessment_service.assessment_deadline_iso(assessment),
            "chatbot_context": chatbot_context,
            "mentor_prompt": mentor_prompt,
            "mentor_opted_in": False,
            "mentor_opted_out": False,
            "opt_in_error": "",
            "active_section": "tests",
        },
    )


@router.get("/students/{student_id}/skills/tests/{assessment_id}/result", response_class=HTMLResponse)
def skill_test_result_page(
    request: Request,
    student_id: int,
    assessment_id: int,
    mentor_opted_in: int = 0,
    mentor_opted_out: int = 0,
    opt_in_error: str = "",
) -> HTMLResponse:
    student = _student_or_404(student_id)
    assessment = _assessment_for_student_or_404(student_id, assessment_id)

    if not assessment.get("submitted_at"):
        return RedirectResponse(
            url=f"/students/{student_id}/skills/{assessment['goal_skill_id']}/test",
            status_code=303,
        )

    try:
        chatbot_context = chatbot_service.get_chat_panel(student_id)
    except ValueError:
        chatbot_context = None

    mentor_prompt = None
    if assessment.get("passed") == 1:
        from backend.mentor_module.services import mentor_service
        from backend.roadmap_engine.services.skill_normalizer import normalize_skill
        from backend.roadmap_engine.storage import goals_repo

        goal_skill = goals_repo.get_goal_skill(int(assessment["goal_skill_id"]))
        if goal_skill is not None:
            normalized_skill = str(goal_skill.get("normalized_skill") or "").strip() or normalize_skill(
                str(goal_skill.get("skill_name") or "")
            )
            opt_in_status = mentor_service.get_mentor_opt_in_status(student_id, normalized_skill)
            mentor_prompt = {
                "skill_name": str(goal_skill.get("skill_name") or normalized_skill),
                "normalized_skill": normalized_skill,
                "skill_display": display_skill(normalized_skill),
                "eligible": bool(opt_in_status["eligible"]),
                "opted_in": bool(opt_in_status["opted_in"]),
            }

    return templates.TemplateResponse(
        request,
        "skill_test.html",
        {
            "request": request,
            "asset_version": _asset_version(),
            "student": student,
            "assessment": assessment,
            "show_results": True,
            "assessment_review": _assessment_review(assessment),
            "chatbot_context": chatbot_context,
            "mentor_prompt": mentor_prompt,
            "mentor_opted_in": bool(mentor_opted_in),
            "mentor_opted_out": bool(mentor_opted_out),
            "opt_in_error": opt_in_error,
            "active_section": "tests",
        },
    )


@router.get("/students/{student_id}/skills/{goal_skill_id}/coding-test", response_class=HTMLResponse)
def coding_test_page(
    request: Request,
    student_id: int,
    goal_skill_id: int,
    mode: str = "test",
    error: str = "",
) -> HTMLResponse:
    student = _student_or_404(student_id)
    coding_assessment = None
    assessment = None
    goal_skill = None

    try:
        from backend.roadmap_engine.storage import assessment_repo, goals_repo, matching_repo
        from backend.roadmap_engine.services import youtube_learning_service

        goal = goals_repo.get_active_goal(student_id)
        if goal is None:
            raise ValueError("Active goal not found.")

        goal_skill = goals_repo.get_goal_skill(goal_skill_id)
        if goal_skill is None or goal_skill["goal_id"] != goal["id"]:
            raise ValueError("Skill not found for active goal.")

        if not requires_coding_test(goal_skill["skill_name"]):
            raise ValueError("Coding test is not required for this skill.")

        latest = assessment_repo.get_latest_assessment(goal_skill_id)
        if latest is None or latest.get("submitted_at") is None:
            raise ValueError("Complete and submit the MCQ test first.")
        if latest.get("passed") != 1:
            raise ValueError("Pass the MCQ test first to unlock coding test.")

        selected_playlist = youtube_learning_service.get_selected_playlist(goal["id"], goal_skill_id)
        assessment = enhanced_assessment_service.ensure_and_attach_coding_assessment(
            latest,
            skill_name=goal_skill["skill_name"],
            selected_playlist=selected_playlist,
        )
        coding_assessment = assessment.get("coding_assessment")
        if not coding_assessment:
            raise ValueError("Coding test could not be created. Please try again.")

        # Retake behavior:
        # - if user opens test mode after a submitted attempt, start a fresh timed attempt.
        # - if the previous timed attempt expired without submission, start a fresh timed attempt.
        mode_key = str(mode).strip().lower()
        if mode_key != "result":
            coding_record = coding_repo.get_coding_assessment(int(assessment["id"]))
            should_reset_attempt = False
            if coding_record is not None:
                if coding_record.get("submitted_at") and coding_record.get("passed") != 1:
                    should_reset_attempt = True
                else:
                    deadline_utc = _coding_deadline_utc(coding_record)
                    if deadline_utc is not None:
                        now_utc = datetime.now(tz=timezone.utc)
                        if now_utc > (deadline_utc + timedelta(seconds=90)):
                            should_reset_attempt = True

            if should_reset_attempt and coding_record is not None:
                questions = list(coding_record.get("questions", []) or [])
                if questions:
                    coding_repo.create_or_replace_coding_assessment(
                        assessment_id=int(assessment["id"]),
                        goal_id=int(assessment["goal_id"]),
                        goal_skill_id=int(assessment["goal_skill_id"]),
                        questions=questions,
                    )
                    assessment = enhanced_assessment_service.attach_existing_coding_assessment(assessment)
                    coding_assessment = assessment.get("coding_assessment")

    except ValueError as exc:
        escaped = quote_plus(str(exc))
        return RedirectResponse(
            url=f"/students/{student_id}/dashboard?section=tests&error={escaped}",
            status_code=303,
        )

    show_results = str(mode).strip().lower() == "result" and bool(
        coding_assessment.get("last_submission")
    )

    return templates.TemplateResponse(
        request,
        "coding_test.html",
        {
            "request": request,
            "asset_version": _asset_version(),
            "student": student,
            "goal_skill": goal_skill,
            "assessment": assessment,
            "coding_assessment": coding_assessment,
            "show_results": show_results,
            "error": error,
            "coding_duration_minutes": CODING_TEST_DURATION_MINUTES,
            "coding_deadline_iso": _coding_deadline_iso(coding_repo.get_coding_assessment(int(assessment["id"]))),
            "active_section": "tests",
        },
    )


@router.post("/students/{student_id}/skills/{goal_skill_id}/coding-test/submit")
async def coding_test_submit(
    request: Request,
    student_id: int,
    goal_skill_id: int,
) -> RedirectResponse:
    _student_or_404(student_id)

    try:
        from backend.roadmap_engine.storage import assessment_repo, goals_repo, matching_repo
        from backend.roadmap_engine.services import youtube_learning_service

        goal = goals_repo.get_active_goal(student_id)
        if goal is None:
            raise ValueError("Active goal not found.")

        goal_skill = goals_repo.get_goal_skill(goal_skill_id)
        if goal_skill is None or goal_skill["goal_id"] != goal["id"]:
            raise ValueError("Skill not found for active goal.")

        if not requires_coding_test(goal_skill["skill_name"]):
            raise ValueError("Coding test is not required for this skill.")

        latest = assessment_repo.get_latest_assessment(goal_skill_id)
        if latest is None or latest.get("submitted_at") is None:
            raise ValueError("Complete and submit the MCQ test first.")
        if latest.get("passed") != 1:
            raise ValueError("Pass the MCQ test first to unlock coding test.")

        selected_playlist = youtube_learning_service.get_selected_playlist(goal["id"], goal_skill_id)
        latest = enhanced_assessment_service.ensure_and_attach_coding_assessment(
            latest,
            skill_name=goal_skill["skill_name"],
            selected_playlist=selected_playlist,
        )

        coding_record = coding_repo.get_coding_assessment(int(latest["id"]))
        if coding_record is None:
            raise ValueError("Coding test could not be loaded.")

        deadline_utc = _coding_deadline_utc(coding_record)
        if deadline_utc is not None:
            now_utc = datetime.now(tz=timezone.utc)
            if now_utc > (deadline_utc + timedelta(seconds=90)):
                raise ValueError("Time is up for this coding test. Please retake.")

        payload = await request.form()
        question_count = len(coding_record.get("questions", []))
        coding_submissions: list[dict] = []
        for idx in range(question_count):
            coding_submissions.append(
                {
                    "question_index": idx,
                    "language": str(payload.get(f"coding_language_{idx}", "") or ""),
                    "code": str(payload.get(f"coding_code_{idx}", "") or ""),
                }
            )

        coding_result = enhanced_assessment_service.evaluate_and_submit_coding(
            assessment=latest,
            skill_name=goal_skill["skill_name"],
            coding_submissions=coding_submissions,
        )
        if not coding_result.get("required"):
            raise ValueError("Coding test is not required for this skill.")

        coding_score = float(coding_result.get("score_percent") or 0.0)
        if coding_result.get("passed"):
            completed_at = utc_now_iso()
            goals_repo.set_goal_skill_status(goal_skill["id"], "completed", completed_at)
            students_repo.add_student_skill(
                student_id=student_id,
                skill_name=goal_skill["skill_name"],
                normalized_skill=goal_skill.get("normalized_skill") or goal_skill["skill_name"].lower(),
                skill_source="roadmap_mastered",
            )
            matching_repo.create_notification(
                student_id=student_id,
                goal_id=goal["id"],
                notification_type="coding_test_passed",
                title="Coding Test Passed",
                body=(
                    f"You passed coding test for {goal_skill['skill_name']} ({coding_score:.1f}%). "
                    "Skill marked as completed."
                ),
            )
        else:
            goals_repo.set_goal_skill_status(goal_skill["id"], "in_progress", None)
            matching_repo.create_notification(
                student_id=student_id,
                goal_id=goal["id"],
                notification_type="coding_test_failed",
                title="Coding Test Failed",
                body=(
                    f"You scored {coding_score:.1f}% in coding test for {goal_skill['skill_name']}. "
                    "Revise and retake."
                ),
            )

        matching_service.refresh_opportunity_matches(student_id)

    except ValueError as exc:
        escaped = quote_plus(str(exc))
        return RedirectResponse(
            url=f"/students/{student_id}/skills/{goal_skill_id}/coding-test?error={escaped}",
            status_code=303,
        )

    return RedirectResponse(
        url=f"/students/{student_id}/skills/{goal_skill_id}/coding-test?mode=result",
        status_code=303,
    )


@router.post("/students/{student_id}/skills/tests/{assessment_id}/coding/run", response_class=JSONResponse)
def skill_test_coding_run(
    student_id: int,
    assessment_id: int,
    question_index: int = Form(...),
    language: str = Form(...),
    code: str = Form(...),
) -> JSONResponse:
    _student_or_404(student_id)
    _assessment_for_student_or_404(student_id, assessment_id)
    try:
        result = enhanced_assessment_service.run_preview(
            assessment_id=assessment_id,
            question_index=int(question_index),
            language=language,
            code=code,
        )
    except ValueError as error:
        return JSONResponse({"ok": False, "error": str(error)}, status_code=400)
    return JSONResponse({"ok": True, "result": result})


@router.post("/students/{student_id}/skills/tests/{assessment_id}/submit")
async def skill_test_submit(
    request: Request,
    student_id: int,
    assessment_id: int,
) -> RedirectResponse:
    _student_or_404(student_id)
    assessment = _assessment_for_student_or_404(student_id, assessment_id)
    payload = await request.form()

    selected_answers: list[int] = []
    total_questions = len(assessment.get("answer_key", []) or [])
    for idx in range(total_questions):
        key = f"answer_{idx}"
        raw_value = payload.get(key)
        if raw_value is None:
            selected_answers.append(-1)
            continue
        try:
            selected_answers.append(int(raw_value))
        except (TypeError, ValueError):
            selected_answers.append(-1)

    try:
        assessment_service.submit_assessment(student_id, assessment_id, selected_answers)
        matching_service.refresh_opportunity_matches(student_id)
    except ValueError as error:
        escaped = quote_plus(str(error))
        return RedirectResponse(
            url=f"/students/{student_id}/dashboard?section=tests&error={escaped}",
            status_code=303,
        )

    return RedirectResponse(
        url=f"/students/{student_id}/skills/tests/{assessment_id}/result",
        status_code=303,
    )
