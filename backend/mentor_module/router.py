import time
from pathlib import Path
from urllib.parse import quote_plus

from fastapi import APIRouter, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from backend.mentor_module.services import chat_service, mentor_service
from backend.mentor_module.storage import mentor_repo
from backend.roadmap_engine.services.skill_normalizer import display_skill, normalize_skill
from backend.roadmap_engine.storage import goals_repo, students_repo

router = APIRouter(prefix="/mentor", tags=["mentor"])

TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "web_portal" / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def _asset_version() -> str:
    return str(int(time.time()))


def _student_or_404(student_id: int) -> dict:
    student = students_repo.get_student(student_id)
    if student is None:
        raise HTTPException(status_code=404, detail="Student not found.")
    return student


def _session_access_check(session: dict, student_id: int) -> None:
    if student_id not in (int(session["seeker_id"]), int(session["mentor_id"])):
        raise HTTPException(status_code=403, detail="Access denied.")


@router.post("/opt-in")
def mentor_opt_in(
    student_id: int = Form(...),
    normalized_skill: str = Form(...),
    assessment_id: int = Form(...),
) -> RedirectResponse:
    normalized_skill_key = normalize_skill(normalized_skill)
    try:
        mentor_service.opt_in(student_id, normalized_skill_key)
    except ValueError as exc:
        escaped = quote_plus(str(exc))
        return RedirectResponse(
            url=f"/students/{student_id}/skills/tests/{assessment_id}/result?opt_in_error={escaped}",
            status_code=303,
        )
    return RedirectResponse(
        url=f"/students/{student_id}/skills/tests/{assessment_id}/result?mentor_opted_in=1",
        status_code=303,
    )


@router.post("/opt-out")
def mentor_opt_out(
    student_id: int = Form(...),
    normalized_skill: str = Form(...),
    assessment_id: int = Form(...),
) -> RedirectResponse:
    mentor_service.opt_out(student_id, normalize_skill(normalized_skill))
    return RedirectResponse(
        url=f"/students/{student_id}/skills/tests/{assessment_id}/result?mentor_opted_out=1",
        status_code=303,
    )


@router.post("/hub/opt-in")
def mentor_hub_opt_in(
    student_id: int = Form(...),
    normalized_skill: str = Form(...),
) -> RedirectResponse:
    _student_or_404(student_id)
    skill_key = normalize_skill(normalized_skill)
    try:
        mentor_service.opt_in(student_id, skill_key)
    except ValueError as exc:
        escaped = quote_plus(str(exc))
        return RedirectResponse(
            url=f"/mentor/hub?student_id={student_id}&error={escaped}",
            status_code=303,
        )

    success = quote_plus(f"You are now available as a mentor for {display_skill(skill_key)}.")
    return RedirectResponse(
        url=f"/mentor/hub?student_id={student_id}&success={success}",
        status_code=303,
    )


@router.post("/hub/opt-out")
def mentor_hub_opt_out(
    student_id: int = Form(...),
    normalized_skill: str = Form(...),
) -> RedirectResponse:
    _student_or_404(student_id)
    skill_key = normalize_skill(normalized_skill)
    mentor_service.opt_out(student_id, skill_key)
    success = quote_plus(f"You are now unavailable for {display_skill(skill_key)} mentor requests.")
    return RedirectResponse(
        url=f"/mentor/hub?student_id={student_id}&success={success}",
        status_code=303,
    )


@router.get("/mentors", response_class=HTMLResponse)
def mentor_list_page(
    request: Request,
    skill: str = Query(...),
    student_id: int = Query(...),
    error: str = Query(default=""),
) -> HTMLResponse:
    student = _student_or_404(student_id)
    normalized_skill = normalize_skill(skill)

    mentors = mentor_service.get_ranked_mentors(normalized_skill, requesting_student_id=student_id)

    seeker_sessions = chat_service.get_seeker_sessions(student_id)
    open_mentor_sessions: dict[int, int] = {
        int(s["mentor_id"]): int(s["id"])
        for s in seeker_sessions
        if s["status"] == "open" and str(s["normalized_skill"]) == normalized_skill
    }

    return templates.TemplateResponse(
        "mentor/mentor_list.html",
        {
            "request": request,
            "asset_version": _asset_version(),
            "student": student,
            "skill": normalized_skill,
            "skill_display": display_skill(normalized_skill),
            "mentors": mentors,
            "open_mentor_sessions": open_mentor_sessions,
            "error": error,
            "active_section": "mentor",
        },
    )


@router.post("/sessions/start")
def start_session(
    seeker_id: int = Form(...),
    mentor_id: int = Form(...),
    normalized_skill: str = Form(...),
) -> RedirectResponse:
    _student_or_404(seeker_id)
    _student_or_404(mentor_id)
    normalized_skill_key = normalize_skill(normalized_skill)

    try:
        session_id = chat_service.start_session(
            seeker_id=seeker_id,
            mentor_id=mentor_id,
            normalized_skill=normalized_skill_key,
        )
    except ValueError as exc:
        escaped_err = quote_plus(str(exc))
        escaped_skill = quote_plus(normalized_skill_key)
        return RedirectResponse(
            url=f"/mentor/mentors?skill={escaped_skill}&student_id={seeker_id}&error={escaped_err}",
            status_code=303,
        )
    return RedirectResponse(
        url=f"/mentor/sessions/{session_id}?student_id={seeker_id}",
        status_code=303,
    )


@router.get("/sessions/{session_id}", response_class=HTMLResponse)
def session_page(
    request: Request,
    session_id: int,
    student_id: int = Query(...),
    error: str = Query(default=""),
    success: str = Query(default=""),
) -> HTMLResponse:
    student = _student_or_404(student_id)
    session = chat_service.get_session_with_messages(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="Session not found.")

    _session_access_check(session, student_id)
    is_seeker = student_id == int(session["seeker_id"])

    return templates.TemplateResponse(
        "mentor/mentor_chat.html",
        {
            "request": request,
            "asset_version": _asset_version(),
            "student": student,
            "session": session,
            "is_seeker": is_seeker,
            "skill_display": display_skill(str(session["normalized_skill"])),
            "error": error,
            "success": success,
            "active_section": "mentor",
        },
    )


@router.post("/sessions/{session_id}/message")
def send_message(
    session_id: int,
    student_id: int = Form(...),
    message_text: str = Form(...),
) -> RedirectResponse:
    try:
        chat_service.send_message(
            session_id=session_id,
            sender_id=student_id,
            message_text=message_text,
        )
    except ValueError as exc:
        escaped = quote_plus(str(exc))
        return RedirectResponse(
            url=f"/mentor/sessions/{session_id}?student_id={student_id}&error={escaped}",
            status_code=303,
        )
    return RedirectResponse(
        url=f"/mentor/sessions/{session_id}?student_id={student_id}",
        status_code=303,
    )


@router.post("/sessions/{session_id}/close")
def close_session(
    session_id: int,
    student_id: int = Form(...),
) -> RedirectResponse:
    try:
        chat_service.close_session(session_id=session_id, student_id=student_id)
    except ValueError as exc:
        escaped = quote_plus(str(exc))
        return RedirectResponse(
            url=f"/mentor/sessions/{session_id}?student_id={student_id}&error={escaped}",
            status_code=303,
        )
    return RedirectResponse(
        url=(
            f"/mentor/sessions/{session_id}?student_id={student_id}"
            "&success=Session+marked+as+resolved.+Please+leave+a+review+for+your+mentor."
        ),
        status_code=303,
    )


@router.post("/sessions/{session_id}/cancel")
def cancel_session(
    session_id: int,
    student_id: int = Form(...),
    normalized_skill: str = Form(...),
) -> RedirectResponse:
    try:
        chat_service.cancel_session(session_id=session_id, student_id=student_id)
    except ValueError as exc:
        escaped = quote_plus(str(exc))
        return RedirectResponse(
            url=f"/mentor/sessions/{session_id}?student_id={student_id}&error={escaped}",
            status_code=303,
        )
    escaped_skill = quote_plus(normalize_skill(normalized_skill))
    return RedirectResponse(
        url=f"/mentor/mentors?skill={escaped_skill}&student_id={student_id}",
        status_code=303,
    )


@router.post("/sessions/{session_id}/review")
def submit_review(
    session_id: int,
    student_id: int = Form(...),
    rating: int = Form(...),
    review_text: str = Form(default=""),
) -> RedirectResponse:
    try:
        chat_service.submit_review(
            session_id=session_id,
            student_id=student_id,
            rating=rating,
            review_text=review_text,
        )
    except ValueError as exc:
        escaped = quote_plus(str(exc))
        return RedirectResponse(
            url=f"/mentor/sessions/{session_id}?student_id={student_id}&error={escaped}",
            status_code=303,
        )
    return RedirectResponse(
        url=f"/mentor/sessions/{session_id}?student_id={student_id}&success=Review+submitted.+Thank+you!",
        status_code=303,
    )


@router.get("/hub", response_class=HTMLResponse)
def mentor_hub(
    request: Request,
    student_id: int = Query(...),
    error: str = Query(default=""),
    success: str = Query(default=""),
) -> HTMLResponse:
    student = _student_or_404(student_id)

    sessions_as_mentor = [
        {**item, "skill_display": display_skill(str(item["normalized_skill"]))}
        for item in chat_service.get_mentor_inbox(student_id)
    ]
    sessions_as_seeker = [
        {**item, "skill_display": display_skill(str(item["normalized_skill"]))}
        for item in chat_service.get_seeker_sessions(student_id)
    ]

    mentor_toggle_skills = [
        {**item, "skill_display": display_skill(str(item["normalized_skill"]))}
        for item in mentor_service.list_mentor_skill_toggle_states(student_id)
    ]
    mentor_skill_profiles = mentor_repo.get_all_mentor_skills_for_student(student_id)
    mentor_skills = [
        {**p, "skill_display": display_skill(str(p["normalized_skill"]))}
        for p in mentor_skill_profiles
        if p["opted_in"] == 1
    ]

    open_as_mentor = sum(1 for s in sessions_as_mentor if s["status"] == "open")
    open_as_seeker = sum(1 for s in sessions_as_seeker if s["status"] == "open")

    request_skill = None
    active_goal = goals_repo.get_active_goal(student_id)
    if active_goal is not None:
        goal_skills = goals_repo.list_goal_skills(int(active_goal["id"]))
        active_skill = next((item for item in goal_skills if item["status"] != "completed"), None)
        if active_skill is not None:
            normalized_skill = str(active_skill.get("normalized_skill") or "").strip()
            if not normalized_skill:
                normalized_skill = normalize_skill(str(active_skill.get("skill_name") or ""))
            request_skill = {
                "normalized_skill": normalized_skill,
                "skill_display": str(active_skill.get("skill_name") or display_skill(normalized_skill)),
            }

    return templates.TemplateResponse(
        "mentor/mentor_hub.html",
        {
            "request": request,
            "asset_version": _asset_version(),
            "student": student,
            "sessions_as_mentor": sessions_as_mentor,
            "sessions_as_seeker": sessions_as_seeker,
            "mentor_toggle_skills": mentor_toggle_skills,
            "mentor_skills": mentor_skills,
            "open_as_mentor": open_as_mentor,
            "open_as_seeker": open_as_seeker,
            "request_skill": request_skill,
            "error": error,
            "success": success,
            "active_section": "mentor",
        },
    )
