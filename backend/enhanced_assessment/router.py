"""
FastAPI router for the enhanced assessment module.

Routes:
  GET  /students/{student_id}/skills/{goal_skill_id}/coding-test
  POST /students/{student_id}/skills/coding-tests/{coding_assessment_id}/submit
  GET  /students/{student_id}/skills/coding-tests/{coding_assessment_id}/result
"""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from backend.enhanced_assessment import coding_repo
from backend.enhanced_assessment import enhanced_assessment_service as svc
from backend.roadmap_engine.storage import students_repo

router = APIRouter()

# Resolve templates directory relative to this file so there is no circular import
_TEMPLATES_DIR = Path(__file__).resolve().parents[2] / "frontend" / "templates"
templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))


def _get_student_or_redirect(student_id: int):
    """Return the student dict or None if not found."""
    try:
        return students_repo.get_student(student_id)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# GET /students/{student_id}/skills/{goal_skill_id}/coding-test
# ---------------------------------------------------------------------------

@router.get(
    "/students/{student_id}/skills/{goal_skill_id}/coding-test",
    response_class=HTMLResponse,
)
async def show_coding_test(
    request: Request,
    student_id: int,
    goal_skill_id: int,
):
    student = _get_student_or_redirect(student_id)
    if student is None:
        return RedirectResponse("/")

    error: str | None = None
    coding_assessment = None

    try:
        from backend.roadmap_engine.storage import assessment_repo, goals_repo

        goal = goals_repo.get_active_goal(student_id)
        if goal is None:
            raise ValueError("Active goal not found.")

        goal_skill = goals_repo.get_goal_skill(goal_skill_id)
        if goal_skill is None:
            raise ValueError("Skill not found.")

        # Find the latest passed MCQ assessment for this skill
        latest_mcq = assessment_repo.get_latest_assessment(goal_skill_id)
        if latest_mcq is None or latest_mcq.get("passed") != 1:
            raise ValueError("You must pass the MCQ test first.")

        # Check if skill is already completed (no coding test needed)
        if goal_skill["status"] == "completed":
            return RedirectResponse(
                f"/students/{student_id}/dashboard?section=tests"
            )

        coding_assessment = svc.generate_coding_assessment(
            student_id=student_id,
            goal_skill_id=goal_skill_id,
            skill_assessment_id=latest_mcq["id"],
        )

    except ValueError as exc:
        error = str(exc)

    return templates.TemplateResponse(
        request,
        "coding_test.html",
        {
            "request": request,
            "student": student,
            "coding_assessment": coding_assessment,
            "show_results": False,
            "error": error,
        },
    )


# ---------------------------------------------------------------------------
# POST /students/{student_id}/skills/coding-tests/{coding_assessment_id}/submit
# ---------------------------------------------------------------------------

@router.post(
    "/students/{student_id}/skills/coding-tests/{coding_assessment_id}/submit",
    response_class=HTMLResponse,
)
async def submit_coding_test(
    request: Request,
    student_id: int,
    coding_assessment_id: int,
):
    student = _get_student_or_redirect(student_id)
    if student is None:
        return RedirectResponse("/")

    # Parse form data — code_0, code_1, ...
    form = await request.form()

    ca = coding_repo.get_coding_assessment(coding_assessment_id)
    if ca is None:
        return RedirectResponse(f"/students/{student_id}/dashboard?section=tests")

    num_questions = len(ca.get("questions") or [])
    code_submissions: list[str] = []
    for i in range(num_questions):
        code = str(form.get(f"code_{i}", "")).strip()
        code_submissions.append(code)

    error: str | None = None
    result = None

    try:
        result = svc.submit_coding_assessment(
            student_id=student_id,
            coding_assessment_id=coding_assessment_id,
            code_submissions=code_submissions,
        )
    except ValueError as exc:
        error = str(exc)
        result = ca

    if error is None:
        return RedirectResponse(
            f"/students/{student_id}/skills/coding-tests/{coding_assessment_id}/result",
            status_code=303,
        )

    # Re-render the test page with the error
    return templates.TemplateResponse(
        request,
        "coding_test.html",
        {
            "request": request,
            "student": student,
            "coding_assessment": result,
            "show_results": False,
            "error": error,
        },
    )


# ---------------------------------------------------------------------------
# GET /students/{student_id}/skills/coding-tests/{coding_assessment_id}/result
# ---------------------------------------------------------------------------

@router.get(
    "/students/{student_id}/skills/coding-tests/{coding_assessment_id}/result",
    response_class=HTMLResponse,
)
async def coding_test_result(
    request: Request,
    student_id: int,
    coding_assessment_id: int,
):
    student = _get_student_or_redirect(student_id)
    if student is None:
        return RedirectResponse("/")

    ca = coding_repo.get_coding_assessment(coding_assessment_id)
    if ca is None:
        return RedirectResponse(f"/students/{student_id}/dashboard?section=tests")

    # Attach computed summary fields if not already present
    if "final_score" not in ca:
        from backend.roadmap_engine.storage import assessment_repo
        from backend.enhanced_assessment.grader import combined_score, grade_coding

        mcq_assessment = assessment_repo.get_assessment(ca["skill_assessment_id"])
        mcq_score = float(mcq_assessment["score_percent"] or 0.0) if mcq_assessment else 0.0
        coding_grade = grade_coding(ca.get("execution_results") or [])
        coding_score = float(ca.get("score_percent") or 0.0)
        final, passed = combined_score(mcq_score, coding_score)
        ca["mcq_score"] = mcq_score
        ca["coding_score"] = coding_score
        ca["final_score"] = final
        ca["passed_overall"] = passed
        ca["coding_grade"] = coding_grade

    return templates.TemplateResponse(
        request,
        "coding_test.html",
        {
            "request": request,
            "student": student,
            "coding_assessment": ca,
            "show_results": True,
            "error": None,
        },
    )
