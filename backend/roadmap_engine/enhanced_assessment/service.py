from typing import Any

from backend.roadmap_engine.enhanced_assessment.coding_languages import (
    default_supported_languages_for_skill,
    locked_coding_language_for_skill,
    normalize_coding_language,
)
from backend.roadmap_engine.enhanced_assessment import coding_repo
from backend.roadmap_engine.enhanced_assessment.coding_builder import build_coding_assessment
from backend.roadmap_engine.enhanced_assessment.mcq_builder import build_mcq_assessment
from backend.roadmap_engine.enhanced_assessment.piston_client import run_code
from backend.roadmap_engine.enhanced_assessment.skill_gate import requires_coding_test
from backend.roadmap_engine.storage import goals_repo


def generate_mcq(skill_name: str, selected_playlist: dict | None) -> tuple[list[dict], list[int]]:
    return build_mcq_assessment(skill_name, selected_playlist)


def ensure_and_attach_coding_assessment(
    assessment: dict,
    *,
    skill_name: str,
    selected_playlist: dict | None,
) -> dict:
    if not requires_coding_test(skill_name):
        assessment["coding_assessment"] = None
        return assessment

    assessment_id = int(assessment["id"])
    existing = coding_repo.get_coding_assessment(assessment_id)
    if existing is None:
        coding_payload = build_coding_assessment(skill_name, selected_playlist)
        questions = coding_payload.get("questions", [])
        coding_repo.create_or_replace_coding_assessment(
            assessment_id=assessment_id,
            goal_id=int(assessment["goal_id"]),
            goal_skill_id=int(assessment["goal_skill_id"]),
            questions=questions,
        )
        existing = coding_repo.get_coding_assessment(assessment_id)
    assessment["coding_assessment"] = _sanitize_for_ui(existing)
    return assessment


def attach_existing_coding_assessment(assessment: dict) -> dict:
    coding = coding_repo.get_coding_assessment(int(assessment["id"]))
    assessment["coding_assessment"] = _sanitize_for_ui(coding)
    return assessment


def run_preview(
    *,
    assessment_id: int,
    question_index: int,
    language: str,
    code: str,
) -> dict:
    coding = coding_repo.get_coding_assessment(assessment_id)
    if coding is None:
        raise ValueError("Coding test is not enabled for this assessment.")
    questions = coding.get("questions", [])
    if question_index < 0 or question_index >= len(questions):
        raise ValueError("Invalid coding question index.")
    question = questions[question_index]
    effective_skill_name = _coding_skill_name(coding)

    if not str(code or "").strip():
        raise ValueError("Code is required.")

    allowed_languages = _allowed_languages_for_question(question, skill_name=effective_skill_name)
    requested_language = normalize_coding_language(language)
    if requested_language not in allowed_languages:
        raise ValueError("Selected language is not allowed for this coding question.")

    sample_cases = [case for case in question.get("test_cases", []) if case.get("is_sample")]
    if not sample_cases:
        sample_cases = list(question.get("test_cases", []))

    case_results = _execute_cases(language=requested_language, code=code, test_cases=sample_cases)
    passed_cases = sum(1 for case in case_results if case["passed"])
    total_cases = len(case_results)
    return {
        "question_id": question.get("question_id"),
        "question_title": question.get("title"),
        "passed_cases": passed_cases,
        "total_cases": total_cases,
        "all_passed": total_cases > 0 and passed_cases == total_cases,
        "results": case_results,
    }


def evaluate_and_submit_coding(
    *,
    assessment: dict,
    skill_name: str,
    coding_submissions: list[dict] | None,
) -> dict:
    if not requires_coding_test(skill_name):
        return {
            "required": False,
            "score_percent": None,
            "passed": True,
            "question_results": [],
        }

    coding = coding_repo.get_coding_assessment(int(assessment["id"]))
    if coding is None:
        return {
            "required": True,
            "score_percent": 0.0,
            "passed": False,
            "question_results": [],
        }

    questions = coding.get("questions", [])
    effective_skill_name = str(skill_name or _coding_skill_name(coding) or "")
    submissions_by_index = _normalize_submissions(
        coding_submissions or [],
        questions=questions,
        skill_name=effective_skill_name,
    )
    question_results: list[dict[str, Any]] = []
    score_accumulator = 0.0

    for index, question in enumerate(questions):
        submission = submissions_by_index.get(index)
        if submission is None:
            question_results.append(
                {
                    "question_id": question.get("question_id"),
                    "title": question.get("title"),
                    "difficulty": question.get("difficulty"),
                    "language": "",
                    "passed_cases": 0,
                    "total_cases": len(question.get("test_cases", [])),
                    "score_percent": 0.0,
                    "passed": False,
                    "results": [],
                    "status": "not_attempted",
                }
            )
            continue

        language = submission["language"]
        code = submission["code"]
        case_results = _execute_cases(
            language=language,
            code=code,
            test_cases=list(question.get("test_cases", [])),
        )
        passed_cases = sum(1 for case in case_results if case["passed"])
        total_cases = len(case_results)
        question_score = (passed_cases / total_cases * 100.0) if total_cases else 0.0
        score_accumulator += question_score

        question_results.append(
            {
                "question_id": question.get("question_id"),
                "title": question.get("title"),
                "difficulty": question.get("difficulty"),
                "language": language,
                "passed_cases": passed_cases,
                "total_cases": total_cases,
                "score_percent": round(question_score, 2),
                "passed": total_cases > 0 and passed_cases == total_cases,
                "results": case_results,
                "status": "attempted",
            }
        )

    total_questions = len(questions)
    score_percent = round((score_accumulator / total_questions), 2) if total_questions else 0.0
    passed = score_percent >= 70.0

    payload = {
        "question_results": question_results,
        "score_percent": score_percent,
        "passed": passed,
    }
    coding_repo.submit_coding_assessment(
        assessment_id=int(assessment["id"]),
        latest_submission=payload,
        score_percent=score_percent,
        passed=passed,
    )

    return {
        "required": True,
        "score_percent": score_percent,
        "passed": passed,
        "question_results": question_results,
    }


def _normalize_submissions(raw: list[dict], *, questions: list[dict], skill_name: str) -> dict[int, dict]:
    normalized: dict[int, dict] = {}
    for item in raw:
        if not isinstance(item, dict):
            continue
        try:
            index = int(item.get("question_index"))
        except (TypeError, ValueError):
            continue
        code = str(item.get("code", "")).strip()
        if not code:
            continue
        if index < 0 or index >= len(questions):
            continue
        allowed_languages = _allowed_languages_for_question(questions[index], skill_name=skill_name)
        language = normalize_coding_language(str(item.get("language", "")))
        if not language and len(allowed_languages) == 1:
            language = allowed_languages[0]
        if language not in allowed_languages:
            continue
        normalized[index] = {"language": language, "code": code}
    return normalized


def _allowed_languages_for_question(question: dict, *, skill_name: str = "") -> list[str]:
    locked_language = locked_coding_language_for_skill(skill_name)
    if locked_language:
        return [locked_language]

    raw_languages = question.get("supported_languages", [])
    supported: list[str] = []
    if isinstance(raw_languages, list):
        for language in raw_languages:
            normalized = normalize_coding_language(str(language))
            if normalized and normalized not in supported:
                supported.append(normalized)

    if not supported:
        legacy_language = normalize_coding_language(str(question.get("language", "")))
        if legacy_language:
            supported.append(legacy_language)

    if supported:
        return supported
    return list(default_supported_languages_for_skill(skill_name))


def _coding_skill_name(coding: dict | None) -> str:
    if not isinstance(coding, dict):
        return ""
    try:
        goal_skill_id = int(coding.get("goal_skill_id") or 0)
    except (TypeError, ValueError):
        goal_skill_id = 0
    if goal_skill_id > 0:
        goal_skill = goals_repo.get_goal_skill(goal_skill_id)
        if goal_skill:
            return str(goal_skill.get("skill_name", "") or "")
    return str(coding.get("skill_name", "") or "")


def _execute_cases(*, language: str, code: str, test_cases: list[dict]) -> list[dict]:
    results: list[dict] = []
    for idx, case in enumerate(test_cases, start=1):
        case_input = str(case.get("input", ""))
        expected = _normalized_output(str(case.get("expected_output", "")))
        execution = run_code(language=language, code=code, stdin=case_input)
        stdout = _normalized_output(str(execution.get("stdout", "")))
        stderr = str(execution.get("stderr", ""))
        runtime_error = str(execution.get("error", ""))
        exit_code = execution.get("exit_code")
        passed = (
            bool(execution.get("ok"))
            and not runtime_error
            and (exit_code in (None, 0))
            and _outputs_match(stdout, expected)
        )
        results.append(
            {
                "case_no": idx,
                "input": case_input,
                "expected_output": expected,
                "actual_output": stdout,
                "stderr": stderr,
                "error": runtime_error,
                "engine": str(execution.get("engine", "")),
                "exit_code": exit_code,
                "passed": passed,
                "is_sample": bool(case.get("is_sample", False)),
            }
        )
    return results


def _normalized_output(value: str) -> str:
    lines = [line.rstrip() for line in value.replace("\r\n", "\n").strip().split("\n")]
    if len(lines) == 1 and lines[0] == "":
        return ""
    return "\n".join(lines)


def _outputs_match(actual: str, expected: str) -> bool:
    if actual == expected:
        return True
    # Accept whitespace-equivalent outputs so newline-vs-space formatting does not cause false failures.
    return actual.split() == expected.split()


def _sanitize_for_ui(coding: dict | None) -> dict | None:
    if coding is None:
        return None
    effective_skill_name = _coding_skill_name(coding)
    locked_language = locked_coding_language_for_skill(effective_skill_name)
    question_summaries = []
    for index, question in enumerate(coding.get("questions", [])):
        question_summaries.append(
            {
                "question_index": index,
                "question_id": question.get("question_id"),
                "difficulty": question.get("difficulty"),
                "title": question.get("title"),
                "statement": question.get("statement"),
                "input_format": question.get("input_format"),
                "output_format": question.get("output_format"),
                "sample_input": question.get("sample_input"),
                "sample_output": question.get("sample_output"),
                "supported_languages": _allowed_languages_for_question(
                    question,
                    skill_name=effective_skill_name,
                ),
                "test_case_count": len(question.get("test_cases", [])),
            }
        )

    latest_submission = coding.get("latest_submission")
    return {
        "assessment_id": coding.get("assessment_id"),
        "skill_name": effective_skill_name,
        "locked_language": locked_language,
        "score_percent": coding.get("score_percent"),
        "passed": coding.get("passed"),
        "submitted_at": coding.get("submitted_at"),
        "questions": question_summaries,
        "last_submission": latest_submission,
    }

