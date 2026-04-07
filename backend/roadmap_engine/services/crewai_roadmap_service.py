import json
import os
from typing import Any

from pydantic import BaseModel, Field

from backend.roadmap_engine.constants import DEFAULT_SKILL_EFFORT_HOURS, SKILL_EFFORT_ESTIMATE_HOURS
from backend.roadmap_engine.services import goal_intelligence_service, verifier_agent
from backend.roadmap_engine.services.skill_normalizer import display_skill, normalize_skill

try:
    from crewai import Agent, Crew, LLM, Process, Task
except Exception:  # pragma: no cover
    Agent = Crew = LLM = Process = Task = None


GROQ_CREW_MODEL = "groq/llama-3.3-70b-versatile"
DEFAULT_GOAL_CONFIDENCE = 0.45

SKILL_EFFORT_METADATA: dict[str, dict[str, float]] = {
    "dsa": {"foundation_hours": 42.0, "full_hours": 90.0},
    "oops": {"foundation_hours": 14.0, "full_hours": 24.0},
    "dbms": {"foundation_hours": 12.0, "full_hours": 20.0},
    "os": {"foundation_hours": 12.0, "full_hours": 20.0},
    "cn": {"foundation_hours": 12.0, "full_hours": 20.0},
    "sql": {"foundation_hours": 16.0, "full_hours": 26.0},
    "api": {"foundation_hours": 18.0, "full_hours": 30.0},
    "git": {"foundation_hours": 6.0, "full_hours": 10.0},
    "linux": {"foundation_hours": 12.0, "full_hours": 20.0},
    "python": {"foundation_hours": 22.0, "full_hours": 35.0},
    "java": {"foundation_hours": 24.0, "full_hours": 40.0},
    "c++": {"foundation_hours": 24.0, "full_hours": 40.0},
    "html": {"foundation_hours": 8.0, "full_hours": 14.0},
    "css": {"foundation_hours": 8.0, "full_hours": 14.0},
    "javascript": {"foundation_hours": 24.0, "full_hours": 36.0},
    "react": {"foundation_hours": 26.0, "full_hours": 44.0},
    "node": {"foundation_hours": 22.0, "full_hours": 36.0},
    "django": {"foundation_hours": 20.0, "full_hours": 32.0},
    "flask": {"foundation_hours": 16.0, "full_hours": 28.0},
    "spring": {"foundation_hours": 24.0, "full_hours": 42.0},
    "docker": {"foundation_hours": 14.0, "full_hours": 24.0},
    "kubernetes": {"foundation_hours": 18.0, "full_hours": 34.0},
    "cloud": {"foundation_hours": 18.0, "full_hours": 36.0},
    "ci/cd": {"foundation_hours": 14.0, "full_hours": 22.0},
    "terraform": {"foundation_hours": 16.0, "full_hours": 28.0},
    "shell scripting": {"foundation_hours": 10.0, "full_hours": 18.0},
    "machine learning": {"foundation_hours": 34.0, "full_hours": 72.0},
    "deep learning": {"foundation_hours": 40.0, "full_hours": 84.0},
    "system design": {"foundation_hours": 16.0, "full_hours": 30.0},
}

ROLE_HINTS = {
    "backend": ["backend", "api", "server", "django", "flask", "spring", "node"],
    "frontend": ["frontend", "react", "angular", "web developer", "ui"],
    "full_stack": ["full stack", "fullstack", "mern", "mean"],
    "data_ai": ["data scientist", "machine learning", "deep learning", "ai", "ml"],
    "devops": ["devops", "site reliability", "cloud engineer", "platform engineer", "sre"],
    "software_engineering": ["software engineer", "sde", "developer", "programmer"],
}


class GoalValidationOutput(BaseModel):
    is_valid: bool = True
    goal_type: str = "role_only"
    target_company: str | None = None
    target_role_family: str = "Software Engineering"
    normalized_role_family: str = "software_engineering"
    confidence: float = DEFAULT_GOAL_CONFIDENCE
    summary: str = ""
    rejection_reason: str = ""


class ResearchSourceItem(BaseModel):
    title: str = ""
    company: str = ""
    url: str = ""
    source_type: str = ""
    skills: list[str] = Field(default_factory=list)


class ResearchOutput(BaseModel):
    evidence_quality: str = "medium"
    trusted_source_count: int = 0
    source_urls: list[str] = Field(default_factory=list)
    source_items: list[ResearchSourceItem] = Field(default_factory=list)
    summary: str = ""


class SkillCandidateItem(BaseModel):
    skill_name: str
    normalized_skill: str
    importance: int = Field(default=5, ge=1, le=10)
    evidence_support_count: int = Field(default=1, ge=0)
    is_must_have: bool = False
    reason: str = ""


class SkillExtractionOutput(BaseModel):
    candidate_skills: list[SkillCandidateItem] = Field(default_factory=list)
    summary: str = ""


class PriorityPlanOutput(BaseModel):
    ordered_skills: list[str] = Field(default_factory=list)
    must_have_skills: list[str] = Field(default_factory=list)
    stretch_skills: list[str] = Field(default_factory=list)
    reasoning_summary: str = ""


def _build_llm() -> Any:
    if LLM is None:
        return None
    api_key = os.getenv("GROQ_API_KEY", "").strip()
    if not api_key:
        return None
    try:
        return LLM(model=GROQ_CREW_MODEL, api_key=api_key)
    except Exception:
        return None


def _clean_goal_type(goal_text: str, target_company: str | None, normalized_role_family: str) -> str:
    has_company = bool(target_company)
    has_role = normalized_role_family != "software_engineering" or any(
        keyword in goal_text.lower() for keyword in ROLE_HINTS["software_engineering"]
    )
    if has_company and has_role:
        return "company_and_role"
    if has_company:
        return "company_only"
    if has_role:
        return "role_only"
    return "vague"


def _is_goal_obviously_invalid(goal_text: str, role_intent: dict, evidence_summary: dict) -> tuple[bool, str]:
    text = " ".join((goal_text or "").split()).strip()
    if len(text) < 4:
        return True, "Goal is too short to understand."
    lowered = text.lower()
    vague_phrases = {"success", "good job", "best job", "money", "dream", "future", "placement"}
    if lowered in vague_phrases:
        return True, "Goal is too vague. A role, company, or both are needed."
    target_company = role_intent.get("target_company")
    normalized_role_family = str(role_intent.get("normalized_role_family") or "software_engineering")
    sample_size = int(evidence_summary.get("sample_size", 0))
    if not target_company and normalized_role_family == "software_engineering" and sample_size == 0:
        return True, "Goal could not be tied to a company, role, or evidence-backed market pattern."
    return False, ""


def _compact_evidence_record(record: dict) -> dict:
    return {
        "title": str(record.get("title") or "")[:160],
        "company": str(record.get("company") or "")[:120],
        "url": str(record.get("url") or "")[:250],
        "source_type": str(record.get("source_type") or "unknown"),
        "skills": [str(skill) for skill in list(record.get("skills_list") or [])[:8]],
        "snippet": str(record.get("raw_text") or "")[:700],
    }


def _evidence_payload(role_intent: dict, evidence_records: list[dict], evidence_summary: dict) -> str:
    payload = {
        "role_intent": role_intent,
        "evidence_summary": evidence_summary,
        "records": [_compact_evidence_record(record) for record in evidence_records[:10]],
    }
    return json.dumps(payload, ensure_ascii=False)


def _heuristic_goal_validation(goal_text: str, role_intent: dict, evidence_summary: dict) -> GoalValidationOutput:
    target_company = role_intent.get("target_company")
    normalized_role_family = str(role_intent.get("normalized_role_family") or "software_engineering")
    invalid, rejection_reason = _is_goal_obviously_invalid(goal_text, role_intent, evidence_summary)
    return GoalValidationOutput(
        is_valid=not invalid,
        goal_type=_clean_goal_type(goal_text, target_company, normalized_role_family),
        target_company=target_company,
        target_role_family=str(
            role_intent.get("parsed_role_family") or role_intent.get("normalized_role_family") or "Software Engineering"
        ),
        normalized_role_family=normalized_role_family,
        confidence=float(role_intent.get("confidence") or DEFAULT_GOAL_CONFIDENCE),
        summary="Goal validated with local parsing and market evidence.",
        rejection_reason=rejection_reason,
    )


def _build_research_output(evidence_records: list[dict], evidence_summary: dict) -> ResearchOutput:
    source_items = [
        ResearchSourceItem(
            title=str(record.get("title") or ""),
            company=str(record.get("company") or ""),
            url=str(record.get("url") or ""),
            source_type=str(record.get("source_type") or "unknown"),
            skills=[str(skill) for skill in list(record.get("skills_list") or [])[:6]],
        )
        for record in evidence_records[:8]
    ]
    evidence_quality = "high" if int(evidence_summary.get("sample_size", 0)) >= 8 else "medium"
    if int(evidence_summary.get("sample_size", 0)) <= 2:
        evidence_quality = "low"
    return ResearchOutput(
        evidence_quality=evidence_quality,
        trusted_source_count=len(source_items),
        source_urls=[item.url for item in source_items if item.url],
        source_items=source_items,
        summary="Market evidence collected from cached opportunities and live sources when available.",
    )


def _heuristic_skill_candidates(role_intent: dict, evidence_summary: dict, known_skills: list[str]) -> SkillExtractionOutput:
    validation = goal_intelligence_service._validate_required_skills(  # noqa: SLF001
        role_intent=role_intent,
        draft_skills=[str(item.get("skill") or "") for item in evidence_summary.get("top_skills", [])],
        evidence_summary=evidence_summary,
        known_skills=known_skills,
    )
    top_skill_counts = {
        normalize_skill(str(item.get("normalized_skill") or item.get("skill") or "")): int(item.get("count", 0))
        for item in evidence_summary.get("top_skills", [])
    }
    candidates: list[SkillCandidateItem] = []
    for index, skill in enumerate(validation.get("required_skills", []), start=1):
        normalized = normalize_skill(skill)
        if not normalized:
            continue
        candidates.append(
            SkillCandidateItem(
                skill_name=display_skill(normalized),
                normalized_skill=normalized,
                importance=max(3, 11 - index),
                evidence_support_count=top_skill_counts.get(normalized, 1),
                is_must_have=index <= 4,
                reason="Selected from validated role evidence and baseline skill rules.",
            )
        )
    return SkillExtractionOutput(
        candidate_skills=candidates,
        summary="Skills extracted from validated evidence and role baseline heuristics.",
    )


def _heuristic_priority_plan(skill_extraction: SkillExtractionOutput) -> PriorityPlanOutput:
    ordered = [item.skill_name for item in skill_extraction.candidate_skills]
    return PriorityPlanOutput(
        ordered_skills=ordered,
        must_have_skills=ordered[:5],
        stretch_skills=ordered[5:8],
        reasoning_summary="Priority order derived from role alignment, evidence support, and prerequisite importance.",
    )


def _effort_hours_for_skill(normalized_skill: str, target_duration_months: int) -> float:
    metadata = SKILL_EFFORT_METADATA.get(normalized_skill)
    if metadata:
        foundation_hours = float(metadata["foundation_hours"])
        full_hours = float(metadata["full_hours"])
    else:
        full_hours = float(SKILL_EFFORT_ESTIMATE_HOURS.get(normalized_skill, DEFAULT_SKILL_EFFORT_HOURS))
        foundation_hours = max(6.0, round(full_hours * 0.6, 1))

    if target_duration_months <= 2:
        weight = 0.0
    elif target_duration_months <= 4:
        weight = 0.25
    elif target_duration_months <= 6:
        weight = 0.45
    elif target_duration_months <= 12:
        weight = 0.75
    else:
        weight = 1.0
    return round(foundation_hours + ((full_hours - foundation_hours) * weight), 1)


def _timeline_budget_hours(target_duration_months: int, weekly_study_hours: int | None) -> float:
    weekly_hours = max(1, int(weekly_study_hours or 8))
    return round(target_duration_months * 4.35 * weekly_hours, 1)


def _priority_score(
    *,
    normalized_skill: str,
    candidate: SkillCandidateItem,
    priority_map: dict[str, int],
    target_duration_months: int,
) -> float:
    role_priority = priority_map.get(normalized_skill, 999)
    effort = _effort_hours_for_skill(normalized_skill, target_duration_months)
    efficiency = max(0.2, 30.0 / max(effort, 6.0))
    must_have_bonus = 6.0 if candidate.is_must_have else 0.0
    return (
        float(candidate.importance) * 3.0
        + float(candidate.evidence_support_count) * 1.4
        + must_have_bonus
        + efficiency
        - (role_priority * 0.35)
    )


def _select_skills_for_timeline(
    *,
    role_intent: dict,
    skill_extraction: SkillExtractionOutput,
    priority_plan: PriorityPlanOutput,
    known_skills: list[str],
    weekly_study_hours: int | None,
    target_duration_months: int,
) -> dict:
    priority_map = goal_intelligence_service._priority_map_for_role(role_intent)  # noqa: SLF001
    known_skill_keys = {normalize_skill(skill) for skill in known_skills if normalize_skill(skill)}
    ordered_lookup = {normalize_skill(skill): index for index, skill in enumerate(priority_plan.ordered_skills)}
    must_have_keys = {normalize_skill(skill) for skill in priority_plan.must_have_skills if normalize_skill(skill)}

    candidates_by_key: dict[str, SkillCandidateItem] = {}
    for candidate in skill_extraction.candidate_skills:
        normalized = normalize_skill(candidate.normalized_skill or candidate.skill_name)
        if not normalized or normalized in known_skill_keys:
            continue
        if not goal_intelligence_service._clean_skill_candidates([display_skill(normalized)], role_intent):  # noqa: SLF001
            continue
        current = candidates_by_key.get(normalized)
        candidate_copy = candidate.model_copy(
            update={
                "normalized_skill": normalized,
                "skill_name": display_skill(normalized),
                "is_must_have": candidate.is_must_have or normalized in must_have_keys,
            }
        )
        if current is None or candidate_copy.importance > current.importance:
            candidates_by_key[normalized] = candidate_copy

    if not candidates_by_key:
        return {
            "selected_skill_details": [],
            "timeline_budget_hours": _timeline_budget_hours(target_duration_months, weekly_study_hours),
            "selected_hours": 0.0,
        }

    ranked_candidates = sorted(
        candidates_by_key.values(),
        key=lambda item: (
            -(1 if item.is_must_have else 0),
            -_priority_score(
                normalized_skill=item.normalized_skill,
                candidate=item,
                priority_map=priority_map,
                target_duration_months=target_duration_months,
            ),
            ordered_lookup.get(item.normalized_skill, 999),
            priority_map.get(item.normalized_skill, 999),
            item.skill_name,
        ),
    )

    timeline_budget_hours = _timeline_budget_hours(target_duration_months, weekly_study_hours)
    soft_budget = timeline_budget_hours * (1.08 if target_duration_months <= 4 else 1.04)
    selected: list[dict] = []
    consumed_hours = 0.0

    for candidate in ranked_candidates:
        estimated_hours = _effort_hours_for_skill(candidate.normalized_skill, target_duration_months)
        if selected and (consumed_hours + estimated_hours) > soft_budget:
            continue
        selected.append(
            {
                "skill_name": candidate.skill_name,
                "normalized_skill": candidate.normalized_skill,
                "estimated_hours": estimated_hours,
                "skill_source": "crewai_timeline_fit",
                "importance": candidate.importance,
                "evidence_support_count": candidate.evidence_support_count,
                "reason": candidate.reason,
            }
        )
        consumed_hours += estimated_hours

    if not selected:
        first = ranked_candidates[0]
        selected.append(
            {
                "skill_name": first.skill_name,
                "normalized_skill": first.normalized_skill,
                "estimated_hours": _effort_hours_for_skill(first.normalized_skill, target_duration_months),
                "skill_source": "crewai_timeline_fit",
                "importance": first.importance,
                "evidence_support_count": first.evidence_support_count,
                "reason": first.reason,
            }
        )
        consumed_hours = float(selected[0]["estimated_hours"])

    selected.sort(key=lambda item: (priority_map.get(item["normalized_skill"], 999), item["skill_name"]))
    for priority, skill in enumerate(selected, start=1):
        skill["priority"] = priority

    return {
        "selected_skill_details": selected,
        "timeline_budget_hours": timeline_budget_hours,
        "selected_hours": round(consumed_hours, 1),
    }


def _trim_selected_skills_to_budget(
    *,
    role_intent: dict,
    selected_skill_details: list[dict],
    timeline_budget_hours: float,
) -> list[dict]:
    if not selected_skill_details:
        return []
    priority_map = goal_intelligence_service._priority_map_for_role(role_intent)  # noqa: SLF001
    trimmed = list(selected_skill_details)
    hard_budget = timeline_budget_hours * 1.18
    role_family = str(role_intent.get("normalized_role_family") or "software_engineering")
    protected_sources = {"verifier_correction"}
    role_protected_skills = {
        "backend": {"sql", "api", "python", "java", "node"},
        "devops": {"linux", "docker", "ci/cd", "cloud", "kubernetes", "terraform"},
    }
    protected_skills = role_protected_skills.get(role_family, set())

    def total_hours(items: list[dict]) -> float:
        return round(sum(float(item.get("estimated_hours") or 0.0) for item in items), 1)

    while len(trimmed) > 1 and total_hours(trimmed) > hard_budget:
        removable_indexes = [
            index
            for index, item in enumerate(trimmed)
            if str(item.get("skill_source") or "") not in protected_sources
            and str(item.get("normalized_skill") or "") not in protected_skills
        ]
        if not removable_indexes:
            removable_indexes = list(range(len(trimmed) - 1, -1, -1))
        drop_index = max(
            removable_indexes,
            key=lambda index: (
                priority_map.get(str(trimmed[index].get("normalized_skill") or ""), 999),
                float(trimmed[index].get("estimated_hours") or 0.0),
                index,
            ),
        )
        trimmed.pop(drop_index)

    trimmed.sort(key=lambda item: (priority_map.get(item["normalized_skill"], 999), item["skill_name"]))
    for priority, skill in enumerate(trimmed, start=1):
        skill["priority"] = priority
    return trimmed


def _apply_verifier_corrections(role_intent: dict, selected_skill_details: list[dict], issues: list[str]) -> list[dict]:
    corrected = list(selected_skill_details)
    required_keys = {item["normalized_skill"] for item in corrected}
    role_family = str(role_intent.get("normalized_role_family") or "software_engineering")

    def insert_skill(skill_name: str, position: int) -> None:
        normalized = normalize_skill(skill_name)
        if not normalized or normalized in required_keys:
            return
        corrected.insert(
            min(position, len(corrected)),
            {
                "skill_name": display_skill(normalized),
                "normalized_skill": normalized,
                "estimated_hours": _effort_hours_for_skill(normalized, int(role_intent.get("target_duration_months", 6))),
                "skill_source": "verifier_correction",
            },
        )
        required_keys.add(normalized)

    if role_family == "backend":
        if any("missing SQL" in issue for issue in issues):
            insert_skill("SQL", 2)
        if any("missing API fundamentals" in issue for issue in issues):
            insert_skill("API", 3)
        if any("backend programming stack" in issue for issue in issues):
            insert_skill("Python", 1)
    elif role_family == "devops":
        if any("Linux fundamentals" in issue for issue in issues):
            insert_skill("Linux", 0)
        if any("Docker" in issue for issue in issues):
            insert_skill("Docker", 2)
        if any("CI/CD" in issue for issue in issues):
            insert_skill("CI/CD", 4)
        if any("cloud fundamentals" in issue for issue in issues):
            insert_skill("Cloud", 5)
        if any("Kubernetes or Terraform" in issue for issue in issues):
            insert_skill("Kubernetes", 6)

    priority_map = goal_intelligence_service._priority_map_for_role(role_intent)  # noqa: SLF001
    corrected.sort(key=lambda item: (priority_map.get(item["normalized_skill"], 999), item["skill_name"]))
    for priority, skill in enumerate(corrected, start=1):
        skill["priority"] = priority
    return corrected


def _run_crewai_tasks(
    *,
    goal_text: str,
    role_intent: dict,
    evidence_records: list[dict],
    evidence_summary: dict,
    known_skills: list[str],
) -> tuple[GoalValidationOutput, ResearchOutput, SkillExtractionOutput, PriorityPlanOutput]:
    llm = _build_llm()
    if llm is None or Agent is None or Task is None or Crew is None or Process is None:
        raise RuntimeError("CrewAI is unavailable.")

    evidence_json = _evidence_payload(role_intent, evidence_records, evidence_summary)
    known_skills_json = json.dumps(known_skills, ensure_ascii=False)

    goal_validator = Agent(
        role="Goal Validator",
        goal="Validate whether the student goal is actionable and classify it.",
        backstory="You reject vague goals and keep roadmap planning grounded.",
        llm=llm,
        verbose=False,
    )
    research_agent = Agent(
        role="Research Analyst",
        goal="Summarize the strongest trustworthy evidence for the target goal.",
        backstory="You only surface market-backed evidence from the supplied records.",
        llm=llm,
        verbose=False,
    )
    skill_agent = Agent(
        role="Skill Extraction Analyst",
        goal="Extract role-relevant skills from evidence without adding fluff.",
        backstory="You reduce noisy evidence into a clean candidate skill list.",
        llm=llm,
        verbose=False,
    )
    prioritizer = Agent(
        role="Roadmap Prioritizer",
        goal="Order skills so prerequisites and highest-value skills come first.",
        backstory="You care about entry-level hiring signal, dependencies, and realistic scope.",
        llm=llm,
        verbose=False,
    )

    goal_task = Task(
        description=(
            "Return JSON only.\n"
            f"Goal text: {goal_text}\n"
            f"Role intent: {json.dumps(role_intent, ensure_ascii=False)}\n"
            f"Evidence summary: {json.dumps(evidence_summary, ensure_ascii=False)}\n"
            "Classify as company_only, role_only, company_and_role, or vague."
        ),
        expected_output="Structured goal validation JSON.",
        output_pydantic=GoalValidationOutput,
        agent=goal_validator,
    )
    research_task = Task(
        description=(
            "Return JSON only.\n"
            f"Evidence payload: {evidence_json}\n"
            "Summarize trust level, top sources, and a concise evidence summary."
        ),
        expected_output="Structured research summary JSON.",
        output_pydantic=ResearchOutput,
        agent=research_agent,
    )
    skill_task = Task(
        description=(
            "Return JSON only.\n"
            f"Goal text: {goal_text}\n"
            f"Known skills: {known_skills_json}\n"
            f"Evidence payload: {evidence_json}\n"
            "Extract candidate skills with normalized names, importance, evidence support, and must-have flags."
        ),
        expected_output="Structured candidate skill JSON.",
        output_pydantic=SkillExtractionOutput,
        agent=skill_agent,
    )
    priority_task = Task(
        description=(
            "Return JSON only.\n"
            f"Goal text: {goal_text}\n"
            f"Role intent: {json.dumps(role_intent, ensure_ascii=False)}\n"
            f"Evidence summary: {json.dumps(evidence_summary, ensure_ascii=False)}\n"
            "Order skills by priority and separate must-have vs stretch topics."
        ),
        expected_output="Structured priority plan JSON.",
        output_pydantic=PriorityPlanOutput,
        agent=prioritizer,
    )

    crew = Crew(
        agents=[goal_validator, research_agent, skill_agent, prioritizer],
        tasks=[goal_task, research_task, skill_task, priority_task],
        process=Process.sequential,
        verbose=False,
    )
    crew.kickoff()

    goal_output = goal_task.output.pydantic if goal_task.output and goal_task.output.pydantic else None
    research_output = research_task.output.pydantic if research_task.output and research_task.output.pydantic else None
    skill_output = skill_task.output.pydantic if skill_task.output and skill_task.output.pydantic else None
    priority_output = priority_task.output.pydantic if priority_task.output and priority_task.output.pydantic else None
    if not all([goal_output, research_output, skill_output, priority_output]):
        raise RuntimeError("CrewAI did not return structured output.")
    return goal_output, research_output, skill_output, priority_output


def generate_crewai_verified_roadmap(
    *,
    goal_text: str,
    target_duration_months: int,
    known_skills: list[str] | None = None,
    weekly_study_hours: int | None = None,
) -> dict:
    known_skills = known_skills or []
    goal_parse = goal_intelligence_service.parse_goal_text(goal_text)
    role_intent = goal_intelligence_service._build_role_intent(  # noqa: SLF001
        goal_text,
        goal_parse.get("target_company"),
        goal_parse.get("target_role_family"),
        target_duration_months,
    )
    role_intent["confidence"] = goal_parse.get("confidence")
    evidence_records = goal_intelligence_service._collect_evidence_records(role_intent)  # noqa: SLF001
    evidence_summary = goal_intelligence_service._summarize_evidence(evidence_records)  # noqa: SLF001

    used_crewai = False
    try:
        goal_validation, research_output, skill_extraction, priority_plan = _run_crewai_tasks(
            goal_text=goal_text,
            role_intent=role_intent,
            evidence_records=evidence_records,
            evidence_summary=evidence_summary,
            known_skills=known_skills,
        )
        used_crewai = True
    except Exception:
        goal_validation = _heuristic_goal_validation(goal_text, role_intent, evidence_summary)
        research_output = _build_research_output(evidence_records, evidence_summary)
        skill_extraction = _heuristic_skill_candidates(role_intent, evidence_summary, known_skills)
        priority_plan = _heuristic_priority_plan(skill_extraction)

    heuristic_skill_extraction = _heuristic_skill_candidates(role_intent, evidence_summary, known_skills)
    heuristic_candidates = {item.normalized_skill: item for item in heuristic_skill_extraction.candidate_skills}
    merged_candidates: dict[str, SkillCandidateItem] = {
        item.normalized_skill: item for item in skill_extraction.candidate_skills if normalize_skill(item.normalized_skill)
    }
    for normalized, heuristic_item in heuristic_candidates.items():
        merged_candidates.setdefault(normalized, heuristic_item)
    skill_extraction = SkillExtractionOutput(
        candidate_skills=list(merged_candidates.values()),
        summary=skill_extraction.summary or heuristic_skill_extraction.summary,
    )
    ordered_keys = {normalize_skill(skill) for skill in priority_plan.ordered_skills if normalize_skill(skill)}
    merged_order = list(priority_plan.ordered_skills)
    for heuristic_item in heuristic_skill_extraction.candidate_skills:
        if heuristic_item.normalized_skill not in ordered_keys:
            merged_order.append(heuristic_item.skill_name)
            ordered_keys.add(heuristic_item.normalized_skill)
    merged_must_haves = list(priority_plan.must_have_skills)
    must_have_keys = {normalize_skill(skill) for skill in merged_must_haves if normalize_skill(skill)}
    for heuristic_item in heuristic_skill_extraction.candidate_skills[:4]:
        if heuristic_item.normalized_skill not in must_have_keys:
            merged_must_haves.append(heuristic_item.skill_name)
            must_have_keys.add(heuristic_item.normalized_skill)
    priority_plan = PriorityPlanOutput(
        ordered_skills=merged_order,
        must_have_skills=merged_must_haves,
        stretch_skills=priority_plan.stretch_skills,
        reasoning_summary=priority_plan.reasoning_summary or heuristic_skill_extraction.summary,
    )

    invalid, rejection_reason = _is_goal_obviously_invalid(goal_text, role_intent, evidence_summary)
    if invalid:
        goal_validation.is_valid = False
        goal_validation.goal_type = "vague"
        if not goal_validation.rejection_reason:
            goal_validation.rejection_reason = rejection_reason

    selected = _select_skills_for_timeline(
        role_intent=role_intent,
        skill_extraction=skill_extraction,
        priority_plan=priority_plan,
        known_skills=known_skills,
        weekly_study_hours=weekly_study_hours,
        target_duration_months=target_duration_months,
    )
    selected_skill_details = selected.get("selected_skill_details", [])
    if not goal_validation.is_valid:
        selected_skill_details = []

    validation_result = goal_intelligence_service._build_validation_result(  # noqa: SLF001
        role_intent=role_intent,
        draft_skills=priority_plan.ordered_skills or [item.skill_name for item in skill_extraction.candidate_skills],
        validated_skills=[item["skill_name"] for item in selected_skill_details],
        evidence_summary=evidence_summary,
        known_skills=known_skills,
    )
    validation_result["overemphasized_topics"] = []
    validation_result["goal_validation"] = goal_validation.model_dump()
    validation_result["timeline_budget_hours"] = selected.get("timeline_budget_hours", 0.0)
    validation_result["selected_hours"] = selected.get("selected_hours", 0.0)
    validation_result["reasoning_summary"] = priority_plan.reasoning_summary

    planning_result = {
        "required_skills": [item["skill_name"] for item in selected_skill_details],
        "validation_result": validation_result,
    }
    verification_result = verifier_agent.run(
        role_intent=role_intent,
        planning_result=planning_result,
        evidence_summary=evidence_summary,
    )
    if not verification_result.passed:
        selected_skill_details = _apply_verifier_corrections(
            role_intent,
            selected_skill_details,
            verification_result.issues,
        )
        selected_skill_details = _trim_selected_skills_to_budget(
            role_intent=role_intent,
            selected_skill_details=selected_skill_details,
            timeline_budget_hours=float(selected.get("timeline_budget_hours", 0.0)),
        )
        planning_result = {
            "required_skills": [item["skill_name"] for item in selected_skill_details],
            "validation_result": goal_intelligence_service._build_validation_result(  # noqa: SLF001
                role_intent=role_intent,
                draft_skills=priority_plan.ordered_skills or [item.skill_name for item in skill_extraction.candidate_skills],
                validated_skills=[item["skill_name"] for item in selected_skill_details],
                evidence_summary=evidence_summary,
                known_skills=known_skills,
            ),
        }
        planning_result["validation_result"]["overemphasized_topics"] = []
        planning_result["validation_result"]["goal_validation"] = goal_validation.model_dump()
        planning_result["validation_result"]["timeline_budget_hours"] = selected.get("timeline_budget_hours", 0.0)
        planning_result["validation_result"]["selected_hours"] = round(
            sum(float(item["estimated_hours"]) for item in selected_skill_details),
            1,
        )
        planning_result["validation_result"]["reasoning_summary"] = priority_plan.reasoning_summary
        verification_result = verifier_agent.run(
            role_intent=role_intent,
            planning_result=planning_result,
            evidence_summary=evidence_summary,
        )

    rationale = " ".join(part for part in [priority_plan.reasoning_summary.strip(), research_output.summary.strip()] if part)
    goal_parse_output = dict(goal_parse)
    goal_parse_output["is_valid"] = goal_validation.is_valid
    goal_parse_output["goal_type"] = goal_validation.goal_type
    goal_parse_output["rejection_reason"] = goal_validation.rejection_reason

    return {
        "goal_parse": goal_parse_output,
        "role_intent": role_intent,
        "required_skills": [item["skill_name"] for item in selected_skill_details],
        "selected_skill_details": selected_skill_details,
        "source": "crewai_timeline_roadmap_v1" if used_crewai else "crewai_heuristic_fallback_v1",
        "source_opportunity_count": evidence_summary.get("sample_size", 0),
        "rationale": rationale,
        "evidence_summary": evidence_summary,
        "evidence_highlights": planning_result["validation_result"].get("notes", []),
        "validation_result": planning_result["validation_result"],
        "verification_result": verification_result.model_dump(),
        "planner_mode": "crewai" if used_crewai else "heuristic_fallback",
        "agent_trace": {
            "goal_validation": goal_validation.model_dump(),
            "research": research_output.model_dump(),
            "skill_extraction": skill_extraction.model_dump(),
            "priority_plan": priority_plan.model_dump(),
            "selection": {
                "timeline_budget_hours": selected.get("timeline_budget_hours", 0.0),
                "selected_hours": selected.get("selected_hours", 0.0),
                "selected_skill_details": selected_skill_details,
            },
            "verification": verification_result.model_dump(),
            "evidence_sources": research_output.source_urls,
        },
    }
