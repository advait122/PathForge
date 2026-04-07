import json
import os
import re
from typing import Any

from pydantic import BaseModel, Field

from backend.roadmap_engine.services.skill_normalizer import display_skill, normalize_skill
from backend.roadmap_engine.utils import parse_iso_deadline, utc_now_iso, utc_today

try:
    from crewai import Agent, Crew, LLM, Process, Task
except Exception:  # pragma: no cover
    Agent = Crew = LLM = Process = Task = None


GROQ_CREW_MODEL = "groq/llama-3.3-70b-versatile"
INTERNSHIP_HINTS = ("intern", "internship", "campus", "graduate", "new grad", "student")
EXPERIENCED_HINTS = ("2+ years", "3+ years", "4+ years", "5+ years", "senior", "staff", "lead", "manager")
STUDENT_FRIENDLY_HINTS = (
    "0-1 years",
    "0 years",
    "freshers",
    "fresher",
    "students can apply",
    "entry level",
    "campus hiring",
    "university",
)
TYPE_HINTS = {
    "internship": ("intern", "internship"),
    "hackathon": ("hackathon", "challenge"),
    "job": ("job", "role", "engineer", "developer", "analyst"),
}
SKILL_PATTERNS = {
    "python": ("python",),
    "sql": ("sql", "mysql", "postgres", "postgresql"),
    "machine learning": ("machine learning", " ml "),
    "deep learning": ("deep learning",),
    "git": ("git", "github"),
    "dsa": ("data structures", "algorithms", " dsa "),
    "dbms": ("dbms", "database management"),
    "os": ("operating system", " os "),
    "cn": ("computer networks", "networking"),
    "java": ("java",),
    "c++": ("c++", "cpp"),
    "javascript": ("javascript", " js ", "typescript"),
    "react": ("react",),
    "html": ("html",),
    "css": ("css",),
    "api": (" api ", "rest", "restful"),
    "docker": ("docker",),
    "linux": ("linux",),
    "cloud": ("aws", "azure", "gcp", "cloud"),
    "tensorflow": ("tensorflow",),
    "pytorch": ("pytorch",),
    "statistics": ("statistics", "probability"),
}
GENERIC_TITLE_EXACT = {
    "events",
    "jobs",
    "job",
    "internship",
    "internships",
    "careers",
    "career",
    "job openings",
    "opportunities",
    "search results",
}
GENERIC_TITLE_CONTAINS = (
    "utm_source",
    "menu dropdown",
    "search result",
    "all jobs",
    "job categories",
    "career programs",
)


class OpportunityDiscoveryOutput(BaseModel):
    is_opportunity: bool = True
    opportunity_type: str = "job"
    confidence: float = 0.5
    summary: str = ""


class OpportunityExtractionOutput(BaseModel):
    title: str = ""
    company: str = ""
    opportunity_type: str = "job"
    deadline: str | None = None
    location: str = ""
    description_summary: str = ""
    cgpa_requirement: float | None = None
    backlog_allowed: bool | None = None
    required_skills: list[str] = Field(default_factory=list)
    optional_skills: list[str] = Field(default_factory=list)


class OpportunityEligibilityOutput(BaseModel):
    audience_type: str = "unknown"
    student_friendly: bool = False
    experience_min: int = 0
    experience_max: int = 0
    reason: str = ""


class OpportunitySkillValidationOutput(BaseModel):
    role_family: str = "software_engineering"
    core_skills: list[str] = Field(default_factory=list)
    secondary_skills: list[str] = Field(default_factory=list)
    normalized_skills: list[str] = Field(default_factory=list)
    quality_score: float = 0.5


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


def _normalize_type(raw_type: str) -> str:
    lowered = str(raw_type or "").lower()
    for canonical, hints in TYPE_HINTS.items():
        if any(token in lowered for token in hints):
            return canonical
    return "job"


def _infer_skills(text: str, seed_skills: list[str] | None = None) -> list[str]:
    haystack = f" {text.lower()} "
    found: list[str] = []
    seen: set[str] = set()
    for skill in seed_skills or []:
        key = normalize_skill(skill)
        if key and key not in seen:
            seen.add(key)
            found.append(display_skill(key))
    for key, patterns in SKILL_PATTERNS.items():
        if any(pattern in haystack for pattern in patterns):
            normalized = normalize_skill(key)
            if normalized and normalized not in seen:
                seen.add(normalized)
                found.append(display_skill(normalized))
    return found


def _infer_role_family(skills: list[str], title: str, summary: str) -> str:
    text = f"{title} {summary}".lower()
    keys = {normalize_skill(skill) for skill in skills if normalize_skill(skill)}
    if {"machine learning", "deep learning", "python"} & keys or any(token in text for token in ("data scientist", "machine learning", "ml engineer")):
        return "data_ai"
    if {"html", "css", "javascript", "react"} & keys or "frontend" in text:
        return "frontend"
    if {"python", "java", "api", "sql"} & keys or "backend" in text:
        return "backend"
    if {"docker", "linux", "cloud", "kubernetes"} & keys or "devops" in text:
        return "devops"
    return "software_engineering"


def _heuristic_audience(clean_text: str, title: str, opportunity_type: str) -> OpportunityEligibilityOutput:
    haystack = f"{title} {opportunity_type} {clean_text}".lower()
    audience_type = "unknown"
    student_friendly = False
    experience_min = 0
    experience_max = 0

    if any(token in haystack for token in INTERNSHIP_HINTS):
        audience_type = "internship"
        student_friendly = True
    if any(token in haystack for token in STUDENT_FRIENDLY_HINTS):
        student_friendly = True
        if audience_type == "unknown":
            audience_type = "fresher"
    if any(token in haystack for token in EXPERIENCED_HINTS):
        audience_type = "experienced"
        student_friendly = False
        experience_min = max(experience_min, 2)
        experience_max = max(experience_max, 5)

    year_matches = re.findall(r"(\d+)\s*\+?\s*(?:years?|yrs?)", haystack)
    if year_matches:
        years = [int(item) for item in year_matches]
        experience_min = max(experience_min, min(years))
        experience_max = max(experience_max, max(years))
        if experience_min >= 2:
            audience_type = "experienced"
            student_friendly = False
        elif audience_type == "unknown":
            audience_type = "fresher"
            student_friendly = True

    if audience_type == "unknown":
        audience_type = "internship" if opportunity_type == "internship" else "fresher"
        student_friendly = opportunity_type in {"internship", "hackathon"}

    return OpportunityEligibilityOutput(
        audience_type=audience_type,
        student_friendly=student_friendly,
        experience_min=experience_min,
        experience_max=experience_max,
        reason="Student friendly" if student_friendly else "Experienced-only or unclear audience",
    )


def _heuristic_extract(clean_text: str, extracted_seed: dict, url: str) -> tuple[OpportunityDiscoveryOutput, OpportunityExtractionOutput, OpportunityEligibilityOutput, OpportunitySkillValidationOutput]:
    title = str(extracted_seed.get("title") or "").strip()
    company = str(extracted_seed.get("company") or "").strip()
    opportunity_type = _normalize_type(str(extracted_seed.get("type") or "job"))
    skills = _infer_skills(clean_text, list(extracted_seed.get("skills") or []))
    optional_skills = skills[3:6]
    core_skills = skills[:3] if len(skills) > 3 else skills[:]
    if not title:
        title = re.sub(r"[-_/]+", " ", url.rstrip("/").split("/")[-1]).strip().title() or "Opportunity"
    if not company:
        company = str(extracted_seed.get("source_name") or "").strip()
    description_summary = " ".join(clean_text.split())[:320]
    discovery = OpportunityDiscoveryOutput(
        is_opportunity=bool(title and company),
        opportunity_type=opportunity_type,
        confidence=0.7 if title and company else 0.3,
        summary="Heuristic page classification.",
    )
    extraction = OpportunityExtractionOutput(
        title=title,
        company=company,
        opportunity_type=opportunity_type,
        deadline=str(extracted_seed.get("deadline") or "") or None,
        location=str(extracted_seed.get("location") or ""),
        description_summary=description_summary,
        cgpa_requirement=None,
        backlog_allowed=None,
        required_skills=core_skills,
        optional_skills=optional_skills,
    )
    eligibility = _heuristic_audience(clean_text, title, opportunity_type)
    skill_validation = OpportunitySkillValidationOutput(
        role_family=_infer_role_family(skills, title, description_summary),
        core_skills=core_skills,
        secondary_skills=optional_skills,
        normalized_skills=[normalize_skill(skill) for skill in skills if normalize_skill(skill)],
        quality_score=0.75 if eligibility.student_friendly and core_skills else 0.45,
    )
    return discovery, extraction, eligibility, skill_validation


def _run_crewai(clean_text: str, extracted_seed: dict, source_name: str, url: str) -> tuple[OpportunityDiscoveryOutput, OpportunityExtractionOutput, OpportunityEligibilityOutput, OpportunitySkillValidationOutput]:
    llm = _build_llm()
    if llm is None or Agent is None or Crew is None or Task is None or Process is None:
        raise RuntimeError("CrewAI unavailable")
    prompt_seed = json.dumps(
        {
            "seed": extracted_seed,
            "source_name": source_name,
            "url": url,
            "text": " ".join(clean_text.split())[:9000],
        },
        ensure_ascii=False,
    )
    agents = [
        Agent(role="Opportunity Discovery Agent", goal="Decide whether the page contains a real opportunity for students or freshers.", backstory="You reject junk pages and classify real opportunities.", llm=llm, verbose=False),
        Agent(role="Opportunity Extraction Agent", goal="Extract structured opportunity fields from the page text.", backstory="You convert messy opportunity text into clean structured fields.", llm=llm, verbose=False),
        Agent(role="Eligibility Classification Agent", goal="Classify the audience as internship, fresher, experienced, or unknown and decide student-friendliness.", backstory="You are careful about fresher-only filtering.", llm=llm, verbose=False),
        Agent(role="Skill Validation Agent", goal="Split skills into core and secondary skills and remove fluff.", backstory="You keep only role-relevant skills with realistic quality.", llm=llm, verbose=False),
    ]
    tasks = [
        Task(description=f"Return JSON only.\n{prompt_seed}\nFields: is_opportunity, opportunity_type, confidence, summary.", expected_output="Discovery JSON", output_pydantic=OpportunityDiscoveryOutput, agent=agents[0]),
        Task(description=f"Return JSON only.\n{prompt_seed}\nFields: title, company, opportunity_type, deadline, location, description_summary, cgpa_requirement, backlog_allowed, required_skills, optional_skills.", expected_output="Extraction JSON", output_pydantic=OpportunityExtractionOutput, agent=agents[1]),
        Task(description=f"Return JSON only.\n{prompt_seed}\nFields: audience_type, student_friendly, experience_min, experience_max, reason.", expected_output="Eligibility JSON", output_pydantic=OpportunityEligibilityOutput, agent=agents[2]),
        Task(description=f"Return JSON only.\n{prompt_seed}\nFields: role_family, core_skills, secondary_skills, normalized_skills, quality_score.", expected_output="Skill JSON", output_pydantic=OpportunitySkillValidationOutput, agent=agents[3]),
    ]
    crew = Crew(agents=agents, tasks=tasks, process=Process.sequential, verbose=False)
    crew.kickoff()
    outputs = []
    for task in tasks:
        if not task.output or not task.output.pydantic:
            raise RuntimeError("Missing structured output")
        outputs.append(task.output.pydantic)
    return tuple(outputs)  # type: ignore[return-value]


def _sanitize_deadline(deadline_text: str | None) -> str | None:
    parsed = parse_iso_deadline(deadline_text)
    return parsed.isoformat() if parsed else None


def _deterministic_keep(discovery: OpportunityDiscoveryOutput, eligibility: OpportunityEligibilityOutput, extraction: OpportunityExtractionOutput, skills: OpportunitySkillValidationOutput) -> bool:
    if not discovery.is_opportunity:
        return False
    if not extraction.title or not extraction.company:
        return False
    lowered_title = extraction.title.strip().lower()
    if lowered_title.startswith("?"):
        return False
    if lowered_title in GENERIC_TITLE_EXACT:
        return False
    if re.fullmatch(r"[a-z0-9&.,'()\\-\\s]+ jobs", lowered_title):
        return False
    if any(token in lowered_title for token in GENERIC_TITLE_CONTAINS):
        return False
    if not eligibility.student_friendly and eligibility.audience_type == "experienced":
        return False
    if eligibility.experience_min >= 2:
        return False
    if not skills.core_skills and not skills.normalized_skills:
        return False
    deadline = parse_iso_deadline(extraction.deadline)
    if deadline and deadline < utc_today():
        return False
    return True


def extract_and_validate_opportunity(
    *,
    clean_text: str,
    extracted_seed: dict,
    source_name: str,
    source: str,
    url: str,
    content_hash: str,
) -> dict | None:
    try:
        discovery, extraction, eligibility, skill_validation = _run_crewai(clean_text, extracted_seed, source_name, url)
        planner_mode = "crewai"
    except Exception:
        discovery, extraction, eligibility, skill_validation = _heuristic_extract(clean_text, extracted_seed, url)
        planner_mode = "heuristic_fallback"

    normalized_skills = [normalize_skill(skill) for skill in skill_validation.normalized_skills if normalize_skill(skill)]
    if not normalized_skills:
        normalized_skills = [normalize_skill(skill) for skill in skill_validation.core_skills + skill_validation.secondary_skills if normalize_skill(skill)]
    normalized_skills = list(dict.fromkeys(normalized_skills))
    core_skills = [display_skill(normalize_skill(skill)) for skill in skill_validation.core_skills if normalize_skill(skill)]
    secondary_skills = [display_skill(normalize_skill(skill)) for skill in skill_validation.secondary_skills if normalize_skill(skill)]
    if not core_skills:
        core_skills = [display_skill(skill) for skill in normalized_skills[:3]]
    if not secondary_skills:
        secondary_skills = [display_skill(skill) for skill in normalized_skills[3:6]]

    sanitized_deadline = _sanitize_deadline(extraction.deadline)
    extraction.deadline = sanitized_deadline
    if not _deterministic_keep(discovery, eligibility, extraction, skill_validation):
        return None

    return {
        "title": extraction.title.strip(),
        "company": extraction.company.strip(),
        "type": _normalize_type(extraction.opportunity_type),
        "deadline": sanitized_deadline,
        "skills": [display_skill(skill) for skill in normalized_skills],
        "core_skills": core_skills,
        "secondary_skills": secondary_skills,
        "normalized_skills": normalized_skills,
        "audience_type": eligibility.audience_type,
        "student_friendly": 1 if eligibility.student_friendly else 0,
        "experience_min": int(eligibility.experience_min),
        "experience_max": int(eligibility.experience_max),
        "location": extraction.location.strip(),
        "cgpa_requirement": extraction.cgpa_requirement,
        "backlog_allowed": None if extraction.backlog_allowed is None else (1 if extraction.backlog_allowed else 0),
        "description_summary": extraction.description_summary.strip(),
        "quality_score": round(float(skill_validation.quality_score), 2),
        "source": source,
        "source_url": url,
        "application_url": url,
        "content_hash": content_hash,
        "is_active": 1,
        "fetched_at": utc_now_iso(),
        "last_validated_at": utc_now_iso(),
        "agent_trace_json": json.dumps(
            {
                "planner_mode": planner_mode,
                "discovery": discovery.model_dump(),
                "extraction": extraction.model_dump(),
                "eligibility": eligibility.model_dump(),
                "skill_validation": skill_validation.model_dump(),
            },
            ensure_ascii=False,
        ),
    }
