import json
import os
import re
from collections import Counter

from backend.roadmap_engine.services.skill_normalizer import display_skill, normalize_skill
from backend.roadmap_engine.storage import opportunities_repo


GROQ_MODEL = "llama-3.3-70b-versatile"


def _extract_json_object(raw_text: str) -> dict | None:
    start = raw_text.find("{")
    end = raw_text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    try:
        return json.loads(raw_text[start : end + 1])
    except json.JSONDecodeError:
        return None


def _heuristic_company(goal_text: str, company_candidates: list[str]) -> str | None:
    lowered_goal = goal_text.lower()

    for company in company_candidates:
        if company.lower() in lowered_goal:
            return company

    pattern = re.compile(r"\b(?:in|at|for)\s+([a-zA-Z0-9][a-zA-Z0-9 .&-]{1,40})", re.IGNORECASE)
    match = pattern.search(goal_text)
    if match:
        return match.group(1).strip(" .")

    return None


def parse_goal_text(goal_text: str) -> dict:
    company_candidates = opportunities_repo.list_company_names()
    fallback_company = _heuristic_company(goal_text, company_candidates)
    fallback = {
        "target_company": fallback_company,
        "target_role_family": "Software Engineering",
        "confidence": 0.45,
    }

    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        return fallback

    try:
        from groq import Groq

        client = Groq(api_key=api_key)
        prompt = (
            "Extract structured goal details from the text. "
            "Return JSON only with keys: target_company, target_role_family, confidence. "
            "confidence must be between 0 and 1.\n\n"
            f"Goal text: {goal_text}\n"
        )
        response = client.chat.completions.create(
            model=GROQ_MODEL,
            temperature=0,
            messages=[
                {
                    "role": "system",
                    "content": "You extract structured career-goal information in strict JSON.",
                },
                {"role": "user", "content": prompt},
            ],
        )
        parsed = _extract_json_object(response.choices[0].message.content or "")
        if not parsed:
            return fallback

        target_company = parsed.get("target_company") or fallback_company
        target_role_family = parsed.get("target_role_family") or "Software Engineering"
        confidence = parsed.get("confidence")
        try:
            confidence = float(confidence)
        except (TypeError, ValueError):
            confidence = fallback["confidence"]

        normalized_company = None
        if target_company:
            company_lower = target_company.strip().lower()
            for company in company_candidates:
                if company.lower() == company_lower:
                    normalized_company = company
                    break
            if normalized_company is None:
                normalized_company = target_company.strip()

        return {
            "target_company": normalized_company,
            "target_role_family": target_role_family.strip(),
            "confidence": max(0.0, min(1.0, confidence)),
        }
    except Exception:
        return fallback


def _skill_counter_from_opportunities(opportunities: list[dict]) -> Counter:
    counter: Counter = Counter()
    display_lookup: dict[str, str] = {}
    for item in opportunities:
        for skill in item.get("skills_list", []):
            normalized = normalize_skill(skill)
            if not normalized:
                continue
            counter[normalized] += 1
            display_lookup.setdefault(normalized, skill.strip() or display_skill(normalized))
    counter.display_lookup = display_lookup  # type: ignore[attr-defined]
    return counter


def _fallback_required_skills(goal_text: str, target_company: str | None) -> list[str]:
    opportunities = opportunities_repo.list_by_company(target_company, limit=200) if target_company else []
    counter = _skill_counter_from_opportunities(opportunities)
    display_lookup = getattr(counter, "display_lookup", {})

    if counter:
        top = [display_lookup[key] for key, _ in counter.most_common(10)]
        return top

    baseline = ["DSA", "OOPS", "SQL", "Python", "C++", "Java"]
    lowered_goal = goal_text.lower()
    if "frontend" in lowered_goal:
        baseline.extend(["JavaScript", "HTML", "CSS"])
    if "ai" in lowered_goal or "ml" in lowered_goal:
        baseline.extend(["Machine Learning", "Deep Learning"])
    return baseline


def synthesize_required_skills(goal_text: str, target_company: str | None) -> dict:
    opportunities = opportunities_repo.list_by_company(target_company, limit=120) if target_company else []
    fallback_skills = _fallback_required_skills(goal_text, target_company)

    sample_postings = []
    for item in opportunities[:10]:
        sample_postings.append(
            {
                "title": item.get("title"),
                "type": item.get("type"),
                "skills": item.get("skills_list", []),
            }
        )

    api_key = os.getenv("GROQ_API_KEY")
    if not api_key or not sample_postings:
        return {
            "required_skills": fallback_skills,
            "source": "opportunity_frequency_fallback",
            "source_opportunity_count": len(opportunities),
        }

    try:
        from groq import Groq

        client = Groq(api_key=api_key)
        prompt = (
            "Given career goal text and opportunity samples, produce a practical skill list.\n"
            "Return JSON only with keys required_skills (array of strings) and rationale (short string).\n"
            "Keep required_skills to 6-15 items ordered by priority.\n\n"
            f"Goal text: {goal_text}\n"
            f"Target company: {target_company}\n"
            f"Opportunity samples: {json.dumps(sample_postings, ensure_ascii=False)}\n"
        )
        response = client.chat.completions.create(
            model=GROQ_MODEL,
            temperature=0.2,
            messages=[
                {
                    "role": "system",
                    "content": "You infer required skills from job/hackathon opportunity data in strict JSON.",
                },
                {"role": "user", "content": prompt},
            ],
        )
        parsed = _extract_json_object(response.choices[0].message.content or "")
        if not parsed:
            raise ValueError("Invalid JSON from LLM")

        skills = parsed.get("required_skills")
        if not isinstance(skills, list) or len(skills) == 0:
            raise ValueError("Missing required_skills")

        cleaned = []
        seen = set()
        for skill in skills:
            text = str(skill).strip()
            if not text:
                continue
            key = normalize_skill(text)
            if not key or key in seen:
                continue
            seen.add(key)
            cleaned.append(text)

        if not cleaned:
            raise ValueError("No valid skills after cleanup")

        return {
            "required_skills": cleaned,
            "source": "llm_from_company_opportunities",
            "source_opportunity_count": len(opportunities),
            "rationale": parsed.get("rationale", ""),
        }
    except Exception:
        return {
            "required_skills": fallback_skills,
            "source": "opportunity_frequency_fallback",
            "source_opportunity_count": len(opportunities),
        }
