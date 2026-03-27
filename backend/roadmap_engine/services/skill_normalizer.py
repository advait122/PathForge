import re


SKILL_ALIAS_MAP = {
    "c plus plus": "c++",
    "cpp": "c++",
    "object oriented programming": "oops",
    "oop": "oops",
    "data structures and algorithms": "dsa",
    "data structures & algorithms": "dsa",
    "js": "javascript",
    "ml": "machine learning",
    "dl": "deep learning",
}

DISPLAY_MAP = {
    "c++": "C++",
    "oops": "OOPS",
    "dsa": "DSA",
    "sql": "SQL",
    "html": "HTML",
    "css": "CSS",
    "javascript": "JavaScript",
}


def normalize_skill(skill: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9+ ]+", " ", skill.lower()).strip()
    normalized = re.sub(r"\s+", " ", normalized)
    return SKILL_ALIAS_MAP.get(normalized, normalized)


def display_skill(normalized_skill: str) -> str:
    if normalized_skill in DISPLAY_MAP:
        return DISPLAY_MAP[normalized_skill]
    return normalized_skill.title()


def deduplicate_skills(skills: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []

    for skill in skills:
        key = normalize_skill(skill)
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(skill.strip())

    return deduped

