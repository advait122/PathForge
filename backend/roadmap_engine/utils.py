import ast
from datetime import date, datetime, timedelta, timezone


def utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def parse_skills_field(raw_skills: str | None) -> list[str]:
    if not raw_skills:
        return []

    text = raw_skills.strip()
    if not text:
        return []

    try:
        parsed = ast.literal_eval(text)
        if isinstance(parsed, list):
            return [str(skill).strip() for skill in parsed if str(skill).strip()]
    except (SyntaxError, ValueError):
        pass

    separators = [",", ";", "\n"]
    normalized = text
    for separator in separators[1:]:
        normalized = normalized.replace(separator, separators[0])

    return [chunk.strip() for chunk in normalized.split(separators[0]) if chunk.strip()]


def parse_custom_skills(custom_skills_text: str) -> list[str]:
    if not custom_skills_text:
        return []

    normalized = custom_skills_text.replace("\n", ",").replace(";", ",")
    return [skill.strip() for skill in normalized.split(",") if skill.strip()]


def utc_today() -> date:
    return datetime.now(tz=timezone.utc).date()


def iso_date(value: date) -> str:
    return value.isoformat()


def end_date_from_months(start_date: date, duration_months: int) -> date:
    # Lightweight month handling for roadmap planning.
    return start_date + timedelta(days=duration_months * 30)


def parse_iso_deadline(deadline_text: str | None) -> date | None:
    if not deadline_text:
        return None

    text = deadline_text.strip()
    if not text:
        return None

    for parser in (
        lambda s: datetime.fromisoformat(s).date(),
        lambda s: datetime.fromisoformat(s.replace("Z", "+00:00")).date(),
    ):
        try:
            return parser(text)
        except ValueError:
            continue

    # Last fallback for plain dates with optional time part.
    try:
        return datetime.strptime(text[:10], "%Y-%m-%d").date()
    except ValueError:
        return None
