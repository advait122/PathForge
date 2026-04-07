import json

from backend.roadmap_engine.storage.database import get_connection
from backend.roadmap_engine.utils import parse_skills_field


def _deserialize_json_list(raw_value: str | None) -> list[str]:
    text = str(raw_value or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item).strip() for item in parsed if str(item).strip()]


def _normalize_row(row: dict) -> dict:
    row_dict = dict(row)
    row_dict["skills_list"] = parse_skills_field(row_dict.get("skills"))
    row_dict["core_skills"] = _deserialize_json_list(row_dict.get("core_skills_json"))
    row_dict["secondary_skills"] = _deserialize_json_list(row_dict.get("secondary_skills_json"))
    row_dict["normalized_skills"] = _deserialize_json_list(row_dict.get("normalized_skills_json"))
    return row_dict


def list_opportunities(
    *,
    search: str = "",
    opportunity_type: str = "",
    company: str = "",
    deadline_before: str = "",
) -> list[dict]:
    where_clauses = ["COALESCE(is_active, 1) = 1", "(deadline IS NULL OR date(deadline) >= date('now'))"]
    parameters: list[str] = []

    if search:
        where_clauses.append("(title LIKE ? OR company LIKE ? OR skills LIKE ? OR description_summary LIKE ?)")
        like_pattern = f"%{search}%"
        parameters.extend([like_pattern, like_pattern, like_pattern, like_pattern])

    if opportunity_type:
        where_clauses.append("type = ?")
        parameters.append(opportunity_type)

    if company:
        where_clauses.append("company LIKE ?")
        parameters.append(f"%{company}%")

    if deadline_before:
        where_clauses.append("deadline IS NOT NULL AND date(deadline) <= date(?)")
        parameters.append(deadline_before)

    where_sql = " AND ".join(where_clauses)
    query = f"""
        SELECT
            id, title, company, type, audience_type, student_friendly, experience_min, experience_max,
            deadline, skills, core_skills_json, secondary_skills_json, normalized_skills_json,
            location, cgpa_requirement, backlog_allowed, description_summary,
            url, application_url, source, source_url, quality_score, is_active, last_validated_at, last_updated
        FROM opportunities
        WHERE {where_sql}
        ORDER BY
            CASE WHEN deadline IS NULL THEN 1 ELSE 0 END,
            deadline ASC,
            quality_score DESC,
            last_updated DESC
        LIMIT 200
    """

    connection = get_connection()
    try:
        rows = connection.execute(query, parameters).fetchall()
    finally:
        connection.close()

    return [_normalize_row(dict(row)) for row in rows]


def get_opportunity(opportunity_id: int) -> dict | None:
    connection = get_connection()
    try:
        row = connection.execute(
            """
            SELECT
                id, title, company, type, audience_type, student_friendly, experience_min, experience_max,
                deadline, skills, core_skills_json, secondary_skills_json, normalized_skills_json,
                location, cgpa_requirement, backlog_allowed, description_summary,
                url, application_url, source, source_url, quality_score, is_active, last_validated_at, last_updated
            FROM opportunities
            WHERE id = ?
            """,
            (opportunity_id,),
        ).fetchone()
    finally:
        connection.close()

    return _normalize_row(dict(row)) if row else None


def list_filter_options() -> dict:
    connection = get_connection()
    try:
        type_rows = connection.execute(
            """
            SELECT DISTINCT type
            FROM opportunities
            WHERE COALESCE(is_active, 1) = 1 AND type IS NOT NULL AND TRIM(type) != ''
            ORDER BY type ASC
            """
        ).fetchall()
        company_rows = connection.execute(
            """
            SELECT DISTINCT company
            FROM opportunities
            WHERE COALESCE(is_active, 1) = 1 AND company IS NOT NULL AND TRIM(company) != ''
            ORDER BY company ASC
            LIMIT 200
            """
        ).fetchall()
    finally:
        connection.close()

    return {
        "types": [row["type"] for row in type_rows],
        "companies": [row["company"] for row in company_rows],
    }


def list_company_names() -> list[str]:
    connection = get_connection()
    try:
        rows = connection.execute(
            """
            SELECT DISTINCT company
            FROM opportunities
            WHERE COALESCE(is_active, 1) = 1 AND company IS NOT NULL AND TRIM(company) != ''
            ORDER BY company ASC
            """
        ).fetchall()
    finally:
        connection.close()
    return [row["company"] for row in rows]


def list_by_company(company_name: str, limit: int = 100) -> list[dict]:
    connection = get_connection()
    try:
        rows = connection.execute(
            """
            SELECT
                id, title, company, type, audience_type, student_friendly, experience_min, experience_max,
                deadline, skills, core_skills_json, secondary_skills_json, normalized_skills_json,
                location, cgpa_requirement, backlog_allowed, description_summary,
                url, application_url, source, source_url, quality_score, is_active, last_validated_at, last_updated
            FROM opportunities
            WHERE COALESCE(is_active, 1) = 1 AND lower(company) = lower(?)
            ORDER BY
                CASE WHEN deadline IS NULL THEN 1 ELSE 0 END,
                deadline ASC,
                quality_score DESC,
                id DESC
            LIMIT ?
            """,
            (company_name, limit),
        ).fetchall()
    finally:
        connection.close()

    return [_normalize_row(dict(row)) for row in rows]


def list_recent(limit: int = 200, include_inactive: bool = False) -> list[dict]:
    connection = get_connection()
    try:
        where_sql = "1 = 1" if include_inactive else "COALESCE(is_active, 1) = 1 AND (deadline IS NULL OR date(deadline) >= date('now'))"
        rows = connection.execute(
            f"""
            SELECT
                id, title, company, type, audience_type, student_friendly, experience_min, experience_max,
                deadline, skills, core_skills_json, secondary_skills_json, normalized_skills_json,
                location, cgpa_requirement, backlog_allowed, description_summary,
                url, application_url, source, source_url, quality_score, is_active, last_validated_at, last_updated
            FROM opportunities
            WHERE {where_sql}
            ORDER BY
                CASE WHEN deadline IS NULL THEN 1 ELSE 0 END,
                deadline ASC,
                quality_score DESC,
                id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    finally:
        connection.close()

    return [_normalize_row(dict(row)) for row in rows]
