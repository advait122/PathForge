from backend.roadmap_engine.storage.database import get_connection
from backend.roadmap_engine.utils import parse_skills_field


def list_opportunities(
    *,
    search: str = "",
    opportunity_type: str = "",
    company: str = "",
    deadline_before: str = "",
) -> list[dict]:
    where_clauses = ["1 = 1"]
    parameters: list[str] = []

    if search:
        where_clauses.append("(title LIKE ? OR company LIKE ? OR skills LIKE ?)")
        like_pattern = f"%{search}%"
        parameters.extend([like_pattern, like_pattern, like_pattern])

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
        SELECT id, title, company, type, deadline, skills, url, source, last_updated
        FROM opportunities
        WHERE {where_sql}
        ORDER BY
            CASE WHEN deadline IS NULL THEN 1 ELSE 0 END,
            deadline ASC,
            last_updated DESC
        LIMIT 200
    """

    connection = get_connection()
    try:
        rows = connection.execute(query, parameters).fetchall()
    finally:
        connection.close()

    opportunities: list[dict] = []
    for row in rows:
        row_dict = dict(row)
        row_dict["skills_list"] = parse_skills_field(row_dict.get("skills"))
        opportunities.append(row_dict)

    return opportunities


def get_opportunity(opportunity_id: int) -> dict | None:
    connection = get_connection()
    try:
        row = connection.execute(
            """
            SELECT id, title, company, type, deadline, skills, url, source, last_updated
            FROM opportunities
            WHERE id = ?
            """,
            (opportunity_id,),
        ).fetchone()
    finally:
        connection.close()

    if row is None:
        return None

    opportunity = dict(row)
    opportunity["skills_list"] = parse_skills_field(opportunity.get("skills"))
    return opportunity


def list_filter_options() -> dict:
    connection = get_connection()
    try:
        type_rows = connection.execute(
            """
            SELECT DISTINCT type
            FROM opportunities
            WHERE type IS NOT NULL AND TRIM(type) != ''
            ORDER BY type ASC
            """
        ).fetchall()

        company_rows = connection.execute(
            """
            SELECT DISTINCT company
            FROM opportunities
            WHERE company IS NOT NULL AND TRIM(company) != ''
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
            WHERE company IS NOT NULL AND TRIM(company) != ''
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
            SELECT id, title, company, type, deadline, skills, url, source, last_updated
            FROM opportunities
            WHERE lower(company) = lower(?)
            ORDER BY
                CASE WHEN deadline IS NULL THEN 1 ELSE 0 END,
                deadline ASC,
                id DESC
            LIMIT ?
            """,
            (company_name, limit),
        ).fetchall()
    finally:
        connection.close()

    result: list[dict] = []
    for row in rows:
        row_dict = dict(row)
        row_dict["skills_list"] = parse_skills_field(row_dict.get("skills"))
        result.append(row_dict)
    return result


def list_recent(limit: int = 200) -> list[dict]:
    connection = get_connection()
    try:
        rows = connection.execute(
            """
            SELECT id, title, company, type, deadline, skills, url, source, last_updated
            FROM opportunities
            ORDER BY
                CASE WHEN deadline IS NULL THEN 1 ELSE 0 END,
                deadline ASC,
                id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    finally:
        connection.close()

    result: list[dict] = []
    for row in rows:
        row_dict = dict(row)
        row_dict["skills_list"] = parse_skills_field(row_dict.get("skills"))
        result.append(row_dict)
    return result
