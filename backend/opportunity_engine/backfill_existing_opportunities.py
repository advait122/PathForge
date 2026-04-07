import json

from backend.roadmap_engine.services.opportunity_agent_service import extract_and_validate_opportunity
from backend.roadmap_engine.storage import students_repo
from backend.roadmap_engine.storage.database import get_connection, transaction
from backend.roadmap_engine.utils import parse_skills_field, utc_now_iso
from backend.roadmap_engine.services import matching_service


def _build_clean_text(row: dict) -> str:
    parts = [
        str(row.get("title") or "").strip(),
        str(row.get("company") or "").strip(),
        str(row.get("type") or "").strip(),
        str(row.get("description_summary") or "").strip(),
    ]
    skills = parse_skills_field(row.get("skills"))
    if skills:
        parts.append("Skills: " + ", ".join(skills))
    deadline = str(row.get("deadline") or "").strip()
    if deadline:
        parts.append(f"Apply before {deadline}")
    return ". ".join(part for part in parts if part)


def _list_existing_opportunities() -> list[dict]:
    connection = get_connection()
    try:
        rows = connection.execute(
            """
            SELECT
                id, title, company, type, deadline, skills, description_summary,
                url, source, content_hash
            FROM opportunities
            ORDER BY id ASC
            """
        ).fetchall()
    finally:
        connection.close()
    return [dict(row) for row in rows]


def _update_row(row_id: int, prepared: dict | None) -> None:
    now = utc_now_iso()
    with transaction() as connection:
        if prepared is None:
            connection.execute(
                """
                UPDATE opportunities
                SET is_active = 0,
                    student_friendly = 0,
                    audience_type = COALESCE(audience_type, 'unknown'),
                    last_validated_at = ?,
                    last_updated = ?
                WHERE id = ?
                """,
                (now, now, row_id),
            )
            return

        connection.execute(
            """
            UPDATE opportunities
            SET
                title = ?,
                company = ?,
                type = ?,
                audience_type = ?,
                student_friendly = ?,
                experience_min = ?,
                experience_max = ?,
                deadline = ?,
                skills = ?,
                core_skills_json = ?,
                secondary_skills_json = ?,
                normalized_skills_json = ?,
                location = ?,
                cgpa_requirement = ?,
                backlog_allowed = ?,
                description_summary = ?,
                application_url = ?,
                source = ?,
                source_url = ?,
                content_hash = ?,
                quality_score = ?,
                is_active = ?,
                agent_trace_json = ?,
                fetched_at = COALESCE(fetched_at, ?),
                last_validated_at = ?,
                last_updated = ?
            WHERE id = ?
            """,
            (
                prepared["title"],
                prepared["company"],
                prepared["type"],
                prepared["audience_type"],
                int(prepared["student_friendly"]),
                int(prepared["experience_min"]),
                int(prepared["experience_max"]),
                prepared.get("deadline"),
                str(prepared.get("skills", [])),
                json.dumps(prepared.get("core_skills", []), ensure_ascii=False),
                json.dumps(prepared.get("secondary_skills", []), ensure_ascii=False),
                json.dumps(prepared.get("normalized_skills", []), ensure_ascii=False),
                prepared.get("location"),
                prepared.get("cgpa_requirement"),
                prepared.get("backlog_allowed"),
                prepared.get("description_summary"),
                prepared.get("application_url"),
                prepared.get("source"),
                prepared.get("source_url"),
                prepared.get("content_hash"),
                float(prepared.get("quality_score") or 0.0),
                int(prepared.get("is_active", 1)),
                prepared.get("agent_trace_json"),
                prepared.get("fetched_at") or now,
                prepared.get("last_validated_at") or now,
                now,
                row_id,
            ),
        )


def backfill_existing_opportunities() -> dict:
    rows = _list_existing_opportunities()
    kept = 0
    deactivated = 0

    for row in rows:
        clean_text = _build_clean_text(row)
        prepared = extract_and_validate_opportunity(
            clean_text=clean_text,
            extracted_seed={
                "title": row.get("title"),
                "company": row.get("company"),
                "type": row.get("type"),
                "deadline": row.get("deadline"),
                "skills": parse_skills_field(row.get("skills")),
                "description_summary": row.get("description_summary"),
                "source_name": row.get("source"),
            },
            source_name=str(row.get("source") or "existing_db"),
            source=str(row.get("source") or "existing_db"),
            url=str(row.get("url") or ""),
            content_hash=str(row.get("content_hash") or ""),
        )
        _update_row(int(row["id"]), prepared)
        if prepared is None:
            deactivated += 1
        else:
            kept += 1

    for student in students_repo.list_students():
        try:
            matching_service.refresh_opportunity_matches(int(student["id"]))
        except Exception:
            continue

    return {
        "processed": len(rows),
        "kept_active": kept,
        "deactivated": deactivated,
    }


def main() -> None:
    summary = backfill_existing_opportunities()
    print(summary)


if __name__ == "__main__":
    main()
