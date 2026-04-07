from backend.roadmap_engine.services import (
    extraction_agent,
    planning_agent,
    research_agent,
    role_intent_agent,
    verifier_agent,
)
from backend.roadmap_engine.services.agent_models import AgentRunTrace
from backend.roadmap_engine.services.agent_models import PlanningResult, VerificationResult
from backend.roadmap_engine.services.crewai_roadmap_service import generate_crewai_verified_roadmap
from backend.roadmap_engine.storage import agent_trace_repo


def _apply_verifier_corrections(role_intent: dict, planning_result: PlanningResult, verification_result: VerificationResult) -> PlanningResult:
    required_skills = list(planning_result.required_skills)
    required_keys = {str(skill).strip().lower() for skill in required_skills}
    role_family = str(role_intent.get("normalized_role_family") or "software_engineering")

    if role_family == "backend":
        if any("missing API fundamentals" in issue for issue in verification_result.issues) and "api" not in required_keys:
            required_skills.insert(3, "API")
        if any("missing SQL" in issue for issue in verification_result.issues) and "sql" not in required_keys:
            required_skills.insert(2, "SQL")
    elif role_family == "devops":
        corrections = [
            ("Linux fundamentals", "Linux", 0),
            ("Docker", "Docker", 2),
            ("CI/CD", "CI/CD", 4),
            ("cloud fundamentals", "Cloud", 5),
        ]
        for issue_text, skill_label, position in corrections:
            if any(issue_text in issue for issue in verification_result.issues) and skill_label.lower() not in required_keys:
                required_skills.insert(min(position, len(required_skills)), skill_label)
                required_keys.add(skill_label.lower())
        if any("Kubernetes or Terraform" in issue for issue in verification_result.issues):
            if "kubernetes" not in required_keys:
                required_skills.insert(min(6, len(required_skills)), "Kubernetes")
                required_keys.add("kubernetes")
            if "terraform" not in required_keys:
                required_skills.insert(min(7, len(required_skills)), "Terraform")
                required_keys.add("terraform")

    validation_result = dict(planning_result.validation_result)
    validation_result["required_skills"] = required_skills
    return PlanningResult(
        required_skills=required_skills,
        validation_result=validation_result,
        draft_requirements=planning_result.draft_requirements,
        rationale=planning_result.rationale,
        llm_plan=planning_result.llm_plan,
    )


def generate_verified_roadmap(
    *,
    goal_text: str,
    target_duration_months: int,
    known_skills: list[str] | None = None,
    weekly_study_hours: int | None = None,
    student_id: int | None = None,
) -> dict:
    known_skills = known_skills or []

    try:
        crew_result = generate_crewai_verified_roadmap(
            goal_text=goal_text,
            target_duration_months=target_duration_months,
            known_skills=known_skills,
            weekly_study_hours=weekly_study_hours,
        )
        trace_id = agent_trace_repo.create_agent_run(
            student_id=student_id,
            goal_text=goal_text,
            status="passed" if crew_result.get("verification_result", {}).get("passed") else "flagged",
            trace=crew_result.get("agent_trace", {}),
        )
        crew_result["agent_trace_id"] = trace_id
        return crew_result
    except Exception:
        pass

    role_intent_result = role_intent_agent.run(
        goal_text=goal_text,
        target_duration_months=target_duration_months,
    )
    research_result = research_agent.run(role_intent=role_intent_result.role_intent)
    extraction_result = extraction_agent.run(evidence_records=research_result.evidence_records)
    planning_result = planning_agent.run(
        goal_text=goal_text,
        role_intent=role_intent_result.role_intent,
        evidence_summary=extraction_result.evidence_summary,
        known_skills=known_skills,
    )
    verification_result = verifier_agent.run(
        role_intent=role_intent_result.role_intent,
        planning_result=planning_result.model_dump(),
        evidence_summary=extraction_result.evidence_summary,
    )
    if not verification_result.passed:
        planning_result = _apply_verifier_corrections(
            role_intent_result.role_intent,
            planning_result,
            verification_result,
        )
        verification_result = verifier_agent.run(
            role_intent=role_intent_result.role_intent,
            planning_result=planning_result.model_dump(),
            evidence_summary=extraction_result.evidence_summary,
        )

    trace = AgentRunTrace(
        role_intent=role_intent_result.model_dump(),
        research=research_result.model_dump(),
        extraction=extraction_result.model_dump(),
        planning=planning_result.model_dump(),
        verification=verification_result.model_dump(),
    )
    trace_id = agent_trace_repo.create_agent_run(
        student_id=student_id,
        goal_text=goal_text,
        status="passed" if verification_result.passed else "flagged",
        trace=trace.model_dump(),
    )

    return {
        "goal_parse": role_intent_result.goal_parse,
        "role_intent": role_intent_result.role_intent,
        "required_skills": planning_result.required_skills,
        "source": "agent_orchestrated_roadmap_v1",
        "source_opportunity_count": extraction_result.evidence_summary.get("sample_size", 0),
        "rationale": planning_result.rationale,
        "evidence_summary": extraction_result.evidence_summary,
        "evidence_highlights": list(planning_result.validation_result.get("notes", [])),
        "validation_result": planning_result.validation_result,
        "draft_requirements": planning_result.draft_requirements,
        "verification_result": verification_result.model_dump(),
        "agent_trace_id": trace_id,
        "planner_mode": planning_result.validation_result.get("planner_mode", "fallback"),
    }
