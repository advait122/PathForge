from backend.roadmap_engine.services.agent_models import VerificationResult
from backend.roadmap_engine.services.skill_normalizer import normalize_skill


def run(*, role_intent: dict, planning_result: dict, evidence_summary: dict) -> VerificationResult:
    role_family = str(role_intent.get("normalized_role_family") or "software_engineering")
    target_duration_months = int(role_intent.get("target_duration_months", 6) or 6)
    required_skills = [str(skill) for skill in planning_result.get("required_skills", [])]
    validation_result = planning_result.get("validation_result", {}) or {}

    required_keys = {normalize_skill(skill) for skill in required_skills if normalize_skill(skill)}
    issues: list[str] = []
    notes: list[str] = []

    minimum_skill_count = 3 if target_duration_months <= 2 else 4
    if len(required_skills) < minimum_skill_count:
        issues.append("Roadmap produced too few required skills to be useful.")

    if role_family == "backend":
        if "sql" not in required_keys:
            issues.append("Backend roadmap is missing SQL.")
        if "api" not in required_keys:
            issues.append("Backend roadmap is missing API fundamentals.")
        if not ({"python", "java", "node"} & required_keys):
            issues.append("Backend roadmap should include at least one backend programming stack.")
    elif role_family == "devops":
        if "linux" not in required_keys:
            issues.append("DevOps roadmap is missing Linux fundamentals.")
        if "docker" not in required_keys:
            issues.append("DevOps roadmap is missing Docker.")
        if "ci/cd" not in required_keys:
            issues.append("DevOps roadmap is missing CI/CD.")
        if "cloud" not in required_keys:
            issues.append("DevOps roadmap is missing cloud fundamentals.")
        if not ({"kubernetes", "terraform"} & required_keys):
            issues.append("DevOps roadmap should include Kubernetes or Terraform.")

    overemphasized = [str(item) for item in validation_result.get("overemphasized_topics", []) if str(item).strip()]
    if overemphasized:
        issues.append(f"Verifier found low-confidence roadmap topics: {', '.join(overemphasized[:3])}.")

    if validation_result.get("scope_risks"):
        notes.extend([str(item) for item in validation_result.get("scope_risks", [])])
    if evidence_summary.get("top_skills"):
        top = [str(item.get("skill")) for item in evidence_summary.get("top_skills", [])[:3]]
        notes.append(f"Verifier checked roadmap against top evidence skills: {', '.join(top)}.")

    return VerificationResult(
        passed=len(issues) == 0,
        issues=issues,
        notes=notes,
    )
