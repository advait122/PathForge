import json
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from backend.roadmap_engine.constants import PASS_PERCENT_FOR_SKILL_TEST
from backend.roadmap_engine.services.skill_normalizer import normalize_skill
from backend.roadmap_engine.storage import (
    assessment_repo,
    goals_repo,
    matching_repo,
    roadmap_repo,
    students_repo,
)
from backend.roadmap_engine.utils import utc_now_iso, utc_today


GROQ_MODEL = "llama-3.1-8b-instant"
TEST_QUESTION_COUNT = 10
TEST_DURATION_MINUTES = 30


def _extract_json(raw_text: str) -> dict | None:
    start = raw_text.find("{")
    end = raw_text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    try:
        return json.loads(raw_text[start : end + 1])
    except json.JSONDecodeError:
        return None


def _parse_assessment_created_at(created_at: str | None) -> datetime | None:
    if not created_at:
        return None
    text = str(created_at).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _assessment_deadline_utc(assessment: dict) -> datetime | None:
    created = _parse_assessment_created_at(assessment.get("created_at"))
    if created is None:
        return None
    return created + timedelta(minutes=TEST_DURATION_MINUTES)


def assessment_deadline_iso(assessment: dict) -> str | None:
    deadline = _assessment_deadline_utc(assessment)
    if deadline is None:
        return None
    return deadline.isoformat()


def _fallback_questions(skill_name: str) -> tuple[list[dict], list[int]]:
    questions = [
        {
            "topic": "Purpose",
            "difficulty": "basic",
            "question": f"Which statement best describes the purpose of {skill_name}?",
            "options": [
                f"It is used to solve practical software problems using {skill_name}.",
                "It is mainly for non-technical tasks unrelated to software.",
                "It cannot be used in real projects.",
                "It is only useful for hardware manufacturing.",
            ],
        },
        {
            "topic": "Core Concepts",
            "difficulty": "basic",
            "question": f"When starting {skill_name}, what should be learned first?",
            "options": [
                "Core concepts and fundamentals.",
                "Only advanced edge cases.",
                "Interview answers without understanding.",
                "Tool shortcuts without concepts.",
            ],
        },
        {
            "topic": "Workflow",
            "difficulty": "basic",
            "question": f"What is a strong learning workflow for {skill_name}?",
            "options": [
                "Learn concept -> see example -> practice independently.",
                "Memorize solutions and skip practice.",
                "Watch random videos without continuity.",
                "Start with highly advanced topics only.",
            ],
        },
        {
            "topic": "Validation",
            "difficulty": "basic",
            "question": f"How can a student validate progress in {skill_name}?",
            "options": [
                "By solving tasks and explaining why the solution works.",
                "By only watching videos and avoiding exercises.",
                "By skipping revision and tests.",
                "By copying answers without checking logic.",
            ],
        },
        {
            "topic": "Troubleshooting",
            "difficulty": "basic",
            "question": f"What should a student do when stuck on a {skill_name} problem?",
            "options": [
                "Break the problem, review basics, and retry with smaller steps.",
                "Ignore the problem and move to unrelated topics.",
                "Memorize one solution and never revisit.",
                "Stop practicing until the next test.",
            ],
        },
        {
            "topic": "Practice Strategy",
            "difficulty": "basic",
            "question": f"What is the best long-term way to improve {skill_name}?",
            "options": [
                "Consistent practice with increasing difficulty.",
                "One-time practice right before tests.",
                "Only reading theory once.",
                "Avoiding feedback and corrections.",
            ],
        },
        {
            "topic": "Real-World Use",
            "difficulty": "basic",
            "question": f"How is {skill_name} most commonly used in career preparation?",
            "options": [
                "Applying concepts in projects, assignments, and interviews.",
                "Keeping it separate from practical work.",
                "Using it only for non-technical communication.",
                "Avoiding it in problem-solving contexts.",
            ],
        },
        {
            "topic": "Revision",
            "difficulty": "basic",
            "question": f"What is the role of revision in mastering {skill_name}?",
            "options": [
                "It strengthens retention and fixes weak areas.",
                "It is unnecessary if one playlist was watched once.",
                "It reduces practical ability.",
                "It should replace all problem-solving.",
            ],
        },
        {
            "topic": "Application Depth",
            "difficulty": "medium",
            "question": f"Which outcome best indicates medium-level understanding of {skill_name}?",
            "options": [
                "You can adapt concepts to solve new but related problems.",
                "You can only repeat one memorized example.",
                "You avoid unfamiliar variations completely.",
                "You rely only on copied templates.",
            ],
        },
        {
            "topic": "Improvement Planning",
            "difficulty": "medium",
            "question": f"After receiving weak-topic feedback in {skill_name}, what is the best next step?",
            "options": [
                "Create a targeted revision plan and retest after practice.",
                "Skip weak topics and move to a different skill immediately.",
                "Retake the test without any revision.",
                "Stop using feedback in future learning.",
            ],
        },
    ]
    answer_key = [0] * len(questions)
    return questions, answer_key


def _context_aware_fallback_questions(skill_name: str, selected_playlist: dict) -> tuple[list[dict], list[int]]:
    summary = selected_playlist.get("summary", {}) or {}
    playlist_title = str(selected_playlist.get("title", "")).strip()
    channel_title = str(selected_playlist.get("channel_title", "")).strip()
    topic_overview = str(summary.get("topic_overview", "")).strip()
    learning_experience = str(summary.get("learning_experience", "")).strip()
    topics_covered = str(summary.get("topics_covered_summary", "")).strip()
    channel_url = str(summary.get("channel_url", "")).strip()
    playlist_url = str(selected_playlist.get("playlist_url", "")).strip()

    overview_option = (
        topic_overview[:120] + ("..." if len(topic_overview) > 120 else "")
        if topic_overview
        else f"Building practical understanding of {skill_name}"
    )
    learning_option = (
        learning_experience[:120] + ("..." if len(learning_experience) > 120 else "")
        if learning_experience
        else "Concept-to-practice progression with structured learning"
    )
    covered_option = (
        topics_covered[:120] + ("..." if len(topics_covered) > 120 else "")
        if topics_covered
        else f"Key concepts and practical applications of {skill_name}"
    )

    questions = [
        {
            "topic": "Topic Overview",
            "difficulty": "basic",
            "question": (
                f"Based on playlist '{playlist_title or skill_name}' for {skill_name}, "
                "what is the core focus area?"
            ),
            "options": [
                overview_option,
                "Unrelated hardware-only workflows",
                "Random topics unrelated to career goals",
                "Non-technical communication-only training",
            ],
        },
        {
            "topic": "Learning Experience",
            "difficulty": "basic",
            "question": (
                f"What learning flow should the student expect from channel "
                f"'{channel_title or 'selected channel'}'?"
            ),
            "options": [
                learning_option,
                "No progression or sequence at all",
                "Only motivational content with no technical depth",
                "Only one-shot advanced topics with no basics",
            ],
        },
        {
            "topic": "Playlist Source",
            "difficulty": "basic",
            "question": "Which link corresponds to the selected channel source?",
            "options": [
                channel_url if channel_url else f"Channel: {selected_playlist.get('channel_title', '')}",
                "https://example.com/random-resource",
                "https://docs.python.org/",
                "https://github.com/",
            ],
        },
        {
            "topic": "Topics Covered",
            "difficulty": "basic",
            "question": "Which option best matches the concepts covered summary?",
            "options": [
                covered_option,
                "Only interview HR behavior questions",
                "Only non-programming tool tutorials",
                "No conceptual coverage mentioned",
            ],
        },
        {
            "topic": "Concept Retention",
            "difficulty": "basic",
            "question": f"What is the best way to retain what was learned in this {skill_name} playlist?",
            "options": [
                "Review key ideas and practice related tasks soon after each session.",
                "Rely only on one-time passive watching.",
                "Skip all notes and revision.",
                "Ignore unclear parts and continue without revisiting.",
            ],
        },
        {
            "topic": "Practical Practice",
            "difficulty": "basic",
            "question": f"How should a student practice {skill_name} after this playlist?",
            "options": [
                "Solve basic-to-intermediate tasks tied to playlist concepts.",
                "Avoid all practice and focus only on theory.",
                "Jump to unrelated topics immediately.",
                "Use only copied answers without understanding.",
            ],
        },
        {
            "topic": "Error Correction",
            "difficulty": "basic",
            "question": f"When a student makes mistakes while practicing {skill_name}, what should they do?",
            "options": [
                "Identify the weak concept, revise it, then retry similar problems.",
                "Ignore the mistake and continue.",
                "Memorize one final answer and stop analyzing.",
                "Switch to an unrelated playlist immediately.",
            ],
        },
        {
            "topic": "Progress Check",
            "difficulty": "basic",
            "question": f"Which behavior best shows basic understanding gained from this {skill_name} playlist?",
            "options": [
                "Explaining key concepts and applying them in small tasks.",
                "Replaying videos without solving any task.",
                "Skipping all self-checks and quizzes.",
                "Avoiding concept explanation entirely.",
            ],
        },
        {
            "topic": "Application Depth",
            "difficulty": "medium",
            "question": f"After completing the playlist ({playlist_url if playlist_url else 'selected URL'}), what indicates medium-level understanding?",
            "options": [
                "Applying concepts to new but related problems with clear reasoning.",
                "Solving only examples identical to the video.",
                "Depending entirely on copied code.",
                "Avoiding any variation in question style.",
            ],
        },
        {
            "topic": "Knowledge Integration",
            "difficulty": "medium",
            "question": f"How should {skill_name} knowledge from this playlist be integrated into the roadmap?",
            "options": [
                "Use learned concepts in roadmap tasks, revision, and test feedback loops.",
                "Keep playlist learning separate from roadmap work.",
                "Skip revision tasks after test feedback.",
                "Ignore weak topics and move to next skills directly.",
            ],
        },
    ]
    answer_key = [0] * len(questions)
    return questions, answer_key


def _llm_questions(skill_name: str, selected_playlist: dict) -> tuple[list[dict], list[int]] | None:
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        return None

    try:
        from groq import Groq

        client = Groq(api_key=api_key)
        summary = selected_playlist.get("summary", {}) or {}
        channel_url = summary.get("channel_url", "")
        prompt = (
            f"Generate exactly {TEST_QUESTION_COUNT} MCQ questions for a post-playlist test on {skill_name}. "
            "Use only the selected playlist context below.\n"
            "Focus on major and important topics covered in the playlist.\n"
            "Goal: check how much basic understanding the student gained from the playlist.\n"
            "Difficulty mix requirements:\n"
            "- Most questions should be 'basic' and concept-checking.\n"
            "- Include 1 to 2 questions with 'medium' difficulty and knowledge-based application.\n"
            "Avoid trivia and avoid topics outside the playlist scope.\n"
            "Return strict JSON with this structure:\n"
            "{ \"questions\": [ { \"topic\": str, \"difficulty\": \"basic\"|\"medium\", "
            "\"question\": str, \"options\": [str, str, str, str], \"correct_option_index\": 0-3 } ] }\n"
            "Keep topics short and practical.\n\n"
            f"Playlist title: {selected_playlist.get('title', '')}\n"
            f"Channel: {selected_playlist.get('channel_title', '')}\n"
            f"Channel URL: {channel_url}\n"
            f"Playlist URL: {selected_playlist.get('playlist_url', '')}\n"
            f"Topic overview: {summary.get('topic_overview', '')}\n"
            f"Learning experience: {summary.get('learning_experience', '')}\n"
            f"Topics covered summary: {summary.get('topics_covered_summary', '')}\n"
        )
        response = client.chat.completions.create(
            model=GROQ_MODEL,
            temperature=0.3,
            messages=[
                {
                    "role": "system",
                    "content": "You generate practical MCQ tests in strict JSON.",
                },
                {"role": "user", "content": prompt},
            ],
        )
        parsed = _extract_json(response.choices[0].message.content or "")
        if not parsed:
            return None

        raw_questions = parsed.get("questions", [])
        if not isinstance(raw_questions, list) or len(raw_questions) == 0:
            return None

        questions = []
        answer_key = []
        for item in raw_questions[: max(TEST_QUESTION_COUNT, 15)]:
            topic = str(item.get("topic", "")).strip() or "General"
            difficulty = str(item.get("difficulty", "basic")).strip().lower()
            if difficulty not in {"basic", "medium"}:
                difficulty = "basic"
            question = str(item.get("question", "")).strip()
            options = item.get("options", [])
            answer = item.get("correct_option_index")
            if not question or not isinstance(options, list) or len(options) != 4:
                continue
            try:
                answer_int = int(answer)
            except (TypeError, ValueError):
                continue
            if answer_int < 0 or answer_int > 3:
                continue
            questions.append(
                {
                    "topic": topic,
                    "difficulty": difficulty,
                    "question": question,
                    "options": [str(opt) for opt in options],
                }
            )
            answer_key.append(answer_int)

        if len(questions) < TEST_QUESTION_COUNT:
            return None

        questions = questions[:TEST_QUESTION_COUNT]
        answer_key = answer_key[:TEST_QUESTION_COUNT]
        medium_count = sum(1 for item in questions if item.get("difficulty") == "medium")
        if medium_count == 0:
            return None

        return questions, answer_key
    except Exception:
        return None


def _skill_is_ready_for_test(goal_id: int, goal_skill_id: int) -> bool:
    plan = roadmap_repo.get_active_plan(goal_id)
    if not plan:
        return False

    tasks = roadmap_repo.list_tasks_for_skill(plan["id"], goal_skill_id)
    if not tasks:
        return False

    return all(task["is_completed"] == 1 for task in tasks)


def _topic_breakdown(questions: list[dict], answer_key: list[int], answers: list[int]) -> dict[str, dict]:
    summary: dict[str, dict] = defaultdict(lambda: {"correct": 0, "total": 0})
    for idx, expected in enumerate(answer_key):
        topic = str(questions[idx].get("topic", "General")) if idx < len(questions) else "General"
        if not topic.strip():
            topic = "General"
        summary[topic]["total"] += 1
        if idx < len(answers) and answers[idx] == expected:
            summary[topic]["correct"] += 1
    return dict(summary)


def _weak_and_strong_topics(topic_stats: dict[str, dict]) -> tuple[list[str], list[str]]:
    weak: list[str] = []
    strong: list[str] = []
    for topic, values in topic_stats.items():
        total = max(int(values["total"]), 1)
        ratio = values["correct"] / total
        if ratio < 0.5:
            weak.append(topic)
        elif ratio >= 0.8:
            strong.append(topic)
    return weak, strong


def _build_feedback(score_percent: float, passed: bool, weak_topics: list[str], strong_topics: list[str]) -> str:
    weak_text = ", ".join(weak_topics) if weak_topics else "None"
    strong_text = ", ".join(strong_topics) if strong_topics else "None"
    status = "Passed" if passed else "Failed"
    action = "Move to next skill." if passed else "Revision tasks added. Complete them and retake."
    return (
        f"Score: {score_percent:.1f}% ({status}). "
        f"Weak topics: {weak_text}. "
        f"Strong topics: {strong_text}. "
        f"{action}"
    )


def _insert_revision_tasks(goal_id: int, goal_skill: dict, weak_topics: list[str]) -> int:
    if not weak_topics:
        weak_topics = ["Core Concepts"]

    plan = roadmap_repo.get_active_plan(goal_id)
    if not plan:
        return 0

    today = utc_today()
    existing = roadmap_repo.list_tasks(plan["id"], today.isoformat(), None)
    existing_titles = {
        task["title"]
        for task in existing
        if task.get("goal_skill_id") == goal_skill["id"] and task["is_completed"] == 0
    }

    tasks_to_add = []
    for idx, topic in enumerate(weak_topics[:3]):
        title = f"Revision: {goal_skill['skill_name']} - {topic}"
        if title in existing_titles:
            continue
        task_date = (today + timedelta(days=idx + 1)).isoformat()
        tasks_to_add.append(
            {
                "goal_skill_id": goal_skill["id"],
                "task_date": task_date,
                "title": title,
                "description": (
                    f"Revise topic '{topic}' for {goal_skill['skill_name']}, "
                    "practice questions, then retake the skill test."
                ),
                "target_minutes": 45,
            }
        )

    roadmap_repo.append_tasks(plan["id"], tasks_to_add)
    return len(tasks_to_add)


def generate_assessment(student_id: int, goal_skill_id: int) -> dict:
    goal = goals_repo.get_active_goal(student_id)
    if goal is None:
        raise ValueError("Active goal not found.")

    goal_skill = goals_repo.get_goal_skill(goal_skill_id)
    if goal_skill is None or goal_skill["goal_id"] != goal["id"]:
        raise ValueError("Skill not found for current goal.")

    goal_skills = goals_repo.list_goal_skills(goal["id"])
    active_skill = next((item for item in goal_skills if item["status"] != "completed"), None)
    if active_skill is None:
        raise ValueError("All skills are already completed.")
    if active_skill["id"] != goal_skill_id:
        raise ValueError(
            f"Complete and pass {active_skill['skill_name']} before unlocking this skill test."
        )

    from backend.roadmap_engine.services import youtube_learning_service

    selected_playlist = youtube_learning_service.get_selected_playlist(goal["id"], goal_skill_id)
    if selected_playlist is None:
        raise ValueError(
            f"Select one of the top 3 playlists for {goal_skill['skill_name']} before taking the test."
        )

    if goal_skill["status"] == "completed":
        raise ValueError("Skill already completed.")

    if not _skill_is_ready_for_test(goal["id"], goal_skill_id):
        raise ValueError("Complete all roadmap tasks for this skill before taking the test.")

    latest = assessment_repo.get_latest_assessment(goal_skill_id)
    if latest:
        # Reuse an existing in-progress attempt to avoid creating hidden attempts
        # whenever the test page is refreshed/reopened.
        if latest.get("submitted_at") is None:
            latest_deadline = _assessment_deadline_utc(latest)
            now_utc = datetime.now(tz=timezone.utc)
            if latest_deadline is None or now_utc <= (latest_deadline + timedelta(seconds=90)):
                return latest
        # After pass, keep returning the passed record.
        if latest.get("passed") == 1:
            return latest

    generated = _llm_questions(goal_skill["skill_name"], selected_playlist)
    if generated:
        questions, answer_key = generated
    else:
        questions, answer_key = _context_aware_fallback_questions(
            goal_skill["skill_name"],
            selected_playlist,
        )
    assessment_id = assessment_repo.create_assessment(
        goal_id=goal["id"],
        goal_skill_id=goal_skill_id,
        questions=questions,
        answer_key=answer_key,
    )
    assessment = assessment_repo.get_assessment(assessment_id)
    if assessment is None:
        raise ValueError("Failed to create assessment.")
    return assessment


def submit_assessment(student_id: int, assessment_id: int, answers: list[int]) -> dict:
    goal = goals_repo.get_active_goal(student_id)
    if goal is None:
        raise ValueError("Active goal not found.")

    assessment = assessment_repo.get_assessment(assessment_id)
    if assessment is None:
        raise ValueError("Assessment not found.")
    if assessment["goal_id"] != goal["id"]:
        raise ValueError("Assessment does not belong to your current goal.")
    if assessment.get("submitted_at"):
        return assessment

    deadline_utc = _assessment_deadline_utc(assessment)
    if deadline_utc is not None:
        now_utc = datetime.now(tz=timezone.utc)
        if now_utc > (deadline_utc + timedelta(seconds=90)):
            raise ValueError("Time is up for this test. Please retake the skill test.")

    answer_key = assessment["answer_key"]
    if len(answers) != len(answer_key):
        raise ValueError("Please answer all questions.")

    total = len(answer_key)
    correct = sum(1 for idx, answer in enumerate(answers) if answer == answer_key[idx])
    score_percent = (correct / total) * 100 if total else 0.0
    passed = score_percent >= PASS_PERCENT_FOR_SKILL_TEST
    topic_stats = _topic_breakdown(assessment["questions"], answer_key, answers)
    weak_topics, strong_topics = _weak_and_strong_topics(topic_stats)

    feedback = _build_feedback(score_percent, passed, weak_topics, strong_topics)
    assessment_repo.submit_assessment(
        assessment_id=assessment_id,
        student_answers=answers,
        score_percent=score_percent,
        passed=passed,
        feedback_text=feedback,
    )

    if passed:
        goal_skill = goals_repo.get_goal_skill(assessment["goal_skill_id"])
        if goal_skill:
            completed_at = utc_now_iso()
            goals_repo.set_goal_skill_status(goal_skill["id"], "completed", completed_at)
            students_repo.add_student_skill(
                student_id=student_id,
                skill_name=goal_skill["skill_name"],
                normalized_skill=normalize_skill(goal_skill["skill_name"]),
                skill_source="roadmap_mastered",
            )
            matching_repo.create_notification(
                student_id=student_id,
                goal_id=goal["id"],
                notification_type="skill_test_passed",
                title="Skill Test Passed",
                body=(
                    f"You passed {goal_skill['skill_name']} ({score_percent:.1f}%). "
                    "Skill marked as completed."
                ),
            )
    else:
        goals_repo.set_goal_skill_status(assessment["goal_skill_id"], "in_progress", None)
        goal_skill = goals_repo.get_goal_skill(assessment["goal_skill_id"])
        added = 0
        if goal_skill:
            added = _insert_revision_tasks(goal["id"], goal_skill, weak_topics)
        matching_repo.create_notification(
            student_id=student_id,
            goal_id=goal["id"],
            notification_type="skill_test_failed",
            title="Skill Test Failed",
            body=(
                f"You scored {score_percent:.1f}%. Weak topics: "
                f"{', '.join(weak_topics) if weak_topics else 'General'}. "
                f"{added} revision task(s) added."
            ),
        )

    updated = assessment_repo.get_assessment(assessment_id)
    if updated is None:
        raise ValueError("Failed to load updated assessment.")
    return updated
