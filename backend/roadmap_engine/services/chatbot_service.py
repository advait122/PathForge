import re

from backend.roadmap_engine.storage import chat_repo, goals_repo, playlist_repo, students_repo


def _active_skill(goal_id: int) -> dict | None:
    goal_skills = goals_repo.list_goal_skills(goal_id)
    pending = [item for item in goal_skills if item["status"] != "completed"]
    return pending[0] if pending else None


def _active_chat_context(student_id: int) -> dict:
    student = students_repo.get_student(student_id)
    if student is None:
        raise ValueError("Student not found.")

    goal = goals_repo.get_active_goal(student_id)
    if goal is None:
        raise ValueError("No active goal found.")

    active_skill = _active_skill(goal["id"])
    if active_skill is None:
        return {
            "enabled": False,
            "reason": "All skills are completed. No active playlist chat needed.",
            "goal": goal,
            "active_skill": None,
            "selected_playlist": None,
        }

    selected_playlist = playlist_repo.get_selected_recommendation(goal["id"], active_skill["id"])
    if selected_playlist is None:
        return {
            "enabled": False,
            "reason": f"Select one playlist for {active_skill['skill_name']} to enable chatbot.",
            "goal": goal,
            "active_skill": active_skill,
            "selected_playlist": None,
        }

    return {
        "enabled": True,
        "reason": "",
        "goal": goal,
        "active_skill": active_skill,
        "selected_playlist": selected_playlist,
    }


def _playlist_prompt_payload(selected_playlist: dict) -> tuple[dict, dict]:
    summary = selected_playlist.get("summary", {}) or {}
    top_titles = summary.get("top_video_titles", [])
    if not isinstance(top_titles, list):
        top_titles = []

    playlist_payload = {
        "title": selected_playlist.get("title", ""),
        "channel_title": selected_playlist.get("channel_title", ""),
        "description": summary.get("topic_overview", ""),
        "top_video_titles": [str(item) for item in top_titles[:8]],
    }
    summary_payload = {
        "topic_overview": summary.get("topic_overview", ""),
        "learning_experience": summary.get("learning_experience", ""),
        "topics_covered_summary": summary.get("topics_covered_summary", ""),
    }
    return playlist_payload, summary_payload


def _fallback_answer(selected_playlist: dict, question: str) -> str:
    summary = selected_playlist.get("summary", {}) or {}
    topic_overview = str(summary.get("topic_overview", "")).strip()
    covered = str(summary.get("topics_covered_summary", "")).strip()
    if topic_overview or covered:
        focus = topic_overview if topic_overview else covered
        return (
            "Chatbot is temporarily unavailable. "
            f"For now, focus on this playlist area: {focus}"
        )
    return (
        "Chatbot is temporarily unavailable. "
        f"Please continue your current playlist and retry your question: {question}"
    )


def _structure_assistant_answer(answer: str) -> str:
    text = str(answer or "").replace("\r", "").strip()
    if not text:
        return ""

    lines = [line.strip() for line in text.split("\n") if line.strip()]
    if not lines:
        return ""

    bullet_pattern = re.compile(r"^([-*•]|\d+[.)])\s+")
    has_bullets = any(bullet_pattern.match(line) for line in lines)

    if has_bullets:
        normalized: list[str] = []
        for line in lines:
            if bullet_pattern.match(line):
                normalized.append("- " + bullet_pattern.sub("", line).strip())
            else:
                normalized.append(line)
        return "\n".join(normalized)

    compact = " ".join(lines)

    sentence_parts = [
        part.strip()
        for part in re.split(r"(?<=[.!?])\s+", compact)
        if part.strip()
    ]
    if len(sentence_parts) >= 2:
        return "\n".join(f"- {part}" for part in sentence_parts)

    clause_parts = [
        part.strip()
        for part in re.split(r"[;]\s*", compact)
        if part.strip()
    ]
    if len(clause_parts) >= 2:
        return "\n".join(f"- {part}" for part in clause_parts)

    return compact


def get_chat_panel(student_id: int, limit: int = 20) -> dict:
    context = _active_chat_context(student_id)
    if not context["enabled"]:
        return {
            "enabled": False,
            "reason": context["reason"],
            "active_skill": context["active_skill"],
            "selected_playlist": context["selected_playlist"],
            "messages": [],
        }

    goal = context["goal"]
    active_skill = context["active_skill"]
    selected_playlist = context["selected_playlist"]
    session = chat_repo.get_session(
        student_id=student_id,
        goal_id=goal["id"],
        goal_skill_id=active_skill["id"],
        playlist_recommendation_id=selected_playlist["id"],
    )
    messages = chat_repo.list_messages(session["id"], limit=limit) if session else []
    return {
        "enabled": True,
        "reason": "",
        "active_skill": active_skill,
        "selected_playlist": selected_playlist,
        "messages": messages,
    }


def ask_question(student_id: int, question: str) -> dict:
    clean_question = str(question or "").strip()
    if not clean_question:
        raise ValueError("Please enter a question for the chatbot.")
    if len(clean_question) > 1000:
        raise ValueError("Question is too long. Keep it under 1000 characters.")

    context = _active_chat_context(student_id)
    if not context["enabled"]:
        raise ValueError(context["reason"])

    goal = context["goal"]
    active_skill = context["active_skill"]
    selected_playlist = context["selected_playlist"]

    session_id = chat_repo.get_or_create_session_id(
        student_id=student_id,
        goal_id=goal["id"],
        goal_skill_id=active_skill["id"],
        playlist_recommendation_id=selected_playlist["id"],
    )

    previous = chat_repo.list_messages(session_id, limit=12)
    history = [
        {"role": item["role"], "content": item["message_text"]}
        for item in previous
        if item["role"] in {"user", "assistant"}
    ]

    chat_repo.add_message(session_id, "user", clean_question)
    answer = ""
    try:
        from backend.youtube_module.llm_explainer.qna import answer_playlist_question_with_history

        playlist_payload, summary_payload = _playlist_prompt_payload(selected_playlist)
        answer = answer_playlist_question_with_history(
            playlist=playlist_payload,
            playlist_summary=summary_payload,
            student_question=clean_question,
            conversation_history=history,
        )
    except Exception:
        answer = _fallback_answer(selected_playlist, clean_question)

    if not answer:
        answer = _fallback_answer(selected_playlist, clean_question)

    answer = _structure_assistant_answer(answer)

    chat_repo.add_message(session_id, "assistant", answer)
    updated_messages = chat_repo.list_messages(session_id, limit=20)
    return {
        "active_skill": active_skill,
        "selected_playlist": selected_playlist,
        "messages": updated_messages,
        "answer": answer,
    }
