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

    raw_lines = [line.strip() for line in text.split("\n")]
    if not any(raw_lines):
        return ""

    lines: list[str] = []
    for line in raw_lines:
        if line:
            lines.append(line)
            continue
        if lines and lines[-1] != "":
            lines.append("")

    while lines and lines[0] == "":
        lines.pop(0)
    while lines and lines[-1] == "":
        lines.pop()
    if not lines:
        return ""
    bullet_pattern = re.compile(r"^(?:[-*]|\u2022)\s+(.+)$")
    numbered_pattern = re.compile(r"^\d+[.)]\s+(.+)$")
    labeled_pattern = re.compile(
        r"^(Title|Explanation|Overview|Quick Recap|Key Points?|Example[s]?|What You Can Ask Next)\s*:\s*(.*)$",
        re.IGNORECASE,
    )

    normalized: list[str] = []
    list_index = 0
    for line in lines:
        if not line:
            if normalized and normalized[-1] != "":
                normalized.append("")
            list_index = 0
            continue

        bullet_match = bullet_pattern.match(line)
        numbered_match = numbered_pattern.match(line)
        if bullet_match or numbered_match:
            list_index += 1
            content = bullet_match.group(1).strip() if bullet_match else numbered_match.group(1).strip()
            normalized.append(f"{list_index}. {content}")
            continue

        list_index = 0
        normalized.append(line)

    paragraphs: list[str] = []
    list_items: list[str] = []
    seen_paragraphs: set[str] = set()
    title_candidate = ""

    def _push_paragraph(value: str) -> None:
        compact_value = re.sub(r"\s+", " ", str(value or "")).strip()
        if not compact_value:
            return
        signature = compact_value.casefold().rstrip(".!?")
        if signature in seen_paragraphs:
            return
        seen_paragraphs.add(signature)
        paragraphs.append(compact_value)

    for line in normalized:
        if not line:
            continue

        labeled_match = labeled_pattern.match(line)
        if labeled_match:
            label = labeled_match.group(1).strip().lower()
            content = labeled_match.group(2).strip()
            if label in {"key point", "key points", "what you can ask next"}:
                if content:
                    list_match = numbered_pattern.match(content) or bullet_pattern.match(content)
                    if list_match:
                        list_items.append(list_match.group(1).strip())
                continue
            if label in {"explanation", "overview", "quick recap"}:
                _push_paragraph(content)
                continue
            if label in {"example", "examples"}:
                if content:
                    example = content
                    if not example.lower().startswith("for example"):
                        example = f"For example, {example[0].lower() + example[1:]}" if len(example) > 1 else f"For example, {example.lower()}"
                    _push_paragraph(example)
                continue
            if label == "title":
                title_candidate = content
                continue

        list_match = numbered_pattern.match(line) or bullet_pattern.match(line)
        if list_match:
            list_items.append(list_match.group(1).strip())
            continue

        _push_paragraph(line)

    if not paragraphs and title_candidate:
        _push_paragraph(title_candidate)

    if not paragraphs and not list_items:
        return ""

    if not paragraphs and list_items:
        _push_paragraph(list_items.pop(0))

    output_lines: list[str] = []
    for idx, paragraph in enumerate(paragraphs):
        if idx:
            output_lines.append("")
        output_lines.append(paragraph)

    if list_items:
        if output_lines:
            output_lines.append("")
        output_lines.extend(
            f"{idx}. {item}" for idx, item in enumerate(list_items, start=1)
        )

    return "\n".join(output_lines).strip()

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

    return get_chat_panel_from_preloaded(
        student_id=student_id,
        goal=context["goal"],
        active_skill=context["active_skill"],
        selected_playlist=context["selected_playlist"],
        limit=limit,
    )


def get_chat_panel_from_preloaded(
    *,
    student_id: int,
    goal: dict,
    active_skill: dict | None,
    selected_playlist: dict | None,
    limit: int = 20,
) -> dict:
    if active_skill is None:
        return {
            "enabled": False,
            "reason": "All skills are completed. No active playlist chat needed.",
            "active_skill": None,
            "selected_playlist": None,
            "messages": [],
        }

    if selected_playlist is None:
        return {
            "enabled": False,
            "reason": f"Select one playlist for {active_skill['skill_name']} to enable chatbot.",
            "active_skill": active_skill,
            "selected_playlist": None,
            "messages": [],
        }

    session = chat_repo.get_session(
        student_id=student_id,
        goal_id=goal["id"],
        goal_skill_id=active_skill["id"],
        playlist_recommendation_id=selected_playlist["id"],
    )
    raw_messages = chat_repo.list_messages(session["id"], limit=limit) if session else []
    messages = [
        {
            **item,
            "message_text": _structure_assistant_answer(item.get("message_text", ""))
            if item.get("role") == "assistant"
            else item.get("message_text", ""),
        }
        for item in raw_messages
    ]
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
