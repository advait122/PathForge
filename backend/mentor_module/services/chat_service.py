from backend.mentor_module.services import mentor_service
from backend.mentor_module.storage import mentor_repo
from backend.roadmap_engine.services.skill_normalizer import display_skill, normalize_skill
from backend.roadmap_engine.storage import goals_repo, matching_repo
from backend.roadmap_engine.storage.database import get_connection


def _get_student_name(student_id: int) -> str:
    conn = get_connection()
    try:
        row = conn.execute("SELECT name FROM students WHERE id = ?", (student_id,)).fetchone()
    finally:
        conn.close()
    return str(row["name"]) if row else f"Student #{student_id}"


def _send_notification(
    *,
    student_id: int,
    notification_type: str,
    title: str,
    body: str,
) -> None:
    goal = goals_repo.get_active_goal(student_id)
    goal_id = int(goal["id"]) if goal else None
    matching_repo.create_notification(
        student_id=student_id,
        goal_id=goal_id,
        notification_type=notification_type,
        title=title,
        body=body,
    )


def start_session(*, seeker_id: int, mentor_id: int, normalized_skill: str) -> int:
    """
    Create a mentor session. Returns session_id.
    If an open session already exists between this pair for this skill,
    returns the existing session_id (no duplicate created).
    """
    if seeker_id == mentor_id:
        raise ValueError("You cannot request yourself as a mentor.")

    normalized_skill_key = normalize_skill(normalized_skill)
    profile = mentor_repo.get_mentor_profile(mentor_id, normalized_skill_key)
    if not profile or not profile.get("opted_in"):
        raise ValueError("This mentor is not available for this skill.")

    # Return existing open session instead of creating a duplicate
    existing = mentor_repo.get_open_session(seeker_id, mentor_id, normalized_skill_key)
    if existing:
        return int(existing["id"])

    session_id = mentor_repo.create_session(seeker_id, mentor_id, normalized_skill_key)

    seeker_name = _get_student_name(seeker_id)
    skill_label = display_skill(normalized_skill_key)
    _send_notification(
        student_id=mentor_id,
        notification_type="mentor_session_request",
        title=f"New Mentor Request: {skill_label}",
        body=(
            f"{seeker_name} needs help with {skill_label}. "
            f"Visit your Mentor Hub to respond."
        ),
    )

    return session_id


def send_message(*, session_id: int, sender_id: int, message_text: str) -> None:
    session = mentor_repo.get_session(session_id)
    if session is None:
        raise ValueError("Session not found.")
    if session["status"] != "open":
        raise ValueError("This session is already closed.")
    if sender_id not in (int(session["seeker_id"]), int(session["mentor_id"])):
        raise ValueError("You are not part of this session.")

    text = str(message_text or "").strip()
    if not text:
        raise ValueError("Message cannot be empty.")
    if len(text) > 2000:
        raise ValueError("Message is too long (max 2000 characters).")

    mentor_repo.add_message(session_id, sender_id, text)


def close_session(*, session_id: int, student_id: int) -> None:
    """Only the seeker (student who requested help) can close the session."""
    session = mentor_repo.get_session(session_id)
    if session is None:
        raise ValueError("Session not found.")
    if int(session["seeker_id"]) != student_id:
        raise ValueError("Only the student who requested help can close this session.")
    if session["status"] != "open":
        raise ValueError("Session is already closed.")

    mentor_repo.close_session(session_id)

    mentor_id = int(session["mentor_id"])
    normalized_skill = str(session["normalized_skill"])

    badge_info = mentor_service.after_session_close(
        mentor_id=mentor_id,
        normalized_skill=normalized_skill,
    )

    if badge_info["badge_upgraded"]:
        badge_level = str(badge_info["new_badge"])
        skill_label = display_skill(normalized_skill)
        badge_labels = {"bronze": "Bronze", "silver": "Silver", "gold": "Gold"}
        _send_notification(
            student_id=mentor_id,
            notification_type="mentor_badge_awarded",
            title=f"Badge Unlocked: {badge_labels.get(badge_level, badge_level.title())} Mentor",
            body=(
                f"You earned the {badge_labels.get(badge_level, badge_level.title())} Mentor badge "
                f"for {skill_label}! You've helped "
                f"{badge_info['people_helped']} student(s) so far. Keep it up!"
            ),
        )


def cancel_session(*, session_id: int, student_id: int) -> None:
    """Seeker can cancel an open session before it's resolved."""
    session = mentor_repo.get_session(session_id)
    if session is None:
        raise ValueError("Session not found.")
    if int(session["seeker_id"]) != student_id:
        raise ValueError("Only the student who requested help can cancel this session.")
    if session["status"] != "open":
        raise ValueError("Session is already closed or cancelled.")
    mentor_repo.cancel_session(session_id)


def submit_review(
    *, session_id: int, student_id: int, rating: int, review_text: str
) -> None:
    """Seeker submits a 1-5 star review after the session is closed. One review per session."""
    session = mentor_repo.get_session(session_id)
    if session is None:
        raise ValueError("Session not found.")
    if int(session["seeker_id"]) != student_id:
        raise ValueError("Only the student who requested help can submit a review.")
    if session["status"] != "closed":
        raise ValueError("Session must be closed before submitting a review.")

    existing = mentor_repo.get_review_for_session(session_id)
    if existing:
        raise ValueError("You have already submitted a review for this session.")

    if not (1 <= rating <= 5):
        raise ValueError("Rating must be between 1 and 5.")

    mentor_repo.create_review(
        session_id=session_id,
        mentor_id=int(session["mentor_id"]),
        seeker_id=student_id,
        rating=rating,
        review_text=str(review_text or "").strip(),
    )


def get_session_with_messages(session_id: int) -> dict | None:
    session = mentor_repo.get_session(session_id)
    if session is None:
        return None
    messages = mentor_repo.get_messages(session_id)
    review = mentor_repo.get_review_for_session(session_id)
    return {**session, "messages": messages, "review": review}


def get_mentor_inbox(mentor_id: int) -> list[dict]:
    sessions = mentor_repo.get_sessions_for_mentor(mentor_id)
    enriched = []
    for s in sessions:
        review = mentor_repo.get_review_for_session(int(s["id"]))
        enriched.append({**s, "review": review})
    return enriched


def get_seeker_sessions(seeker_id: int) -> list[dict]:
    """All sessions where this student requested help (as seeker)."""
    return mentor_repo.get_sessions_for_seeker(seeker_id)
