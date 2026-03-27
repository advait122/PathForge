# qna_prompt.py

def build_playlist_qna_prompt(
    playlist_title: str,
    channel_name: str,
    playlist_description: str,
    top_video_titles: list,
    playlist_summary: dict,
    student_question: str,
) -> dict:
    """
    Builds the prompt for Mode 2: Playlist-specific Q&A.
    Output is plain text (not JSON).
    """

    system_prompt = (
    "You are a helpful study assistant for students.\n"
    "The playlist you are given clearly claims to cover certain computer science topics.\n"
    "You MAY explain standard, well-known concepts that are clearly part of the playlist topic "
    "(for example: Binary Search Trees, AVL Trees, rotations, traversals), "
    "even if the playlist summary is high-level.\n"
    "Do NOT introduce topics that are outside the scope of the playlist.\n"
    "Do NOT invent advanced or unrelated material.\n"
    "If a question is genuinely outside the playlist scope, politely say so.\n"
    "Explain concepts in a clear, student-friendly way.\n"
)

    user_prompt = f"""
PLAYLIST CONTEXT

Playlist Title:
{playlist_title}

Channel:
{channel_name}

Playlist Description:
{playlist_description}

Top Video Titles:
{", ".join(top_video_titles)}

Playlist Summary (for reference):
- Topic Overview: {playlist_summary.get("topic_overview")}
- Learning Experience: {playlist_summary.get("learning_experience")}
- Topics Covered: {playlist_summary.get("topics_covered_summary")}

STUDENT QUESTION:
{student_question}

INSTRUCTIONS:
Answer the student's question using only the playlist context above.
Be clear, honest, and helpful.
"""

    return {
        "system_prompt": system_prompt.strip(),
        "user_prompt": user_prompt.strip(),
    }