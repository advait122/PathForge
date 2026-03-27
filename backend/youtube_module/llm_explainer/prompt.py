# prompt.py

def build_playlist_explainer_prompt(
    playlist_title: str,
    playlist_description: str,
    channel_name: str,
    top_video_titles: list[str],
) -> dict:
    """
    Builds the system and user prompt for the offline LLM playlist explanation.

    Returns:
        dict: {
            "system_prompt": str,
            "user_prompt": str
        }
    """

    system_prompt = (
        "You are an educational assistant helping students understand learning resources.\n\n"
        "Your task is to explain technical topics and playlists in clear, simple, and student-friendly language.\n"
        "Do not assume prior expertise.\n"
        "Do not classify learners by level.\n"
        "Do not exaggerate or add concepts that are not present in the input.\n"
        "Do not recommend or compare playlists.\n\n"
        "Your goal is clarity, neutrality, and confidence-building."
    )

    user_prompt = (
        "You are given metadata about a YouTube playlist.\n\n"
        f"Playlist Title:\n{playlist_title}\n\n"
        f"Playlist Description:\n{playlist_description}\n\n"
        f"Channel Name:\n{channel_name}\n\n"
        "Top Video Titles:\n"
        + "\n".join(f"- {title}" for title in top_video_titles)
        + "\n\n"
        "Based only on the information above, generate the following three sections:\n\n"
        "1. Topic Overview:\n"
        "Give a generalized, high-level explanation of the topic itself. "
        "Explain what the topic is, why it is important, and where it is commonly used. "
        "Do not reference the playlist directly in this section.\n\n"
        "2. Learning Experience:\n"
        "Explain what a learner can expect while following this specific playlist. "
        "Describe the teaching flow, progression of ideas, and style of explanation. "
        "Do not classify the learner or label difficulty.\n\n"
        "3. Topics Covered Summary:\n"
        "Aggregate and summarize the main concepts covered across the playlist. "
        "Use simple language and focus on conceptual coverage rather than ordering.\n\n"
        "Return the response strictly in valid JSON with exactly these keys:\n"
        "- topic_overview\n"
        "- learning_experience\n"
        "- topics_covered_summary"
    )

    return {
        "system_prompt": system_prompt,
        "user_prompt": user_prompt,
    }
