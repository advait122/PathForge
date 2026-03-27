# qna.py

import os
from openai import OpenAI

from .qna_prompt import build_playlist_qna_prompt


MODEL_NAME = "llama-3.1-8b-instant"

client = OpenAI(
    api_key=os.getenv("GROQ_API_KEY"),
    base_url="https://api.groq.com/openai/v1",
)


def _base_messages(playlist: dict, playlist_summary: dict) -> list[dict]:
    prompts = build_playlist_qna_prompt(
        playlist_title=playlist["title"],
        channel_name=playlist.get("channel_title", ""),
        playlist_description=playlist.get("description", ""),
        top_video_titles=playlist.get("top_video_titles", []),
        playlist_summary=playlist_summary,
        student_question="",
    )
    return [
        {"role": "system", "content": prompts["system_prompt"]},
        {"role": "user", "content": prompts["user_prompt"]},
    ]


def answer_playlist_question(
    playlist: dict,
    playlist_summary: dict,
    student_question: str,
) -> str:
    """
    Answers a student's question using playlist-specific context.
    Returns plain text answer.
    """

    return answer_playlist_question_with_history(
        playlist=playlist,
        playlist_summary=playlist_summary,
        student_question=student_question,
        conversation_history=[],
    )


def answer_playlist_question_with_history(
    playlist: dict,
    playlist_summary: dict,
    student_question: str,
    conversation_history: list[dict] | None = None,
) -> str:
    messages = _base_messages(playlist, playlist_summary)

    for item in conversation_history or []:
        role = str(item.get("role", "")).strip().lower()
        content = str(item.get("content", "")).strip()
        if role in {"user", "assistant"} and content:
            messages.append({"role": role, "content": content})

    messages.append({"role": "user", "content": student_question})
    response = client.chat.completions.create(
        model=MODEL_NAME,
        messages=messages,
        temperature=0.4,
    )
    return response.choices[0].message.content.strip()


def start_playlist_chatbot(
    playlist: dict,
    playlist_summary: dict,
):
    """
    Starts a multi-turn, playlist-scoped chatbot session.
    Conversation memory is kept in RAM only.
    """

    # Build the base prompt once
    conversation_history = _base_messages(playlist, playlist_summary)

    print("\nYou can now ask multiple questions about this playlist.")
    print("Type 'exit' to end the chat.\n")

    answer = ""
    while True:
        student_question = input("> ").strip()

        if student_question.lower() == "exit":
            print("\nEnding chat.\n")
            break

        conversation_history.append(
            {"role": "user", "content": student_question}
        )

        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=conversation_history,
            temperature=0.4,
        )

        answer = response.choices[0].message.content.strip()

        conversation_history.append(
            {"role": "assistant", "content": answer}
        )

        print("\nANSWER:\n")
        print(answer)
        print("\n---\n")
    return answer
