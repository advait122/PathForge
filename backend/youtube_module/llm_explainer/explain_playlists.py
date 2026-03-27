# explain_playlists.py

import json
import os
from typing import List, Dict

from openai import OpenAI
from .prompt import build_playlist_explainer_prompt


OUTPUT_DIR = "output"
MODEL_NAME = "llama-3.1-8b-instant"

client = OpenAI(
    api_key=os.getenv("GROQ_API_KEY"),
    base_url="https://api.groq.com/openai/v1",
)

def extract_json_from_text(text: str) -> dict:
    """
    Extracts the first JSON object found in a text string.
    Works with Groq / verbose LLM outputs.
    """
    start = text.find("{")
    end = text.rfind("}")

    if start == -1 or end == -1 or end <= start:
        raise ValueError("No valid JSON object found in LLM output")

    json_str = text[start : end + 1]
    return json.loads(json_str)
def generate_playlist_explanation(playlist: Dict) -> Dict:
    """
    Generates explanation for a single playlist using LLM.
    """

    prompts = build_playlist_explainer_prompt(
        playlist_title=playlist["title"],
        playlist_description=playlist.get("description", ""),
        channel_name=playlist.get("channel_title", ""),
        top_video_titles=playlist.get("top_video_titles", []),
    )

    response = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[
            {"role": "system", "content": prompts["system_prompt"]},
            {"role": "user", "content": prompts["user_prompt"]},
        ],
        temperature=0.4,
    )

    raw_output = response.choices[0].message.content
    parsed_output = extract_json_from_text(raw_output)

    return parsed_output


def get_or_generate_explanation(playlist: Dict) -> Dict:
    """
    Returns cached explanation if exists, otherwise generates and caches it.
    """

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    playlist_id = playlist["playlist_id"]
    output_path = os.path.join(OUTPUT_DIR, f"{playlist_id}.json")

    if os.path.exists(output_path):
        with open(output_path, "r", encoding="utf-8") as f:
            return json.load(f)

    explanation = generate_playlist_explanation(playlist)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "playlist_id": playlist_id,
                **explanation,
            },
            f,
            indent=2,
            ensure_ascii=False,
        )

    return explanation
