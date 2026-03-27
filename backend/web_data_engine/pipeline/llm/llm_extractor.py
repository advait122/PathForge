import os
import json
from groq import Groq

client = Groq(api_key=os.getenv("GROQ_API_KEY"))

MODEL = "llama-3.3-70b-versatile"


SYSTEM_PROMPT = """
You are an information extraction engine.

Extract structured opportunity data from the given text.

Return ONLY valid JSON.

Fields:
- title
- company
- type → job / internship / hackathon
- deadline → last date to apply (null if not found)
- skills → list of required skills (empty list if not found)

Return valid JSON only. No explanations.
"""


def extract_opportunity_with_llm(clean_text: str) -> dict:

    if not clean_text:
        return None

    user_prompt = f"""
Extract the opportunity details from the text below.

TEXT:
{clean_text}
"""

    response = client.chat.completions.create(
        model=MODEL,
        temperature=0,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
    )

    
    content = response.choices[0].message.content.strip()

# 🔧 Remove markdown code fences if present
    if content.startswith("```"):
     content = content.split("```")[1]

# 🔧 Remove optional 'json' label
    content = content.replace("json", "", 1).strip()

    try:
        return json.loads(content)
    except Exception as e:
        print("Invalid JSON from LLM")
        print(content)
        return None

