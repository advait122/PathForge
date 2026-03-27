from bs4 import BeautifulSoup
import re


def extract_google_job_data(html: str):
    try:
        soup = BeautifulSoup(html, "html.parser")

        # ---------------------------
        # TITLE
        # ---------------------------
        title = None
        title_tag = soup.find("h2")

        if title_tag:
            title = title_tag.text.strip()

        # ---------------------------
        # DEADLINE (search whole page text)
        # ---------------------------
        deadline = None

        text = soup.get_text(" ", strip=True)

        # simple date pattern like "Jun 30, 2026"
        date_pattern = r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2}, \d{4}"

        match = re.search(date_pattern, text)

        if match:
            deadline = match.group(0)

        return title, deadline

    except Exception as e:
        print(f"Extraction error: {e}")
        return None, None
