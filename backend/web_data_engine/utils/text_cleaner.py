from bs4 import BeautifulSoup


def extract_clean_text(html: str) -> str:
    """
    Converts raw HTML into clean visible text for LLM processing.
    """

    if not html:
        return ""

    soup = BeautifulSoup(html, "html.parser")

    # Remove unwanted tags
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    text = soup.get_text(separator=" ")

    # Remove extra whitespace
    clean_text = " ".join(text.split())

    return clean_text
