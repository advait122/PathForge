import hashlib


def generate_content_hash(text: str) -> str:
    """
    Generates a stable hash for the given text content.
    Used for change detection.
    """

    if not text:
        return None

    text = text.strip().encode("utf-8")

    return hashlib.sha256(text).hexdigest()
