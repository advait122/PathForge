from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse


def _matches_domain(url: str, allowed_domains: list[str] | None) -> bool:
    if not allowed_domains:
        return True
    host = urlparse(url).netloc.lower()
    return any(host == domain or host.endswith(f".{domain}") for domain in allowed_domains)


def _matches_keywords(url: str, include_keywords: list[str] | None, exclude_keywords: list[str] | None) -> bool:
    parsed = urlparse(url)
    lowered = f"{parsed.path} {parsed.query}".lower()
    if include_keywords and not any(keyword in lowered for keyword in include_keywords):
        return False
    if exclude_keywords and any(keyword in lowered for keyword in exclude_keywords):
        return False
    return True


def extract_internal_links(
    html,
    base_url,
    *,
    allowed_domains: list[str] | None = None,
    include_keywords: list[str] | None = None,
    exclude_keywords: list[str] | None = None,
    limit: int | None = None,
):
    soup = BeautifulSoup(html or "", "html.parser")

    links = []
    seen = set()
    base_domain = urlparse(base_url).netloc.lower()
    domain_allowlist = allowed_domains or [base_domain]

    for anchor in soup.find_all("a", href=True):
        href = urljoin(base_url, anchor["href"]).split("#", 1)[0]
        parsed = urlparse(href)
        if parsed.scheme not in {"http", "https"}:
            continue
        if not _matches_domain(href, domain_allowlist):
            continue
        if not _matches_keywords(href, include_keywords, exclude_keywords):
            continue
        if href in seen:
            continue
        seen.add(href)
        links.append(href)
        if limit and len(links) >= limit:
            break

    return links
