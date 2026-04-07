from urllib.parse import urlparse

from backend.web_data_engine.pipeline.crawler.page_fetcher import fetch_page
from backend.web_data_engine.pipeline.discovery.devpost_fetcher import fetch_devpost_hackathons
from backend.web_data_engine.pipeline.discovery.sitemap_fetcher import fetch_sitemap
from backend.web_data_engine.utils.link_extractor import extract_internal_links


def _normalize_domains(source: dict) -> list[str]:
    domains = [str(domain).lower() for domain in source.get("allowed_domains") or [] if str(domain).strip()]
    base_url = str(source.get("base_url") or "").strip()
    if base_url:
        host = urlparse(base_url).netloc.lower()
        if host and host not in domains:
            domains.append(host)
    return domains


def _normalize_keywords(values: list[str] | None) -> list[str]:
    return [str(value).lower() for value in (values or []) if str(value).strip()]


def _dedupe(urls: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for url in urls:
        cleaned = str(url or "").strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        ordered.append(cleaned)
    return ordered


def _discover_seed_urls(source: dict) -> list[str]:
    seed_urls = list(source.get("seed_urls") or [])
    if not seed_urls:
        return []

    expand_links = bool(source.get("expand_links", True))
    allowed_domains = _normalize_domains(source)
    include_keywords = _normalize_keywords(source.get("include_keywords"))
    exclude_keywords = _normalize_keywords(source.get("exclude_keywords"))
    per_seed_limit = int(source.get("per_seed_limit") or 20)

    discovered: list[str] = []
    for seed_url in seed_urls:
        if not expand_links:
            discovered.append(seed_url)
            continue

        html = fetch_page(seed_url)
        if not html:
            discovered.append(seed_url)
            continue

        links = extract_internal_links(
            html,
            seed_url,
            allowed_domains=allowed_domains,
            include_keywords=include_keywords,
            exclude_keywords=exclude_keywords,
            limit=per_seed_limit,
        )
        if links:
            discovered.extend(links)
        else:
            discovered.append(seed_url)

    return discovered


def _discover_sitemap_urls(source: dict) -> list[str]:
    base_url = str(source.get("base_url") or "").strip()
    if not base_url:
        return []

    allowed_domains = _normalize_domains(source)
    include_keywords = _normalize_keywords(source.get("include_keywords"))
    exclude_keywords = _normalize_keywords(source.get("exclude_keywords"))

    urls = fetch_sitemap(base_url)
    if not urls:
        return []

    filtered: list[str] = []
    for url in urls:
        parsed = urlparse(url)
        host = parsed.netloc.lower()
        if allowed_domains and not any(host == domain or host.endswith(f".{domain}") for domain in allowed_domains):
            continue
        lowered = f"{parsed.path} {parsed.query}".lower()
        if include_keywords and not any(keyword in lowered for keyword in include_keywords):
            continue
        if exclude_keywords and any(keyword in lowered for keyword in exclude_keywords):
            continue
        filtered.append(url)
    return filtered


def discover_source_urls(source: dict) -> list[str]:
    if source.get("enabled") is False:
        return []

    source_kind = str(source.get("source_kind") or "seed_urls").strip().lower()
    max_urls = int(source.get("max_discovered_urls") or 25)

    if source_kind == "devpost_api":
        urls = fetch_devpost_hackathons(int(source.get("pages") or 5))
    elif source_kind == "sitemap":
        urls = _discover_sitemap_urls(source)
    else:
        urls = _discover_seed_urls(source)

    return _dedupe(urls)[:max_urls]
