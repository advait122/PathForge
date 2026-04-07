from backend.web_data_engine.config.companies import COMPANIES
from backend.web_data_engine.pipeline.crawler.page_fetcher import fetch_page
from backend.web_data_engine.pipeline.discovery.source_discovery import discover_source_urls
from backend.web_data_engine.pipeline.llm.llm_extractor import extract_opportunity_with_llm
from backend.web_data_engine.pipeline.storage.sqlite_db import delete_expired_opportunities, init_db, upsert_opportunity
from backend.web_data_engine.utils.hash_utils import generate_content_hash
from backend.web_data_engine.utils.text_cleaner import extract_clean_text
from backend.roadmap_engine.services import matching_service, opportunity_agent_service
from backend.roadmap_engine.storage import students_repo


def process_company(company):
    print("\n==============================")
    print(f"Processing: {company['name']}")
    print("==============================")

    urls = discover_source_urls(company)
    print(f"Total URLs discovered: {len(urls)}")

    for url in urls:
        print(f"\nProcessing URL: {url}")
        page_html = fetch_page(url)
        clean_text = extract_clean_text(page_html)
        if not clean_text:
            continue

        content_hash = generate_content_hash(clean_text)
        data = extract_opportunity_with_llm(clean_text)

        if isinstance(data, list):
            if not data:
                continue
            data = data[0]

        prepared = opportunity_agent_service.extract_and_validate_opportunity(
            clean_text=clean_text,
            extracted_seed={**(data or {}), "source_name": company["name"]},
            source_name=company["name"],
            source="crawler",
            url=url,
            content_hash=content_hash,
        )

        if prepared:
            upsert_opportunity(
                data=prepared,
                content_hash=content_hash,
                source="crawler",
                url=url,
            )



def main():
    print("Web Data Pipeline Started")

    init_db()
    delete_expired_opportunities()

    for company in COMPANIES:
        process_company(company)

    for student in students_repo.list_students():
        try:
            matching_service.refresh_opportunity_matches(int(student["id"]))
        except Exception:
            continue


if __name__ == "__main__":
    main()
