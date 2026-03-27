from config.companies import COMPANIES

from pipeline.discovery.sitemap_fetcher import fetch_sitemap
from pipeline.crawler.page_fetcher import fetch_page

from utils.text_cleaner import extract_clean_text
from utils.hash_utils import generate_content_hash

from pipeline.llm.llm_extractor import extract_opportunity_with_llm
from pipeline.storage.sqlite_db import init_db, upsert_opportunity, delete_expired_opportunities

from pipeline.discovery.devpost_fetcher import fetch_devpost_hackathons
from utils.link_extractor import extract_internal_links


def process_company(company):

    print(f"\n==============================")
    print(f"Processing: {company['name']}")
    print(f"==============================")

    # 🔍 DISCOVERY

    if company["name"] == "Devpost":
        urls = fetch_devpost_hackathons()

    elif "seed_urls" in company:
        urls = company["seed_urls"]

    elif company["use_sitemap"]:
        urls = fetch_sitemap(company["base_url"])

    else:
        urls = []


    print(f"Total URLs discovered: {len(urls)}")

    

    # 🔴 limit for testing
    #urls = urls[:3]

    # ⚙️ PROCESSING (runs for BOTH Google & Devpost)
    for url in urls:

        print(f"\n🌐 Processing URL: {url}")

        html = fetch_page(url)

        # 🧠 If this company uses seed URLs → expand links
        if "seed_urls" in company:
            discovered_links = extract_internal_links(html, url)

            print(f"🔗 Found {len(discovered_links)} internal links")

            # optional test limit
            #discovered_links = discovered_links[:5]

            target_urls = discovered_links
        else:
            target_urls = [url]

        # ⚙️ PROCESS EACH TARGET PAGE
        for target in target_urls:

            print(f"➡️ Processing target: {target}")

            page_html = fetch_page(target)
            clean_text = extract_clean_text(page_html)
            content_hash = generate_content_hash(clean_text)

            data = extract_opportunity_with_llm(clean_text)

            if isinstance(data, list):
                if len(data) == 0:
                    continue
                data = data[0]

            if data:
                upsert_opportunity(
                    data=data,
                    content_hash=content_hash,
                    source="crawler",
                    url=target
                )



def main():

    print("🚀 Web Data Pipeline Started")

    init_db()
    
    # 🗑️ Clean up expired opportunities first
    delete_expired_opportunities()

    for company in COMPANIES:
        process_company(company)



if __name__ == "__main__":
    main()
