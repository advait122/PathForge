import requests
from urllib.parse import urljoin


def parse_robots(base_url: str):
    robots_url = urljoin(base_url, "/robots.txt")

    allowed = True
    sitemaps = []

    try:
        response = requests.get(robots_url, timeout=10)

        if response.status_code != 200:
            print(f"No robots.txt found for {base_url}")
            return allowed, sitemaps

        lines = response.text.splitlines()

        for line in lines:
            line = line.strip()

            if line.lower().startswith("disallow:"):
                path = line.split(":")[1].strip()
                if path == "/":
                    allowed = False

            if line.lower().startswith("sitemap:"):
                sitemap_url = line.split(":")[1].strip()
                sitemaps.append(sitemap_url)

        print(f"robots.txt parsed for {base_url}")
        print(f"Crawling allowed: {allowed}")
        print(f"Found {len(sitemaps)} sitemap(s) in robots.txt")

        return allowed, sitemaps

    except Exception as e:
        print(f"Error reading robots.txt for {base_url}: {e}")
        return allowed, sitemaps
