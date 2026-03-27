from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse


def extract_internal_links(html, base_url):
    soup = BeautifulSoup(html, "html.parser")

    links = set()
    domain = urlparse(base_url).netloc

    for a in soup.find_all("a", href=True):
        href = urljoin(base_url, a["href"])
        if urlparse(href).netloc == domain:
            links.add(href)

    return list(links)
