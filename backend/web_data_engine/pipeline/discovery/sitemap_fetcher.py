import requests
from lxml import etree
from urllib.parse import urljoin


def fetch_sitemap(base_url: str):
    sitemap_url = urljoin(base_url, "/sitemap.xml")

    all_urls = []

    try:
        response = requests.get(sitemap_url, timeout=10)

        if response.status_code != 200:
            print(f"No sitemap found for {base_url}")
            return []

        xml_root = etree.fromstring(response.content)

        loc_tags = xml_root.findall(".//{*}loc")

        for loc in loc_tags:
            link = loc.text

            # If it's another sitemap → fetch it
            if link.endswith(".xml") or "sitemap" in link:
                try:
                    sub_resp = requests.get(link, timeout=10)
                    sub_root = etree.fromstring(sub_resp.content)

                    sub_urls = [
                        elem.text
                        for elem in sub_root.findall(".//{*}loc")
                    ]

                    all_urls.extend(sub_urls)

                except Exception as e:
                    print(f"Error reading sub-sitemap {link}: {e}")

            else:
                all_urls.append(link)

        print(f"Total discovered URLs: {len(all_urls)}")
        return all_urls

    except Exception as e:
        print(f"Error fetching sitemap for {base_url}: {e}")
        return []
