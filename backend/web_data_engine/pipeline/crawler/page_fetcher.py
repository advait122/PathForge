import requests


def fetch_page(url: str):
    try:
        response = requests.get(url, timeout=10)

        if response.status_code == 200:
            return response.text

        print(f"Failed to fetch page: {url}")
        return None

    except Exception as e:
        print(f"Error fetching page {url}: {e}")
        return None
