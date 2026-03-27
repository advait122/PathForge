import requests


BASE_URL = "https://devpost.com/api/hackathons"


def fetch_devpost_hackathons(pages=5):

    print("🔎 Fetching Devpost hackathons via API...")

    all_links = []

    for page in range(1, pages + 1):

        url = f"{BASE_URL}?page={page}"

        response = requests.get(url)
        data = response.json()

        hackathons = data.get("hackathons", [])

        print(f"Page {page}: {len(hackathons)} hackathons")

        for hackathon in hackathons:
            link = hackathon.get("url")
            if link:
                all_links.append(link)

    print(f"\n✅ Total hackathons collected: {len(all_links)}")

    return all_links
