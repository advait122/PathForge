import requests


def fetch_jobs_from_api(api_url: str):
    try:
        response = requests.get(api_url, timeout=10)

        if response.status_code != 200:
            print("API request failed")
            return []

        data = response.json()

        print(f"Fetched data from API: {api_url}")

        return data

    except Exception as e:
        print(f"API fetch error: {e}")
        return []
