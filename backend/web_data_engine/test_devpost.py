from pipeline.discovery.devpost_fetcher import fetch_devpost_hackathons

urls = fetch_devpost_hackathons()

print("\nSample hackathons:\n")

for url in urls[:5]:
    print(url)
