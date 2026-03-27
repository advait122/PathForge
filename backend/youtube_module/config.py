# config.py

import os

# YouTube Data API key
# Loaded from environment variable
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

if not YOUTUBE_API_KEY:
    raise RuntimeError(
        "YOUTUBE_API_KEY not set. Please set it as an environment variable."
    )

# YouTube API service name and version
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

# Maximum number of playlists to fetch per query
MAX_RESULTS_PER_QUERY = 25