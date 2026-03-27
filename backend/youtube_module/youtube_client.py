# youtube_client.py

from googleapiclient.discovery import build
try:
    # Package import path (used by web app runtime)
    from .config import (
        MAX_RESULTS_PER_QUERY,
        YOUTUBE_API_KEY,
        YOUTUBE_API_SERVICE_NAME,
        YOUTUBE_API_VERSION,
    )
except ImportError:
    # Script import path (used by `python main.py` from this directory)
    from config import (
        MAX_RESULTS_PER_QUERY,
        YOUTUBE_API_KEY,
        YOUTUBE_API_SERVICE_NAME,
        YOUTUBE_API_VERSION,
    )


def get_youtube_client():
    """
    Creates and returns a YouTube API client.
    """
    return build(
        YOUTUBE_API_SERVICE_NAME,
        YOUTUBE_API_VERSION,
        developerKey=YOUTUBE_API_KEY,
    )


def search_playlists(query: str):
    """
    Searches YouTube for playlists related to the given query.

    Args:
        query (str): Search query string

    Returns:
        list of dicts containing raw playlist metadata
    """
    youtube = get_youtube_client()

    request = youtube.search().list(
        part="snippet",
        q=query,
        type="playlist",
        maxResults=MAX_RESULTS_PER_QUERY,
    )

    response = request.execute()

    playlists = []

    for item in response.get("items", []):
        playlist_id = item["id"]["playlistId"]
        snippet = item["snippet"]

        playlists.append(
            {
                "playlist_id": playlist_id,
                "title": snippet["title"],
                "description": snippet.get("description", ""),
                "channel_title": snippet.get("channelTitle", ""),
                "channel_id": snippet.get("channelId", ""),
            }
        )

    return playlists

def get_videos_in_playlist(playlist_id: str, max_videos: int = 200):
    """
    Fetches video IDs from a playlist using pagination.

    Args:
        playlist_id (str): YouTube playlist ID
        max_videos (int): Maximum number of videos to fetch

    Returns:
        list: List of video IDs
    """
    youtube = get_youtube_client()

    video_ids = []
    next_page_token = None

    while len(video_ids) < max_videos:
        request = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token,
        )

        response = request.execute()

        for item in response.get("items", []):
            video_id = item.get("contentDetails", {}).get("videoId")
            if video_id:
                video_ids.append(video_id)

                if len(video_ids) >= max_videos:
                    break

        next_page_token = response.get("nextPageToken")

        if not next_page_token:
            break

    return video_ids
def get_video_statistics(video_ids: list):
    """
    Fetches statistics for a list of video IDs.

    Args:
        video_ids (list): List of YouTube video IDs

    Returns:
        dict: Mapping of video_id -> stats
    """
    youtube = get_youtube_client()

    stats_map = {}

    # YouTube allows up to 50 video IDs per request
    for i in range(0, len(video_ids), 50):
        batch_ids = video_ids[i:i + 50]

        request = youtube.videos().list(
            part="statistics",
            id=",".join(batch_ids),
        )

        response = request.execute()

        for item in response.get("items", []):
            video_id = item["id"]
            statistics = item.get("statistics", {})

            stats_map[video_id] = {
                "views": int(statistics.get("viewCount", 0)),
                "likes": int(statistics.get("likeCount", 0)),
                "comments": int(statistics.get("commentCount", 0)),
            }

    return stats_map
def get_video_titles(video_ids: list):
    """
    Fetches video titles for a list of video IDs.
    """

    youtube = get_youtube_client()
    titles_map = {}

    # YouTube allows up to 50 IDs per request
    for i in range(0, len(video_ids), 50):
        batch_ids = video_ids[i:i + 50]

        request = youtube.videos().list(
            part="snippet",
            id=",".join(batch_ids),
        )

        response = request.execute()

        for item in response.get("items", []):
            video_id = item["id"]
            snippet = item.get("snippet", {})
            titles_map[video_id] = snippet.get("title", "")

    return titles_map




