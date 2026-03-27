# ranking.py

def aggregate_playlist_stats(video_ids: list, video_stats_map: dict) -> dict:
    """
    Aggregates video-level statistics into playlist-level statistics.

    Args:
        video_ids (list): List of video IDs in the playlist
        video_stats_map (dict): Mapping of video_id -> stats

    Returns:
        dict: Aggregated playlist statistics
    """
    total_views = 0
    total_likes = 0
    total_comments = 0

    for video_id in video_ids:
        stats = video_stats_map.get(video_id)
        if not stats:
            continue

        total_views += stats.get("views", 0)
        total_likes += stats.get("likes", 0)
        total_comments += stats.get("comments", 0)

    if total_views == 0:
        engagement_ratio = 0.0
    else:
        engagement_ratio = total_likes / total_views

    return {
        "total_views": total_views,
        "total_likes": total_likes,
        "total_comments": total_comments,
        "engagement_ratio": engagement_ratio,
    }


def rank_playlists(playlists: list) -> list:
    """
    Ranks playlists based on precomputed engagement ratio.

    Args:
        playlists (list): List of playlist dictionaries
                          (must contain 'engagement_ratio')

    Returns:
        list: Playlists sorted by engagement ratio (descending)
    """
    ranked_playlists = sorted(
        playlists,
        key=lambda x: x.get("engagement_ratio", 0),
        reverse=True,
    )

    return ranked_playlists
