from backend.roadmap_engine.storage import playlist_repo, roadmap_repo


def _fetch_recommendations_from_youtube(skill_name: str, limit: int = 3) -> tuple[list[dict], str | None]:
    try:
        from backend.youtube_module.llm_explainer.explain_playlists import get_or_generate_explanation
        from backend.youtube_module.ranking import aggregate_playlist_stats, rank_playlists
        from backend.youtube_module.youtube_client import (
            get_video_statistics,
            get_video_titles,
            get_videos_in_playlist,
            search_playlists,
        )
    except Exception as error:
        return [], f"YouTube module import failed: {error}"

    try:
        playlists = search_playlists(skill_name)
    except Exception as error:
        return [], f"YouTube search failed: {error}"

    if not playlists:
        return [], f"No playlists found for '{skill_name}'."

    playlists = playlists[:10]
    playlist_video_map = {}
    all_video_ids = set()

    for playlist in playlists:
        playlist_id = playlist["playlist_id"]
        try:
            video_ids = get_videos_in_playlist(playlist_id, max_videos=120)
        except Exception as error:
            return [], f"Failed to read playlist videos: {error}"
        playlist_video_map[playlist_id] = video_ids
        all_video_ids.update(video_ids)

        try:
            title_map = get_video_titles(video_ids[:8]) if video_ids else {}
        except Exception:
            title_map = {}
        playlist["top_video_titles"] = list(title_map.values())

    try:
        video_stats = get_video_statistics(list(all_video_ids)) if all_video_ids else {}
    except Exception as error:
        return [], f"Failed to fetch video statistics: {error}"
    for playlist in playlists:
        ids = playlist_video_map.get(playlist["playlist_id"], [])
        playlist.update(aggregate_playlist_stats(ids, video_stats))

    ranked = rank_playlists(playlists)[:limit]
    if not ranked:
        return [], "No ranked playlists available after scoring."

    output = []
    for item in ranked:
        video_ids = playlist_video_map.get(item["playlist_id"], [])
        summary = {}
        try:
            summary = get_or_generate_explanation(item)
        except Exception:
            summary = {}

        enhanced_summary = {
            **summary,
            "video_count": len(video_ids),
            "top_video_titles": item.get("top_video_titles", []),
        }

        output.append(
            {
                "playlist_id": item["playlist_id"],
                "title": item["title"],
                "channel_title": item.get("channel_title", ""),
                "playlist_url": f"https://www.youtube.com/playlist?list={item['playlist_id']}",
                "rank_score": float(item.get("engagement_ratio", 0.0)),
                "summary": {
                    **enhanced_summary,
                    "channel_url": (
                        f"https://www.youtube.com/channel/{item.get('channel_id', '')}"
                        if item.get("channel_id")
                        else ""
                    ),
                },
            }
        )

    return output, None


def get_or_create_recommendations(goal_id: int, goal_skill_id: int, skill_name: str) -> tuple[list[dict], str | None]:
    cached = playlist_repo.list_skill_recommendations(goal_id, goal_skill_id)
    if cached:
        return cached[:3], None

    generated, error = _fetch_recommendations_from_youtube(skill_name, limit=3)
    if generated:
        playlist_repo.replace_skill_recommendations(goal_id, goal_skill_id, generated)
        # Re-load from DB so recommendations include row ids required by selection form.
        refreshed = playlist_repo.list_skill_recommendations(goal_id, goal_skill_id)
        if refreshed:
            return refreshed[:3], None
        return [], "Playlist generation succeeded, but save failed. Please refresh and retry."
    return [], error or "No playlist suggestions available yet."


def _annotate_tasks_with_playlist(
    *,
    goal_id: int,
    goal_skill_id: int,
    skill_name: str,
    playlist: dict,
) -> None:
    plan = roadmap_repo.get_active_plan(goal_id)
    if not plan:
        return

    tasks = roadmap_repo.list_tasks_for_skill(plan["id"], goal_skill_id)
    active_tasks = [task for task in tasks if task["is_completed"] == 0]
    if not active_tasks:
        return

    summary = playlist.get("summary", {}) or {}
    video_count_raw = summary.get("video_count", 0)
    try:
        video_count = int(video_count_raw)
    except (TypeError, ValueError):
        video_count = 0

    total_days = len(active_tasks)
    updates: list[tuple[int, str, str]] = []
    for idx, task in enumerate(active_tasks):
        day_index = idx + 1
        title = f"{skill_name}: Playlist Day {day_index}"

        if video_count > 0 and total_days > 0:
            start_video = int((idx * video_count) / total_days) + 1
            end_video = int(((idx + 1) * video_count) / total_days)
            if end_video < start_video:
                end_video = start_video
            schedule_line = f"Watch videos {start_video}-{end_video}"
        else:
            schedule_line = "Watch the next part of your selected playlist"

        description = (
            f"{schedule_line} for {skill_name}.\n"
            f"Playlist: {playlist.get('title', '')}\n"
            f"Channel: {playlist.get('channel_title', '')}\n"
            f"URL: {playlist.get('playlist_url', '')}"
        )
        updates.append((task["id"], title, description))

    roadmap_repo.bulk_update_task_content(updates)


def select_playlist(goal_id: int, goal_skill_id: int, recommendation_id: int, skill_name: str) -> dict:
    playlist_repo.select_recommendation(goal_id, goal_skill_id, recommendation_id)
    selected = playlist_repo.get_selected_recommendation(goal_id, goal_skill_id)
    if selected is None:
        raise ValueError("Failed to save selected playlist.")
    _annotate_tasks_with_playlist(
        goal_id=goal_id,
        goal_skill_id=goal_skill_id,
        skill_name=skill_name,
        playlist=selected,
    )
    return selected


def get_selected_playlist(goal_id: int, goal_skill_id: int) -> dict | None:
    return playlist_repo.get_selected_recommendation(goal_id, goal_skill_id)
