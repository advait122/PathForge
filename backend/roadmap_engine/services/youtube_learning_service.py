from datetime import date, timedelta
import os

from backend.roadmap_engine.storage import goals_repo, playlist_repo, roadmap_repo, students_repo


def _hydrate_playlist_summary(playlist: dict) -> dict:
    summary = dict(playlist.get("summary", {}) or {})
    existing_videos = summary.get("videos", [])
    if isinstance(existing_videos, list) and existing_videos:
        return playlist

    playlist_id = str(playlist.get("playlist_id", "")).strip()
    if not playlist_id:
        return playlist

    try:
        from backend.youtube_module.youtube_client import get_video_metadata, get_videos_in_playlist
    except Exception:
        return playlist

    try:
        video_ids = get_videos_in_playlist(playlist_id, max_videos=120)
        video_metadata = get_video_metadata(video_ids) if video_ids else {}
    except Exception:
        return playlist

    ordered_video_details = []
    total_duration_seconds = 0
    for index, video_id in enumerate(video_ids, start=1):
        metadata = video_metadata.get(video_id, {})
        duration_seconds = int(metadata.get("duration_seconds", 0) or 0)
        total_duration_seconds += duration_seconds
        ordered_video_details.append(
            {
                "index": index,
                "video_id": video_id,
                "title": metadata.get("title", ""),
                "duration_seconds": duration_seconds,
                "duration_minutes": int(metadata.get("duration_minutes", 0) or 0),
                "video_url": f"https://www.youtube.com/watch?v={video_id}",
            }
        )

    if not ordered_video_details:
        return playlist

    summary["video_count"] = len(ordered_video_details)
    summary["total_duration_minutes"] = max(1, round(total_duration_seconds / 60)) if total_duration_seconds else 0
    summary["top_video_titles"] = [video["title"] for video in ordered_video_details[:8] if video.get("title")]
    summary["videos"] = ordered_video_details

    updated_playlist = {**playlist, "summary": summary}
    recommendation_id = playlist.get("id")
    if recommendation_id:
        try:
            playlist_repo.update_recommendation_summary(int(recommendation_id), summary)
        except Exception:
            pass
    return updated_playlist


def _fetch_recommendations_from_youtube(skill_name: str, limit: int = 3) -> tuple[list[dict], str | None]:
    try:
        from backend.youtube_module.llm_explainer.explain_playlists import get_or_generate_explanation
        from backend.youtube_module.ranking import aggregate_playlist_stats, rank_playlists
        from backend.youtube_module.youtube_client import (
            get_video_metadata,
            get_video_statistics,
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
    playlist_video_details_map = {}
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
        video_stats = get_video_statistics(list(all_video_ids)) if all_video_ids else {}
    except Exception as error:
        return [], f"Failed to fetch video statistics: {error}"

    try:
        video_metadata = get_video_metadata(list(all_video_ids)) if all_video_ids else {}
    except Exception as error:
        return [], f"Failed to fetch video metadata: {error}"

    for playlist in playlists:
        ids = playlist_video_map.get(playlist["playlist_id"], [])
        ordered_video_details = []
        total_duration_seconds = 0
        for index, video_id in enumerate(ids, start=1):
            metadata = video_metadata.get(video_id, {})
            duration_seconds = int(metadata.get("duration_seconds", 0) or 0)
            total_duration_seconds += duration_seconds
            ordered_video_details.append(
                {
                    "index": index,
                    "video_id": video_id,
                    "title": metadata.get("title", ""),
                    "duration_seconds": duration_seconds,
                    "duration_minutes": int(metadata.get("duration_minutes", 0) or 0),
                    "video_url": f"https://www.youtube.com/watch?v={video_id}",
                }
            )
        playlist_video_details_map[playlist["playlist_id"]] = ordered_video_details
        playlist["top_video_titles"] = [video["title"] for video in ordered_video_details[:8] if video.get("title")]
        playlist["total_duration_minutes"] = max(1, round(total_duration_seconds / 60)) if total_duration_seconds else 0
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
            "total_duration_minutes": item.get("total_duration_minutes", 0),
            "top_video_titles": item.get("top_video_titles", []),
            "videos": playlist_video_details_map.get(item["playlist_id"], []),
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


def _student_daily_minutes_budget(goal_id: int) -> int:
    goal = goals_repo.get_goal(goal_id)
    if not goal:
        return 60

    student = students_repo.get_student(int(goal.get("student_id", 0) or 0))
    if not student:
        return 60

    weekly_hours = max(1, int(student.get("weekly_study_hours", 8) or 8))
    daily_minutes = round((weekly_hours * 60) / 7)
    return max(30, daily_minutes)


def _build_video_chunks(videos: list[dict], daily_budget_minutes: int) -> list[dict]:
    if not videos:
        return []

    chunks: list[dict] = []
    current_chunk: list[dict] = []
    current_minutes = 0
    upper_bound = max(daily_budget_minutes, round(daily_budget_minutes * 1.15))
    standalone_threshold = max(30, round(daily_budget_minutes * 0.85))

    for video in videos:
        video_minutes = max(1, int(video.get("duration_minutes", 0) or 0))

        if video_minutes >= standalone_threshold:
            if current_chunk:
                chunks.append({"videos": current_chunk, "minutes": current_minutes})
                current_chunk = []
                current_minutes = 0
            chunks.append({"videos": [video], "minutes": video_minutes})
            continue

        projected = current_minutes + video_minutes
        if current_chunk and projected > upper_bound:
            chunks.append({"videos": current_chunk, "minutes": current_minutes})
            current_chunk = [video]
            current_minutes = video_minutes
            continue

        current_chunk.append(video)
        current_minutes = projected

    if current_chunk:
        chunks.append({"videos": current_chunk, "minutes": current_minutes})

    return chunks


def get_or_create_recommendations(goal_id: int, goal_skill_id: int, skill_name: str) -> tuple[list[dict], str | None]:
    cached = playlist_repo.list_skill_recommendations(goal_id, goal_skill_id)
    if cached:
        return cached[:3], None

    # Avoid repeated slow external calls when YouTube credentials are not configured.
    if not os.getenv("YOUTUBE_API_KEY", "").strip():
        return [], "YOUTUBE_API_KEY not set. Please set it as an environment variable."

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
    playlist = _hydrate_playlist_summary(playlist)
    plan = roadmap_repo.get_active_plan(goal_id)
    if not plan:
        return

    tasks = roadmap_repo.list_tasks_for_skill(plan["id"], goal_skill_id)
    active_tasks = [task for task in tasks if task["is_completed"] == 0]
    if not active_tasks:
        return
    completed_tasks = [task for task in tasks if task["is_completed"] == 1]

    summary = playlist.get("summary", {}) or {}
    videos = summary.get("videos", [])
    valid_videos = [video for video in videos if isinstance(video, dict)]
    if not valid_videos:
        return

    daily_budget_minutes = _student_daily_minutes_budget(goal_id)
    chunks = _build_video_chunks(valid_videos, daily_budget_minutes)
    if not chunks:
        return

    def _format_minutes(total_minutes: int) -> str:
        hours, minutes = divmod(max(0, total_minutes), 60)
        if hours and minutes:
            return f"{hours}h {minutes}m"
        if hours:
            return f"{hours}h"
        return f"{minutes}m"

    if completed_tasks:
        latest_completed_date = max(str(task.get("task_date", "")) for task in completed_tasks)
        try:
            start_day = date.fromisoformat(latest_completed_date) + timedelta(days=1)
        except Exception:
            start_day = date.today()
        completed_offset = len(completed_tasks)
    else:
        first_task_date = active_tasks[0].get("task_date")
        try:
            start_day = date.fromisoformat(str(first_task_date))
        except Exception:
            start_day = date.today()
        completed_offset = 0

    replacement_tasks: list[dict] = []

    for idx, chunk in enumerate(chunks):
        day_index = completed_offset + idx + 1
        title = f"{skill_name}: Playlist Day {day_index}"
        assigned_videos = chunk["videos"]
        assigned_minutes = max(1, int(chunk["minutes"] or 0))
        start_video = assigned_videos[0].get("index", 1)
        end_video = assigned_videos[-1].get("index", start_video)
        schedule_line = (
            f"Watch videos {start_video}-{end_video} "
            f"({_format_minutes(assigned_minutes)} total)"
        )
        video_lines = []
        for video in assigned_videos[:6]:
            line = (
                f"- Video {video.get('index', '?')}: {video.get('title', 'Untitled')} "
                f"({_format_minutes(max(1, int(video.get('duration_minutes', 0) or 0)))})"
            )
            video_lines.append(line)
        if len(assigned_videos) > 6:
            video_lines.append(f"- Plus {len(assigned_videos) - 6} more videos")

        description = (
            f"{schedule_line} for {skill_name}.\n"
            f"Playlist: {playlist.get('title', '')}\n"
            f"Channel: {playlist.get('channel_title', '')}\n"
            f"URL: {playlist.get('playlist_url', '')}\n"
            f"Daily target based on your study capacity: {_format_minutes(daily_budget_minutes)}\n"
            f"Assigned videos:\n" + "\n".join(video_lines)
        )
        scheduled_day = start_day + timedelta(days=idx)
        replacement_tasks.append(
            {
                "goal_skill_id": goal_skill_id,
                "task_date": scheduled_day.isoformat(),
                "title": title,
                "description": description,
                "target_minutes": assigned_minutes,
            }
        )

    roadmap_repo.replace_incomplete_tasks_for_skill(plan["id"], goal_skill_id, replacement_tasks)


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


def clear_selected_playlist(goal_id: int, goal_skill_id: int) -> None:
    playlist_repo.clear_selected_recommendation(goal_id, goal_skill_id)


def get_selected_playlist(goal_id: int, goal_skill_id: int) -> dict | None:
    return playlist_repo.get_selected_recommendation(goal_id, goal_skill_id)
