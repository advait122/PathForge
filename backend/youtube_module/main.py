# main.py

from youtube_client import get_video_titles
from llm_explainer.explain_playlists import get_or_generate_explanation
from llm_explainer.qna import start_playlist_chatbot
from youtube_client import (
    search_playlists,
    get_videos_in_playlist,
    get_video_statistics,
)
from ranking import aggregate_playlist_stats, rank_playlists


def main():
    # -----------------------------
    # STEP 1: USER INPUT
    # -----------------------------
    skill_query = input("Enter skill/topic you want to learn: ").strip()

    if not skill_query:
        print("Skill cannot be empty.")
        return

    print(f"\nSearching YouTube for: {skill_query}\n")

    # -----------------------------
    # STEP 2: Search playlists
    # -----------------------------
    playlists = search_playlists(skill_query)

    if not playlists:
        print("No playlists found.")
        return

    # -----------------------------
    # STEP 3: Collect all video IDs
    # -----------------------------
    playlist_video_map = {}
    all_video_ids = set()

    for playlist in playlists:
        playlist_id = playlist["playlist_id"]
        video_ids = get_videos_in_playlist(playlist_id, max_videos=200)

        # Fetch titles for first 10 videos only
        video_titles_map = get_video_titles(video_ids[:10])
        playlist["top_video_titles"] = list(video_titles_map.values())

        playlist_video_map[playlist_id] = video_ids
        all_video_ids.update(video_ids)

    # -----------------------------
    # STEP 4: Fetch stats for ALL videos (once)
    # -----------------------------
    print(f"Fetching statistics for {len(all_video_ids)} videos...\n")
    video_stats_map = get_video_statistics(list(all_video_ids))

    # -----------------------------
    # STEP 5: Aggregate stats per playlist
    # -----------------------------
    for playlist in playlists:
        playlist_id = playlist["playlist_id"]
        video_ids = playlist_video_map.get(playlist_id, [])

        aggregated_stats = aggregate_playlist_stats(
            video_ids, video_stats_map
        )

        playlist.update(aggregated_stats)

    # -----------------------------
    # STEP 6: Rank playlists
    # -----------------------------
    ranked_playlists = rank_playlists(playlists)
    top_playlists = ranked_playlists[:3]

    # -----------------------------
    # STEP 7: Print results
    # -----------------------------
    print("\nTop 3 Playlists Based on Engagement:\n")

    for idx, playlist in enumerate(top_playlists, start=1):
        explanation = get_or_generate_explanation(playlist)

        print("====================================================")
        print(f"{idx}. {playlist['title']}")
        print(f"Channel: {playlist['channel_title']}")
        print(f"URL: https://www.youtube.com/playlist?list={playlist['playlist_id']}")
        print("----------------------------------------------------\n")

        print("TOPIC OVERVIEW:\n")
        print(explanation["topic_overview"], "\n")

        print("LEARNING EXPERIENCE:\n")
        print(explanation["learning_experience"], "\n")

        print("TOPICS COVERED SUMMARY:\n")
        print(explanation["topics_covered_summary"], "\n")

        print("====================================================\n")

    # -----------------------------
    # STEP 8: MODE 2 — CHATBOT Q&A
    # -----------------------------
    print("\nWould you like to ask questions about any playlist?")
    print("Enter playlist number (1-3) or press Enter to skip:")

    choice = input("> ").strip()

    if choice in ["1", "2", "3"]:
        selected_index = int(choice) - 1
        selected_playlist = top_playlists[selected_index]

        # Load cached summary (Mode 1 output)
        playlist_summary = get_or_generate_explanation(selected_playlist)

        # Start multi-turn chatbot (Mode 2)
        start_playlist_chatbot(
            selected_playlist,
            playlist_summary,
        )
    else:
        print("\nSkipping Q&A.\n")


if __name__ == "__main__":
    main()