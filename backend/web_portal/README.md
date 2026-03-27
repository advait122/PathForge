# Web Portal (MVP)

Run from project root:

```powershell
uvicorn backend.web_portal.main:app --reload
```

Open:

```text
http://127.0.0.1:8000/onboarding
```

Flow:

1. Enter profile + current skills + goal text + timeline.
2. Dashboard auto-builds roadmap tasks in skill order.
3. For the current skill only, get top 3 YouTube playlist options and select one.
4. Playlist cards show title, channel, channel URL, playlist URL, topic overview, learning experience, and topics covered.
5. Daily tasks for that skill are annotated with allotted playlist video ranges.
6. Track daily task completion with checkbox completion.
7. Take end-of-skill test after tasks complete. Test generation uses selected playlist metadata + summaries.
8. If a test fails, weak-topic revision tasks are auto-added before retest.
9. After pass, next skill unlocks with fresh top 3 playlist options.
10. Use the playlist doubt chatbot anytime for the active skill after selecting a playlist.
11. Monitor opportunity tabs/notifications and 7-day eligibility forecast.
12. If tasks are missed, roadmap auto-replans to keep progress aligned to goal end date.
