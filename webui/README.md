Simple Web UI (Flask + SSE) for realtime metrics.

Run locally (requires Redis running):

1. Start Redis (docker):
   docker run -d --name redis -p 6379:6379 redis:7-alpine

2. Start the webui backend:
   cd webui/backend
   pip install -r requirements.txt
   python app.py

3. Open http://localhost:5000

Integration notes:
- Spark Streaming job should compute metrics periodically and write JSON to Redis key (default: `live_metrics`).
- JSON schema example:
{
  "events_per_sec": 12,
  "active_players": 7,
  "purchases_5min": 2,
  "open_sessions": 4,
  "recent_events": [ {"timestamp":"...","player_id":9001,"event_type":"Purchase","game_genre":"Action","metadata":{}} ]
}
