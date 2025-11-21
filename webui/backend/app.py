from flask import Flask, jsonify, request
from redis_client import (
    get_live_metrics,
    get_recent_events,
    get_player_state,
    update_player_state,
    load_player_profiles,
)

app = Flask(__name__)

# Load static player info
PROFILES = load_player_profiles()

# ────────────────────────────────────────────────
# API: LIVE METRICS FOR DASHBOARD
# ────────────────────────────────────────────────
@app.route("/api/live_metrics")
def api_live_metrics():
    return jsonify(get_live_metrics())


# ────────────────────────────────────────────────
# API: RECENT EVENTS
# ────────────────────────────────────────────────
@app.route("/api/recent_events")
def api_recent_events():
    events = get_recent_events(30)
    return jsonify(events)


# ────────────────────────────────────────────────
# API: PLAYER LIST
# ────────────────────────────────────────────────
@app.route("/api/players")
def api_players():
    ids = list(PROFILES.keys())
    return jsonify(ids)


# ────────────────────────────────────────────────
# API: PLAYER DETAILS (full fusion)
# ────────────────────────────────────────────────
@app.route("/api/player/<pid>")
def api_player(pid):
    pid = int(pid)

    if pid not in PROFILES:
        return jsonify({"error": "Player not found"}), 404

    base = PROFILES[pid]
    live = get_player_state(pid)

    merged = base.copy()
    merged.update(live)

    # Determine Active state
    last_ts = live.get("last_event_ts")
    if last_ts:
        from datetime import datetime, timedelta
        last = datetime.fromisoformat(last_ts.replace("Z", "+00:00"))
        merged["active"] = (datetime.utcnow() - last) < timedelta(seconds=30)
    else:
        merged["active"] = False

    # Engagement is predicted via spark streaming + XGBoost model (no need to update here)

    return jsonify(merged)


# ────────────────────────────────────────────────
# HOME
# ────────────────────────────────────────────────
@app.route("/")
def index():
    return jsonify({"status": "ok", "message": "WebUI backend running"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=9000, debug=False)
