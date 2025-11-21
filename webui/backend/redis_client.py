import redis
import json
import os
from datetime import datetime, timedelta

REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)

# ─────────────────────────────────────────────────────────────
# SAVE RECENT EVENTS (for realtime UI)
# ─────────────────────────────────────────────────────────────
def push_recent_event(event: dict):
    """
    Store last 100 events in a Redis list.
    """
    redis_client.lpush("recent_events", json.dumps(event))
    redis_client.ltrim("recent_events", 0, 99)


def get_recent_events(limit=30):
    raw = redis_client.lrange("recent_events", 0, limit - 1)
    return [json.loads(x) for x in raw]


# ─────────────────────────────────────────────────────────────
# SAVE PER-PLAYER LIVE STATE
# ─────────────────────────────────────────────────────────────
def update_player_state(player_id: int, updates: dict):
    """
    Save / update fields for player.
    """
    key = f"player:{player_id}"
    redis_client.hset(key, mapping=updates)


def get_player_state(player_id: int):
    key = f"player:{player_id}"
    data = redis_client.hgetall(key)
    if not data:
        return {}

    decoded = {k.decode(): _safe(k.decode(), v.decode()) for k, v in data.items()}
    return decoded


def _safe(k, v):
    """
    Convert string back to int/float when appropriate.
    """
    try:
        if k in ["PlayerID", "PlayTimeHours", "SessionsPerWeek",
                 "AvgSessionDurationMinutes", "PlayerLevel",
                 "AchievementsUnlocked", "InGamePurchases"]:
            return float(v) if "." in v else int(v)
    except:
        pass
    return v


# ─────────────────────────────────────────────────────────────
# LIVE METRICS
# ─────────────────────────────────────────────────────────────
def get_live_metrics():
    raw = redis_client.get("live_metrics")
    if not raw:
        return {}
    return json.loads(raw)