import redis
import os
import json

REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def write_metrics(key, metrics_dict):
    """Write metrics JSON into Redis key."""
    r.set(key, json.dumps(metrics_dict))


def read_metrics(key):
    raw = r.get(key)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None
