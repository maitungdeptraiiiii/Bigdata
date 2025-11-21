"""
Spark Structured Streaming processor for:
 - player state updates (player:<id> in Redis)
 - realtime recent events (publish + recent_events_list)
 - global metrics (live_metrics)
 - engagement prediction using pretrained XGBoost/sklearn model

Design: Use foreachBatch handlers for simplicity and robustness.
"""

import os
import json
import time
import redis
import pickle
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# ----------------------------
# CONFIG
# ----------------------------
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "game_events")

LIVE_METRICS_KEY = os.environ.get("REDIS_KEY", "live_metrics")
RECENT_EVENTS_LIST = "recent_events_list"
RECENT_EVENTS_CHANNEL = "recent_events"
ACTIVE_PLAYERS_SET = "active_players"
PURCHASES_ZSET = "purchases_ts"   # sorted set of purchase timestamps (epoch seconds)
PLAYER_KEY_PREFIX = "player:"     # player:{id}

# Model path (inside container)
MODEL_PATH = "/app/model.pkl"

# Model features expected (match how your model was trained)
MODEL_FEATURES = [
    "PlayTimeHours",
    "InGamePurchases",
    "SessionsPerWeek",
    "AvgSessionDurationMinutes",
    "PlayerLevel",
    "AchievementsUnlocked"
]

# ----------------------------
# Redis client (used by drivers)
# ----------------------------
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# ----------------------------
# Load model
# ----------------------------
engagement_model = None
if os.path.exists(MODEL_PATH):
    with open(MODEL_PATH, "rb") as f:
        engagement_model = pickle.load(f)
        print("Loaded engagement model from", MODEL_PATH)
else:
    print("Warning: model not found at", MODEL_PATH, "- EngagementLevel will be 'Unknown'")

# ----------------------------
# Spark session
# ----------------------------
spark = SparkSession.builder.appName("GameEventsStreaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ----------------------------
# Kafka event schema
# ----------------------------
event_schema = StructType([
    StructField("event_id", StringType()),
    StructField("player_id", StringType()),      # keep as string to be safe
    StructField("event_type", StringType()),
    StructField("timestamp", StringType()),
    StructField("location", StringType()),
    StructField("device", StringType()),
    StructField("session_duration", IntegerType()),
    StructField("item", StringType()),
    StructField("amount", DoubleType()),
    StructField("new_level", IntegerType()),
    StructField("achievement", StringType()),
    # Optionally include CSV columns in events for hydrate new players
    StructField("Age", IntegerType()),
    StructField("Gender", StringType()),
    StructField("GameGenre", StringType()),
    StructField("GameDifficulty", StringType()),
])

# ----------------------------
# Read from Kafka
# ----------------------------
raw = (spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", KAFKA_SERVERS)
       .option("subscribe", KAFKA_TOPIC)
       .option("startingOffsets", "latest")
       .load())

json_df = raw.selectExpr("CAST(value AS STRING) as value")
parsed = json_df.select(from_json(col("value"), event_schema).alias("data")).select("data.*")

# timestamp column to timestamp type (supports millisecond or no-millis fallback)
parsed = parsed.withColumn(
    "timestamp_ts",
    to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
)
parsed = parsed.withColumn(
    "timestamp_ts",
    when(col("timestamp_ts").isNull(), to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    .otherwise(col("timestamp_ts"))
)

# ----------------------------
# Helper functions for Redis updates (used in foreachBatch)
# ----------------------------
def safe_float(x, default=0.0):
    try:
        return float(x) if x is not None else default
    except:
        return default

def safe_int(x, default=0):
    try:
        return int(x) if x is not None else default
    except:
        return default

def predict_engagement(state_dict):
    """
    Given player state dict, produce EngagementLevel using model if available.
    Returns string label (or "Unknown").
    """
    if engagement_model is None:
        return "Unknown"

    # prepare feature vector
    features = []
    for f in MODEL_FEATURES:
        val = state_dict.get(f)
        if val is None:
            val = 0
        features.append(val)
    try:
        pred = engagement_model.predict([features])[0]
        # if model outputs numeric index, convert to string
        return str(pred)
    except Exception as e:
        print("Engagement prediction error:", e)
        return "Unknown"

# ----------------------------
# foreachBatch: recent events (pubsub + list)
# ----------------------------
def recent_events_batch(batch_df, batch_id):
    rows = batch_df.select("player_id", "event_type", "timestamp").collect()
    if not rows:
        return
    for r in rows:
        payload = {
            "player_id": r["player_id"],
            "event_type": r["event_type"],
            "timestamp": r["timestamp"]
        }
        try:
            redis_client.publish(RECENT_EVENTS_CHANNEL, json.dumps(payload))
            # store list: newest first
            redis_client.lpush(RECENT_EVENTS_LIST, json.dumps(payload))
            redis_client.ltrim(RECENT_EVENTS_LIST, 0, 49)
        except Exception as e:
            print("recent_events_batch: redis error", e)

# ----------------------------
# foreachBatch: handle purchases zset & active players set
# ----------------------------
def aux_state_updates(events):
    """
    events: list of dict rows from microbatch
    manages:
      - ACTIVE_PLAYERS_SET add/remove
      - PURCHASES_ZSET add timestamps for purchase events
    """
    now_ts = int(time.time())
    five_min_ago = now_ts - 300

    # process events
    pipe = redis_client.pipeline()
    for ev in events:
        pid = str(ev.get("player_id"))
        etype = ev.get("event_type")
        ts = ev.get("timestamp")
        # add/remove active players
        if etype == "login" or etype == "session_start":
            pipe.sadd(ACTIVE_PLAYERS_SET, pid)
        elif etype == "logout" or etype == "session_end":
            pipe.srem(ACTIVE_PLAYERS_SET, pid)

        # purchases to ZSET (score = epoch seconds)
        if etype == "purchase":
            # try parse timestamp
            try:
                # accept ISO format
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                score = int(dt.replace(tzinfo=timezone.utc).timestamp())
            except Exception:
                score = now_ts
            # member can be JSON or just player:ts
            member = json.dumps({"player_id": pid, "ts": score})
            pipe.zadd(PURCHASES_ZSET, {member: score})

    # remove old purchases (keep last > 5 minutes)
    pipe.zremrangebyscore(PURCHASES_ZSET, 0, five_min_ago * 1.0 - 1)
    pipe.execute()

# ----------------------------
# foreachBatch: player state updates + engagement prediction
# ----------------------------
def player_state_batch(batch_df, batch_id):
    """
    For each microbatch: compute per-player aggregated updates, apply to Redis state,
    predict engagement, and persist player:<id> JSON in Redis.
    """
    rows = batch_df.collect()
    if not rows:
        return

    # Convert rows to per-player list
    players = {}
    for r in rows:
        pid = str(r["player_id"])
        players.setdefault(pid, []).append(r.asDict())

    # first update aux state (active players, purchases zset)
    all_events = [r.asDict() for r in rows]
    aux_state_updates(all_events)

    # For each player, load existing redis state (if any), update by events
    pipe = redis_client.pipeline()
    for pid, evs in players.items():
        key = PLAYER_KEY_PREFIX + pid
        raw = redis_client.get(key)
        if raw:
            try:
                state = json.loads(raw)
            except:
                state = {}
        else:
            # initialize from possible event fields or defaults
            state = {
                "PlayerID": pid,
                "Age": None,
                "Gender": None,
                "Location": None,
                "GameGenre": None,
                "PlayTimeHours": 0.0,
                "InGamePurchases": 0,
                "GameDifficulty": None,
                "SessionsPerWeek": 0,
                "AvgSessionDurationMinutes": 0,
                "PlayerLevel": 1,
                "AchievementsUnlocked": 0,
                "IsActive": False,
                "LastEventTs": None,
            }

        # apply events in chronological order (they are microbatch; assume near order)
        for ev in evs:
            et = ev.get("event_type")
            # update basic fields if event provides them (e.g., Age, Gender in event)
            for maybe in ["Age", "Gender", "Location", "GameGenre", "GameDifficulty"]:
                if ev.get(maybe) is not None:
                    state[maybe] = ev.get(maybe)

            if et == "login" or et == "session_start":
                state["IsActive"] = True
            elif et == "logout" or et == "session_end":
                state["IsActive"] = False
                # optionally compute last session duration if LastSessionStart present
                last_start = state.get("LastSessionStart")
                if last_start:
                    try:
                        dt_now = datetime.fromisoformat(ev.get("timestamp").replace("Z", "+00:00"))
                        dt_start = datetime.fromisoformat(last_start.replace("Z", "+00:00"))
                        minutes = (dt_now - dt_start).total_seconds() / 60.0
                        # update average session duration (simple exponential avg)
                        prev = safe_float(state.get("AvgSessionDurationMinutes", 0))
                        state["AvgSessionDurationMinutes"] = (prev + minutes) / 2.0 if prev > 0 else minutes
                        state["SessionsPerWeek"] = safe_int(state.get("SessionsPerWeek", 0)) + 1
                    except Exception:
                        pass
                    state["LastSessionStart"] = None
            elif et == "purchase":
                state["InGamePurchases"] = safe_int(state.get("InGamePurchases", 0)) + 1
                # also track total money? if amount present, we could add separate sum
            elif et == "level_up" or et == "levelup":
                state["PlayerLevel"] = safe_int(state.get("PlayerLevel", 1)) + (ev.get("new_level") or 1)
            elif et == "achievement_unlocked" or et == "achievement":
                state["AchievementsUnlocked"] = safe_int(state.get("AchievementsUnlocked", 0)) + 1
            elif et == "playtime_tick":
                # event.amount is minutes increment in this design
                inc_minutes = safe_float(ev.get("amount", 0.0))
                # convert to hours
                state["PlayTimeHours"] = safe_float(state.get("PlayTimeHours", 0.0)) + (inc_minutes / 60.0)

            # if event marks session start record timestamp
            if et == "session_start":
                state["LastSessionStart"] = ev.get("timestamp")

            # update last event ts
            state["LastEventTs"] = ev.get("timestamp")

        # predict engagement
        # ensure model features exist as numeric
        model_ready = {}
        for f in MODEL_FEATURES:
            model_ready[f] = safe_float(state.get(f, 0.0))

        if engagement_model:
            try:
                pred = engagement_model.predict([[model_ready[f] for f in MODEL_FEATURES]])[0]
                state["EngagementLevel"] = str(pred)
            except Exception as e:
                state["EngagementLevel"] = "Unknown"
                print("model predict error for player", pid, e)
        else:
            state["EngagementLevel"] = "Unknown"

        # write back to Redis
        try:
            pipe.set(key, json.dumps(state))
        except Exception as e:
            print("redis set error for player", pid, e)

    # execute pipeline writes
    try:
        pipe.execute()
    except Exception as e:
        print("redis pipeline execute error", e)

# ----------------------------
# foreachBatch: compute windowed metrics and write live_metrics
# ----------------------------
def metrics_batch(window_df, batch_id):
    """
    window_df: contains columns window, event_count aggregated by 10s window
    We'll compute events_per_sec, active_players, purchases_5min, open_sessions and push live_metrics
    """
    rows = window_df.collect()
    total_events = 0
    if rows:
        for r in rows:
            total_events += int(r["event_count"])

    events_per_sec = round(total_events / 10.0, 2)

    # active players count
    try:
        active_players = redis_client.scard(ACTIVE_PLAYERS_SET)
    except Exception:
        active_players = 0

    # purchases in last 5 minutes using ZSET
    now_ts = int(time.time())
    five_min_ago = now_ts - 300
    try:
        # remove expired and count
        redis_client.zremrangebyscore(PURCHASES_ZSET, 0, five_min_ago - 1)
        purchases_5min = redis_client.zcount(PURCHASES_ZSET, five_min_ago, now_ts)
    except Exception:
        purchases_5min = 0

    open_sessions = active_players

    # recent events snapshot (list)
    try:
        recent_list = redis_client.lrange(RECENT_EVENTS_LIST, 0, 49)
        recent_events = [json.loads(x) for x in recent_list]
    except Exception:
        recent_events = []

    dashboard = {
        "events_per_sec": events_per_sec,
        "active_players": int(active_players),
        "purchases_5min": int(purchases_5min),
        "open_sessions": int(open_sessions),
        "recent_events": recent_events
    }

    try:
        redis_client.set(LIVE_METRICS_KEY, json.dumps(dashboard))
        redis_client.publish("metrics", json.dumps(dashboard))
        print("Pushed to Redis live_metrics:", dashboard)
    except Exception as e:
        print("metrics_batch: redis error", e)

# ----------------------------
# Start streaming queries
# ----------------------------

# Recent events stream: push each event to recent_events (pub/sub + list)
recent_query = (
    parsed.select("player_id", "event_type", "timestamp")
    .writeStream
    .outputMode("append")
    .foreachBatch(recent_events_batch)
    .start()
)

# Player state updates: process whole microbatch and update per-player state & model
player_query = (
    parsed
    .select("player_id", "event_type", "timestamp", "location", "device", "session_duration",
            "item", "amount", "new_level", "achievement", "Age", "Gender", "GameGenre", "GameDifficulty")
    .writeStream
    .outputMode("append")
    .foreachBatch(player_state_batch)
    .start()
)

# Windowed metrics (10s) aggregator
windowed = (
    parsed
    .select("event_id", "timestamp_ts")
    .withWatermark("timestamp_ts", "30 seconds")
    .groupBy(window(col("timestamp_ts"), "10 seconds"))
    .count()
    .withColumnRenamed("count", "event_count")
)

metrics_query = (
    windowed.writeStream
    .outputMode("update")
    .foreachBatch(metrics_batch)
    .start()
)

print("All streams started.")
spark.streams.awaitAnyTermination()
