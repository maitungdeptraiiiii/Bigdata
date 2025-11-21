import os
import json
import redis
import joblib
import pandas as pd
import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# --- CONFIG ---
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "game_events")
MODEL_PATH = "/data/engagement_model.pkl" 
LABEL_PATH = "/data/label_encoder.pkl"

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# --- LOAD AI MODEL ---
ml_pipeline = None
label_encoder = None
try:
    if os.path.exists(MODEL_PATH):
        ml_pipeline = joblib.load(MODEL_PATH)
        label_encoder = joblib.load(LABEL_PATH)
        print(">>> AI MODEL LOADED SUCCESSFULLY")
    else:
        print(f">>> WARNING: Model not found at {MODEL_PATH}")
except Exception as e:
    print(f">>> ERROR LOADING MODEL: {e}")

def predict_engagement(player_data):
    """Dự đoán độ nghiện dựa trên dữ liệu hiện tại"""
    if not ml_pipeline: return "Unknown"
    try:
        features = {
            'Age': int(player_data.get('Age', 20)),
            'Gender': player_data.get('Gender', 'Male'),
            'GameDifficulty': player_data.get('GameDifficulty', 'Medium'),
            'SessionsPerWeek': float(player_data.get('SessionsPerWeek', 0)),
            'AvgSessionDurationMinutes': float(player_data.get('AvgSessionDurationMinutes', 0)),
            'PlayerLevel': int(player_data.get('PlayerLevel', 1))
        }
        input_df = pd.DataFrame([features])
        pred_idx = ml_pipeline.predict(input_df)[0]
        return label_encoder.inverse_transform([pred_idx])[0]
    except Exception as e:
        # print(f"Prediction Error: {e}")
        return "Error"

def process_batch(df, epoch_id):
    rows = df.collect()
    if not rows: return

    print(f"--- Processing Batch {epoch_id}: {len(rows)} events ---")
    
    # Danh sách các user cần tính toán lại chỉ số
    affected_players = set()
    
    # === PHASE 1: CẬP NHẬT SỐ LIỆU THÔ (COUNTERS) ===
    pipe = redis_client.pipeline()
    
    for row in rows:
        event = row.asDict()
        pid = event.get('player_id')
        etype = event.get('event_type')
        
        if not pid or not etype: continue
        
        affected_players.add(pid)
        pkey = f"player:{pid}"
        
        # 1. Register
        if etype == 'register':
            data = {
                "Age": event.get('Age') or 0,
                "Gender": event.get('Gender') or 'Unknown',
                "Location": event.get('Location') or 'Unknown',
                "GameDifficulty": event.get('GameDifficulty') or 'Medium',
                "PlayerLevel": 1,
                "TotalSessions": 0,
                "TotalPlayTimeMinutes": 0,
                "TotalSpent": 0, # Thêm trường này
                "status": "offline",
                "EngagementLevel": "New"
            }
            clean = {k: v for k, v in data.items() if v is not None}
            pipe.hset(pkey, mapping=clean)
            pipe.sadd("all_player_ids", pid)

        # 2. Login
        elif etype == 'login':
            pipe.hset(pkey, "status", "online")
            pipe.hset(pkey, "last_seen", event['timestamp'])
            pipe.hincrby(pkey, "TotalSessions", 1)

        # 3. Logout
        elif etype == 'logout':
            pipe.hset(pkey, "status", "offline")
            pipe.hset(pkey, "last_seen", event['timestamp'])
            # Giả lập thời gian chơi session này (random 10-40 phút)
            session_time = random.uniform(10.5, 40.0)
            pipe.hincrbyfloat(pkey, "TotalPlayTimeMinutes", session_time)

        # 4. Purchase (MỚI THÊM)
        elif etype == 'purchase':
            amount = event.get('amount', 0.0)
            if amount:
                pipe.hincrbyfloat(pkey, "TotalSpent", amount)
            pipe.hincrby(pkey, "PurchaseCount", 1)

        # 5. Level Up
        elif etype == 'level_up':
            pipe.hincrby(pkey, "PlayerLevel", 1)

        # Log sự kiện
        pipe.hincrby("live_metrics", "total_events", 1)
        log_entry = json.dumps({"id": pid, "type": etype, "time": event['timestamp']})
        pipe.lpush("recent_events_list", log_entry)
        pipe.ltrim("recent_events_list", 0, 19)

    # Thực thi Phase 1
    try:
        pipe.execute()
    except Exception as e:
        print(f"Pipeline Error: {e}")

    # === PHASE 2: TÍNH TOÁN CHỈ SỐ PHÁI SINH (AVG, AI) ===
    # (Đọc lại data đã update để tính toán chính xác)
    
    for pid in affected_players:
        pkey = f"player:{pid}"
        try:
            # Lấy toàn bộ data mới nhất
            current_data = redis_client.hgetall(pkey)
            if not current_data: continue

            updates = {}

            # A. Tính lại AvgSessionDurationMinutes
            # Công thức: Tổng thời gian / Tổng số session
            total_time = float(current_data.get("TotalPlayTimeMinutes", 0))
            total_sessions = int(current_data.get("TotalSessions", 1)) # Tránh chia cho 0
            if total_sessions < 1: total_sessions = 1
            
            new_avg = total_time / total_sessions
            updates["AvgSessionDurationMinutes"] = new_avg

            # B. Chạy AI Model dự đoán lại Engagement
            # Cập nhật metrics tạm vào dictionary để model dùng số liệu mới nhất
            current_data["AvgSessionDurationMinutes"] = new_avg
            
            predicted_engagement = predict_engagement(current_data)
            updates["EngagementLevel"] = predicted_engagement

            # C. Ghi đè các chỉ số đã tính toán lại vào Redis
            redis_client.hset(pkey, mapping=updates)
            
            # Debug log cho user thấy
            # print(f"Updated {pid}: AvgSession={new_avg:.1f}m | Spent=${current_data.get('TotalSpent', 0)} | AI={predicted_engagement}")

        except Exception as e:
            print(f"Phase 2 Error for {pid}: {e}")

def main():
    spark = SparkSession.builder.appName("GameAnalyticsFixed").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Cập nhật Schema để nhận diện trường amount của purchase
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("player_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("Age", IntegerType(), True),
        StructField("Gender", StringType(), True),
        StructField("Location", StringType(), True),
        StructField("GameDifficulty", StringType(), True),
        StructField("new_level", IntegerType(), True),
        StructField("amount", DoubleType(), True) # <-- Quan trọng cho purchase
    ])

    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    parsed_stream = raw_stream.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    query = parsed_stream.writeStream \
        .foreachBatch(process_batch) \
        .trigger(processingTime='2 seconds') \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()