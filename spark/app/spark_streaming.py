import os
import json
import redis
import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.ml import PipelineModel

# --- CẤU HÌNH ---
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "game_events")
MODEL_PATH = "/app/cv_pipeline_model/bestModel"

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
spark_model = None

# --- MAPPING LABEL CHUẨN ---
PREDICTION_LABELS = {
    0.0: "Medium",
    1.0: "High",
    2.0: "Low"
}

def get_model():
    """Singleton để load model một lần duy nhất"""
    global spark_model
    if spark_model is None:
        try:
            print(f">>> LOADING SPARK ML MODEL FROM: {MODEL_PATH}")
            spark_model = PipelineModel.load(MODEL_PATH)
            print(">>> MODEL LOADED SUCCESSFULLY!")
        except Exception as e:
            print(f">>> ERROR LOADING MODEL: {e}")
    return spark_model

def process_batch(df, epoch_id):
    rows = df.collect()
    if not rows: return

    print(f"--- Processing Batch {epoch_id}: {len(rows)} events ---")
    affected_players = set()
    
    # ====================================================
    # PHASE 1: FAST UPDATE (Cập nhật Redis ngay lập tức)
    # ====================================================
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
                "Age": event.get('Age', 20),
                "Gender": event.get('Gender', 'Unknown'),
                "Location": event.get('Location', 'Unknown'),
                "GameGenre": event.get('GameGenre', 'Action'),
                "GameDifficulty": event.get('GameDifficulty', 'Medium'),
                "PlayerLevel": 1,
                "TotalPlayTimeMinutes": 0.0,
                "TotalSessions": 0,
                "PurchaseCount": 0,
                "AchievementsCount": 0,
                "TotalSpent": 0.0,
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
            
            # Lấy duration từ producer gửi lên
            duration = event.get('session_duration')
            if duration is None: 
                duration = random.uniform(10.0, 60.0)
            pipe.hincrbyfloat(pkey, "TotalPlayTimeMinutes", float(duration))

        # 4. Purchase
        elif etype == 'purchase':
            pipe.hincrbyfloat(pkey, "TotalSpent", event.get('amount', 0.0))
            pipe.hincrby(pkey, "PurchaseCount", 1)

        # 5. Level Up
        elif etype == 'level_up':
            pipe.hincrby(pkey, "PlayerLevel", 1)
            
        # 6. Achievement
        elif etype == 'achievement_unlocked':
            pipe.hincrby(pkey, "AchievementsCount", 1)

        # --> Log Event cho Dashboard UI
        pipe.hincrby("live_metrics", "total_events", 1)
        log_entry = json.dumps({"id": pid, "type": etype, "time": event['timestamp']})
        pipe.lpush("recent_events_list", log_entry)
        pipe.ltrim("recent_events_list", 0, 19)

    try:
        pipe.execute()
    except Exception as e:
        print(f"Phase 1 Error: {e}")

    # ====================================================
    # PHASE 2: AI PREDICTION & HYBRID LOGIC
    # ====================================================
    
    model = get_model()
    if not model or not affected_players: return

    feature_rows = []
    spark = SparkSession.builder.getOrCreate()
    pipe_update = redis_client.pipeline()

    for pid in affected_players:
        p_data = redis_client.hgetall(f"player:{pid}")
        if not p_data: continue

        try:
            # A. Tính toán Metrics
            total_time = float(p_data.get("TotalPlayTimeMinutes", 0))
            total_sessions = int(p_data.get("TotalSessions", 0))
            
            # --- LOGIC 1: COLD START ---
            # Người mới chưa chơi -> Gán cứng là "New" (hoặc "Low")
            if total_time < 1.0:
                pipe_update.hset(f"player:{pid}", "EngagementLevel", "New")
                continue # Bỏ qua, không cần AI đoán
            
            # Fix lỗi logic nếu có (Time > 0 mà Session = 0)
            if total_time > 0 and total_sessions == 0:
                total_sessions = 1
                redis_client.hset(f"player:{pid}", "TotalSessions", 1)

            avg_duration = total_time / total_sessions if total_sessions > 0 else 0.0
            
            # Update lại Avg vào Redis để UI hiển thị
            redis_client.hset(f"player:{pid}", "AvgSessionDurationMinutes", avg_duration)

            # B. Feature Engineering (Tạo cột thủ công cho Model)
            game_difficulty = p_data.get("GameDifficulty", "Medium")
            
            # 1. Tạo IsStressed
            is_stressed = "true" if game_difficulty == "Hard" else "false"
            
            # 2. Tạo GameDifficultyQuantified
            diff_quantified = 4 # Medium
            if game_difficulty == "Easy": diff_quantified = 1
            elif game_difficulty == "Hard": diff_quantified = 8

            # Tạo Row chuẩn Input Model
            row = {
                "PlayerID": pid,
                "Age": int(p_data.get("Age", 20)),
                "Gender": p_data.get("Gender", "Unknown"),
                "Location": p_data.get("Location", "Unknown"),
                "GameGenre": p_data.get("GameGenre", "Action"),
                "InGamePurchases": int(p_data.get("PurchaseCount", 0)),
                "SessionsPerWeek": float(total_sessions),
                "AvgSessionDurationMinutes": float(avg_duration),
                "PlayerLevel": int(p_data.get("PlayerLevel", 1)),
                "AchievementsUnlocked": int(p_data.get("AchievementsCount", 0)),
                
                # Các cột phái sinh bắt buộc
                "IsStressed": is_stressed,
                "GameDifficultyQuantified": int(diff_quantified)
            }
            feature_rows.append(row)
            
        except ValueError: continue

    # Thực thi update cho nhóm Newbie trước
    pipe_update.execute()

    # Dự đoán cho nhóm Active
    if not feature_rows: return

    try:
        pred_df = spark.createDataFrame(feature_rows)
        
        # Transform: X -> Model -> Prediction Index
        predictions = model.transform(pred_df)
        
        # Lấy kết quả
        results = predictions.select("PlayerID", "prediction").collect()

        pipe_p2 = redis_client.pipeline()
        for res in results:
            pid_res = res["PlayerID"]
            pred_idx = res["prediction"]
            
            # Map Index -> Label (0->Medium, 1->High, 2->Low)
            label = PREDICTION_LABELS.get(pred_idx, "Unknown")
            
            pipe_p2.hset(f"player:{pid_res}", "EngagementLevel", label)
            
        pipe_p2.execute()
        
    except Exception as e:
        print(f"Phase 2 Prediction Error: {e}")

def main():
    spark = SparkSession.builder \
        .appName("GameAnalyticsFinal") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # Schema Kafka đầu vào
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("player_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("Age", IntegerType(), True),
        StructField("Gender", StringType(), True),
        StructField("Location", StringType(), True),
        StructField("GameDifficulty", StringType(), True),
        StructField("GameGenre", StringType(), True),
        StructField("new_level", IntegerType(), True),
        StructField("amount", DoubleType(), True),
        StructField("session_duration", DoubleType(), True)
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

    # Load model sớm để cache
    get_model()

    query = parsed_stream.writeStream \
        .foreachBatch(process_batch) \
        .trigger(processingTime='5 seconds') \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()