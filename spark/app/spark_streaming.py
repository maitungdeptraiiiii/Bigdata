from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count
from pyspark.sql.types import *
import redis
import json

# Redis client
redis_client = redis.Redis(host="redis", port=6379, db=0)

# Spark Session
spark = (SparkSession.builder
         .appName("GameEventsStreaming")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# Schema for Kafka JSON
event_schema = StructType([
    StructField("event_id", StringType()),
    StructField("player_id", IntegerType()),
    StructField("event_type", StringType()),
    StructField("game_genre", StringType()),
    StructField("value", DoubleType()),
    StructField("timestamp", TimestampType())
])

# Read stream from Kafka
raw_df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "game_events")
    .option("startingOffsets", "latest")
    .load())

json_df = raw_df.selectExpr("CAST(value AS STRING)")
parsed_df = json_df.select(from_json(col("value"), event_schema).alias("data")).select("data.*")

# Example metric: count events in 10s window
metric_df = (parsed_df
    .groupBy(window(col("timestamp"), "10 seconds"))
    .agg(count("event_id").alias("event_count")))

# Function to write metrics to Redis
def write_to_redis(batch_df, batch_id):
    rows = batch_df.collect()
    for r in rows:
        payload = {
            "window_start": str(r["window"].start),
            "window_end": str(r["window"].end),
            "event_count": r["event_count"]
        }
        redis_client.publish("metrics", json.dumps(payload))
        redis_client.set("latest_metrics", json.dumps(payload))
        print("Pushed to Redis:", payload)

# Start Streaming Query
query = (metric_df.writeStream
         .outputMode("update")
         .foreachBatch(write_to_redis)
         .start())

query.awaitTermination()
