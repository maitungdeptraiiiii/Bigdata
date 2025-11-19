from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, to_timestamp
from pyspark.sql.types import *
import redis
import json
import os

REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_KEY = os.environ.get("REDIS_KEY", "live_metrics")

# Redis client
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)

# Spark Session
spark = (SparkSession.builder
         .appName("GameEventsStreaming")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# Schema for Kafka JSON (timestamp as string -> convert later)
event_schema = StructType([
    StructField("event_id", StringType()),
    StructField("player_id", IntegerType()),
    StructField("event_type", StringType()),
    StructField("location", StringType()),
    StructField("device", StringType()),
    StructField("session_duration", IntegerType()),
    StructField("item", StringType()),
    StructField("amount", DoubleType()),
    StructField("new_level", IntegerType()),
    StructField("achievement", StringType()),
    StructField("timestamp", StringType())
])

# Read stream from Kafka
raw_df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "game_events")
    .option("startingOffsets", "latest")
    .load())

json_df = raw_df.selectExpr("CAST(value AS STRING) as value")
parsed = json_df.select(from_json(col("value"), event_schema).alias("data")).select("data.*")

# convert timestamp string to timestamp type (assumes ISO millis e.g. 2023-01-01T00:00:00.123Z)
parsed_df = parsed.withColumn("timestamp_ts", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))

# fallback: if milliseconds not present, try without .SSS
from pyspark.sql.functions import when
parsed_df = parsed_df.withColumn(
    "timestamp_ts",
    when(col("timestamp_ts").isNull(), to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    .otherwise(col("timestamp_ts"))
)

# Example metric: count events in 10s window
metric_df = (parsed_df
    .withWatermark("timestamp_ts", "30 seconds")
    .groupBy(window(col("timestamp_ts"), "10 seconds"))
    .agg(count("event_id").alias("event_count")))

# Function to write metrics to Redis
def write_to_redis(batch_df, batch_id):
    rows = batch_df.collect()
    for r in rows:
        payload = {
            "window_start": str(r["window"].start),
            "window_end": str(r["window"].end),
            "event_count": int(r["event_count"])
        }
        # push both publish and set for webui
        try:
            redis_client.publish("metrics", json.dumps(payload))
            redis_client.set(REDIS_KEY, json.dumps(payload))
            print("Pushed to Redis:", payload)
        except Exception as e:
            print("Redis write error:", e)

# Start Streaming Query
query = (metric_df.writeStream
         .outputMode("update")
         .foreachBatch(write_to_redis)
         .start())

query.awaitTermination()
