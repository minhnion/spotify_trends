import sys
from pathlib import Path

# =========================
# Project import setup
# =========================
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from spark_jobs.utils.spark_session import create_spark_session

from pyspark.sql.functions import (
    from_json, col, to_timestamp,
    window, count, min, max,lower, trim
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType
)


# ============================================================
# MAIN STREAMING JOB
# ============================================================
def run_streaming_job():
    spark = create_spark_session(app_name="SpotifyStreamingWindows")

    spark.sparkContext.setLogLevel("WARN")

    # ========================================================
    # 1. Read Kafka
    # ========================================================
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "playlist_events")
        .option("startingOffsets", "latest")
        .load()
    )

    # ========================================================
    # 2. Event schema
    # ========================================================
    event_schema = StructType([
        StructField("event_type", StringType(), True),
        StructField("pid", IntegerType(), True),
        StructField("track_uri", StringType(), True),
        StructField("playlist_name", StringType(), True),
        StructField("event_ts", StringType(), True)
    ])

    events_raw = (
        kafka_df
        .select(from_json(col("value").cast("string"), event_schema).alias("data"))
        .select("data.*")
    )

    # ========================================================
    # 3. Normalize timestamp
    # ========================================================
    events_ts = (
        events_raw
        .withColumn(
            "event_ts",
            to_timestamp(col("event_ts"))
        )
        .filter(col("event_ts").isNotNull())
    )

    
     # ========================================================
    # 4. CLEANING STEP 1: Drop invalid events
    # ========================================================
    events_clean = (
        events_ts
        .filter(col("event_ts").isNotNull())
        .filter(col("pid").isNotNull())
        .filter(col("track_uri").isNotNull())
        .filter(col("event_type").isNotNull())
    )

    # ========================================================
    # 5. CLEANING STEP 2: Normalize text
    # ========================================================
    events_clean = (
        events_clean
        .withColumn("playlist_name", trim(lower(col("playlist_name"))))
    )

    # ========================================================
    # 6. CLEANING STEP 3: Domain filter
    # ========================================================
    add_track_events = events_clean.filter(col("event_type") == "add_track")

    # ========================================================
    # 7. CLEANING STEP 4: Deduplication (IMPORTANT)
    # ========================================================
    add_track_events = (
        add_track_events
        .withWatermark("event_ts", "10 minutes")
        .dropDuplicates([
            "pid", "track_uri", "event_ts", "event_type"
        ])
    )


    # ========================================================
    # 5. WINDOW 5 MINUTES (REALTIME)
    # ========================================================
    df_5m = (
        add_track_events
        .groupBy(
            window(col("event_ts"), "5 minutes"),
            col("pid"),
            col("track_uri")
        )
        .agg(
            count("*").alias("event_count")
        )
    )

    # ========================================================
    # 6. WINDOW 1 HOUR (ONLINE FEATURE)
    # ========================================================
    """df_1h = (
        add_track_events
        .withWatermark("event_ts", "2 hours")
        .groupBy(
            window(col("event_ts"), "1 hour"),
            col("pid"),
            col("playlist_name"),
            col("track_uri")
        )
        .agg(
            count("*").alias("event_count"),
            max("event_ts").alias("last_seen_ts")
        )
    )"""

    # ========================================================
    # 7. WINDOW 1 DAY (BATCH-LIKE)
    # ========================================================
    """df_1d = (
        add_track_events
        .withWatermark("event_ts", "2 days")
        .groupBy(
            window(col("event_ts"), "1 day"),
            col("pid"),
            col("playlist_name"),
            col("track_uri")
        )
        .agg(
            count("*").alias("event_count"),
            min("event_ts").alias("first_seen_ts"),
            max("event_ts").alias("last_seen_ts")
        )
    )"""

    # ========================================================
    # 8. WRITE STREAMS TO MINIO
    # ========================================================
    base_output = "s3a://spotify-processed-data"
    base_ckpt = "s3a://spotify-streaming-checkpoint"

    # ---- 5 minutes ----
    q_5m = (
        df_5m.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", f"{base_output}/features_5m")
        .option("checkpointLocation", f"{base_ckpt}/features_5m")
        .start()
    )

    # ---- 1 hour ----
    """q_1h = (
        df_1h.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", f"{base_output}/features_1h")
        .option("checkpointLocation", f"{base_ckpt}/features_1h")
        .start()
    )"""

    # ---- 1 day ----
    """q_1d = (
        df_1d.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", f"{base_output}/spotify_tracks_daily")
        .option("checkpointLocation", f"{base_ckpt}/spotify_tracks_daily")
        .start()
    )"""

    print(" Spotify Streaming ETL started")
    print("✓ 5m  -> features_5m")
    #print("✓ 1h  -> features_1h")
    #print("✓ 1d  -> spotify_tracks_daily")
    print("Press Ctrl+C to stop")

    spark.streams.awaitAnyTermination()


# ============================================================
# ENTRYPOINT
# ============================================================
if __name__ == "__main__":
    run_streaming_job()
