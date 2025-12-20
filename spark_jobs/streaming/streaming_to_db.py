import sys
import os
import traceback
from pathlib import Path
import json
from pymongo import MongoClient

# ============================================================
# Project import setup
# ============================================================
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, LongType, TimestampType
)
from pyspark.sql.functions import col, struct

# ============================================================
# Spark session (reuse YOUR config)
# ============================================================
if os.getenv("KUBERNETES_SERVICE_HOST"):
    print("🚀 Running in Kubernetes environment")
    from spark_jobs.utils.spark_session_k8s import create_spark_session_with_mongo
else:
    print("💻 Running in local environment")
    from spark_jobs.utils.spark_session import create_spark_session_with_mongo


# ============================================================
# MongoDB connection test
# ============================================================
def test_mongodb_connection(mongo_uri, database):
    try:
        print("\n=== Testing MongoDB Connection ===")
        client = MongoClient(mongo_uri)
        client.admin.command("ping")
        print("✅ MongoDB connected")
        print(f"   Database: {database}")
        print("=" * 80)
        return True
    except ConnectionFailure:
        print("❌ Cannot connect to MongoDB")
        return False
    except Exception as e:
        print(f"❌ MongoDB error: {e}")
        return False


# ============================================================
# Write batch to MongoDB (DEBUG VERSION)
# ============================================================
def write_batch_to_mongo(batch_df, batch_id):
    print(f"\n{'='*30} [Batch {batch_id}] START {'='*30}")

    if batch_df is None:
        print("[DEBUG] batch_df is None ❌")
        return

    row_count = batch_df.count()
    print(f"[DEBUG] Row count = {row_count}")

    if row_count == 0:
        print(f"[Batch {batch_id}] Empty batch → skip")
        return

    print("[DEBUG] Batch schema:")
    batch_df.printSchema()

    print("[DEBUG] Sample rows:")
    batch_df.show(5, truncate=False)

    print("[DEBUG] NULL statistics:")
    batch_df.select(
        col("pid").isNull().alias("pid_null"),
        col("track_uri").isNull().alias("track_uri_null"),
        col("event_count").isNull().alias("event_count_null"),
        col("window.start").isNull().alias("window_start_null")
    ).groupBy().sum().show()

    final_df = (
        batch_df
        .select(
            col("pid"),
            col("track_uri"),
            col("event_count"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end")
        )
        .withColumn(
            "_id",
            struct(
                col("pid"),
                col("track_uri"),
                col("window_start")
            )
        )
    )

    print("[DEBUG] Final DF preview:")
    final_df.show(5, truncate=False)


    rows = final_df.collect()  # KHÔNG JSON

    client = MongoClient(MONGO_URI)
    collection = client[MONGO_DB][COLLECTION_NAME]

    for row in rows:
        doc = row.asDict(recursive=True)  # Row -> dict (nested OK)
        collection.replace_one(
            {"_id": doc["_id"]},
            doc,
            upsert=True
        )


# ============================================================
# MAIN JOB
# ============================================================
def run_features_5m_to_mongo_debug():
    global MONGO_URI, MONGO_DB, COLLECTION_NAME

    spark = None

    try:
        print("=" * 80)
        print(" START STRUCTURED STREAMING FEATURES (5m) → MONGODB [DEBUG]")
        print("   Press Ctrl+C to stop")
        print("=" * 80)

        # ----------------------------------------------------
        # 1. Spark + Mongo config
        # ----------------------------------------------------
        spark, MONGO_URI, MONGO_DB = create_spark_session_with_mongo()

        if not test_mongodb_connection(MONGO_URI, MONGO_DB):
            raise RuntimeError("MongoDB connection failed")

        # ----------------------------------------------------
        # 2. Paths
        # ----------------------------------------------------
        INPUT_PATH = "s3a://spotify-processed-data/features_5m"

        CHECKPOINT_PATH = (
            "s3a://spotify-streaming-checkpoint/"
            "features_5m_to_mongo_DEBUG"
        )

        COLLECTION_NAME = "playlist_track_features_5m"

        print(f" Streaming source : {INPUT_PATH}")
        print(f" Checkpoint path  : {CHECKPOINT_PATH}")
        print(f" Mongo collection: {COLLECTION_NAME}")

        # ----------------------------------------------------
        # 3. OFFLINE PARQUET DEBUG (CỰC KỲ QUAN TRỌNG)
        # ----------------------------------------------------
        print("\n[DEBUG] OFFLINE PARQUET READ")
        offline_df = spark.read.parquet(INPUT_PATH)

        print("[DEBUG] Offline schema:")
        offline_df.printSchema()

        offline_count = offline_df.count()
        print(f"[DEBUG] Offline row count = {offline_count}")

        if offline_count > 0:
            offline_df.show(5, truncate=False)
        else:
            print(" OFFLINE PARQUET EMPTY → STREAMING WILL NEVER WORK")

        # ----------------------------------------------------
        # 4. Streaming schema
        # ----------------------------------------------------
        features_5m_schema = StructType([
            StructField(
                "window",
                StructType([
                    StructField("start", TimestampType(), True),
                    StructField("end", TimestampType(), True),
                ]),
                True
            ),
            StructField("pid", IntegerType(), True),
            StructField("track_uri", StringType(), True),
            StructField("event_count", LongType(), True),
        ])

        # ----------------------------------------------------
        # 5. Read stream
        # ----------------------------------------------------
        stream_df = (
            spark.readStream
            .schema(features_5m_schema)
            .format("parquet")
            .option("maxFilesPerTrigger", 5)
            .load(INPUT_PATH)
        )

        print("\n[DEBUG] STREAM SCHEMA:")
        stream_df.printSchema()

        # ----------------------------------------------------
        # 6. Write stream (NO FILTER, NO DROP)
        # ----------------------------------------------------
        query = (
            stream_df.writeStream
            .foreachBatch(write_batch_to_mongo)
            .option("checkpointLocation", CHECKPOINT_PATH)
            .outputMode("update")
            .trigger(processingTime="30 seconds")
            .start()
        )

        print("\n Streaming query started")
        query.awaitTermination()

    except KeyboardInterrupt:
        print("\n" + "=" * 80)
        print(" CTRL+C RECEIVED → STOPPING STREAMING JOB")
        print("=" * 80)

    except Exception as e:
        print("\n" + "=" * 80)
        print(" JOB FAILED")
        print("=" * 80)
        print(f"Error: {e}")
        print("\nTraceback:")
        traceback.print_exc()
        print("=" * 80)
        raise

    finally:
        if spark:
            print("\n Stopping Spark session...")
            spark.stop()
            print(" Spark stopped")


# ============================================================
# ENTRYPOINT
# ============================================================
if __name__ == "__main__":
    run_features_5m_to_mongo_debug()
