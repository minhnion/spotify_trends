from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, sum as spark_sum, min, max
)
from pyspark.sql.functions import  lit
import sys
from pathlib import Path
import os

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / ".env")

if os.getenv("KUBERNETES_SERVICE_HOST"):
    print("🚀 Running in Kubernetes environment")
    from spark_jobs.utils.spark_session_k8s import create_spark_session
else:
    print("💻 Running in local environment")
    from spark_jobs.utils.spark_session import create_spark_session

from spark_jobs.utils.s3_utils import ensure_s3_bucket_exists

# Ensure S3 bucket exists
ensure_s3_bucket_exists("s3a://spotify-processed-data")

spark = create_spark_session(app_name="SpotifyStreamingWindows")

spark.sparkContext.setLogLevel("WARN")

# ======================================================
# 1. Read streaming output
# ======================================================
df = spark.read.parquet(
    "s3a://spotify-processed-data/features_5m"
)

# ======================================================
# 2. Extract date from window.start
# ======================================================
df = df.withColumn(
    "hour",
    to_date(col("window.start"))
)

# ======================================================
# 3. Aggregate to DAILY level (TRAINING READY)
# ======================================================
daily_df = (
    df.groupBy(
        "hour",
        "pid",
        "track_uri"
    )
    .agg(
        spark_sum("event_count").alias("daily_event_count"),
        min("window.start").alias("first_seen_ts"),
        max("window.end").alias("last_seen_ts")
    )
    .withColumn("pid", col("pid").cast("long")) # Đồng nhất bigint
    # Thêm các cột metadata trống để khớp schema Batch nếu cần join sau này
    .withColumn("playlist_name", lit(None).cast("string")) 
    .withColumn("track_name", lit(None).cast("string"))
)

# ======================================================
# 4. Compact (control number of files)
# ======================================================
daily_df = daily_df.repartition(1, "hour")

# ======================================================
# 5. Write training data
# ======================================================
daily_df.write \
    .mode("append") \
    .parquet("s3a://spotify-processed-data/spotify_tracks")

spark.stop()
