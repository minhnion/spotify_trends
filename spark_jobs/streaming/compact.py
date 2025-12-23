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
try:
    print("Attempting to read streaming data from s3a://spotify-processed-data/features_5m ...")
    df = spark.read.parquet("s3a://spotify-processed-data/features_5m")
    
    # Check if directory is empty (has schema but no data, though parquet read usually fails earlier if strictly empty)
    if not df.head(1): 
        print("⚠️ Streaming data path exists but contains no data. Skipping compaction.")
        spark.stop()
        sys.exit(0)
        
except Exception as e:
    # Catch generic exception because Spark/Py4J errors vary
    error_msg = str(e)
    if "Path does not exist" in error_msg or "FileNotFoundException" in error_msg:
        print(f"⚠️ Streaming data path not found: {error_msg}")
        print("Skipping compaction job gracefully.")
        spark.stop()
        sys.exit(0)
    else:
        # Re-raise real errors
        raise e

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

print("✓ Data successfully appended to spotify_tracks.")

# ======================================================
# 6. Delete processed files to prevent duplication
# ======================================================
try:
    print("🧹 Cleaning up processed files in s3a://spotify-processed-data/features_5m ...")
    
    # Get Hadoop FileSystem
    sc = spark.sparkContext
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
    src_path = sc._jvm.org.apache.hadoop.fs.Path("s3a://spotify-processed-data/features_5m")
    
    # Check if path exists and delete it
    if fs.exists(src_path):
        is_deleted = fs.delete(src_path, True) # True = recursive delete
        if is_deleted:
            print("✓ Successfully deleted source directory: s3a://spotify-processed-data/features_5m")
        else:
            print("⚠️ Failed to delete source directory.")
    else:
        print("⚠️ Source directory not found during cleanup (already deleted?).")
        
except Exception as e:
    print(f"❌ Error during cleanup: {str(e)}")
    # We don't exit(1) here because the main job (writing data) succeeded. 
    # But this needs attention if it fails.

spark.stop()
