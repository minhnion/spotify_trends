from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import (
    col, to_date, sum as spark_sum, min, max, lit
)
import sys
from pathlib import Path

# Setup paths (giữ nguyên logic của bạn)
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))
from spark_jobs.utils.spark_session import create_spark_session

spark = create_spark_session(app_name="SpotifyStreamingWindows")
spark.sparkContext.setLogLevel("WARN")

# Đường dẫn input
INPUT_PATH = "s3a://spotify-processed-data/features_5m"

# ======================================================
# 1. Read streaming output (SAFE MODE)
# ======================================================
print(f"Checking data at: {INPUT_PATH}")

try:
    # Cố gắng đọc dữ liệu
    df = spark.read.parquet(INPUT_PATH)
    
    # Kiểm tra xem có bản ghi nào không (dùng head(1) để tối ưu hiệu năng thay vì count())
    if len(df.head(1)) == 0:
        print(">>> Folder exists but contains NO data. Skipping job.")
        spark.stop()
        sys.exit(0)

except AnalysisException:
    # Lỗi này xảy ra khi folder không tồn tại hoặc không thể đọc schema
    print(f">>> Path '{INPUT_PATH}' does not exist or is empty. Skipping job.")
    spark.stop()
    sys.exit(0)
except Exception as e:
    # Các lỗi khác (quyền truy cập, kết nối mạng...)
    print(f">>> An unexpected error occurred: {str(e)}")
    spark.stop()
    sys.exit(1) # Exit code 1 để báo job fail thật sự nếu cần

print(">>> Data found. Proceeding with processing...")

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
OUTPUT_PATH = "s3a://spotify-processed-data/spotify_tracks"
print(f"Writing data to: {OUTPUT_PATH}")

daily_df.write \
    .mode("append") \
    .parquet(OUTPUT_PATH)

print(">>> Job completed successfully.")
spark.stop()