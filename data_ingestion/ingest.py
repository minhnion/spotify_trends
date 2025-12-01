import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# --- 1. SETUP MÔI TRƯỜNG ---
os.environ['HADOOP_HOME'] = "D:\\hadoop"
sys.path.append("D:\\hadoop\\bin")

# Make sure pyspark uses the same Python interpreter as this process (helps avoid worker crashes)
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

JAR_PATH = "D:\\spotify_trends\\jars"
jars = [
    f"{JAR_PATH}\\hadoop-aws-3.3.4.jar",
    f"{JAR_PATH}\\aws-java-sdk-bundle-1.12.262.jar"
]

spark = SparkSession.builder \
    .appName("Spotify Project Final Fix") \
    .config("spark.jars", ",".join(jars)) \
    \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.maxResultSize", "1g") \
    \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.default.parallelism", "4") \
    \
    .config("spark.network.timeout", "600s") \
    .config("spark.executor.heartbeatInterval", "100s") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark OK! (Arrow disabled for stability)")

# Helper: safe show (fall back to take) — avoids crashing driver when Python workers disconnect
def safe_show(df, n=5, truncate=True):
    try:
        df.show(n, truncate=truncate)
    except Exception as ex:
        print(f"⚠️ show() failed with: {ex}\nFalling back to take({n}) and printing rows.")
        import traceback
        traceback.print_exc()
        try:
            rows = df.take(n)
            print(rows)
        except Exception as e2:
            print(f"⚠️ take() also failed: {e2}")

# ==========================================
# PHẦN 1: INGESTION
# ==========================================
print("\n--- [1] INGESTION ---")
try:
    # Đọc file
    raw_df = spark.read.option("multiline", "true").json("s3a://spotify-raw-data/mpd.slice.0-999.json")

    # Flatten
    tracks_df = raw_df.select(explode(col("playlists")).alias("p")) \
        .select(
            col("p.pid").alias("playlist_id"),
            col("p.name").alias("playlist_name"),
            explode(col("p.tracks")).alias("t")
        ).select(
            "playlist_id", "playlist_name",
            col("t.track_uri"),
            col("t.track_name"),
            col("t.artist_name"),
            col("t.duration_ms")
        )

    # --- QUAN TRỌNG: LẤY MẪU ĐỂ GIẢM TẢI CHO WINDOWS ---
    # Chỉ lấy 10% dữ liệu để demo luồng chạy mượt mà
    print("⚠️ Đang lấy mẫu 10% dữ liệu để tránh Crash trên Windows...")
    tracks_df = tracks_df.sample(fraction=0.1, seed=42)
    
    # Cache lại
    tracks_df.cache()
    count = tracks_df.count()
    print(f"Total Tracks (Sampled): {count}")

    # ==========================================
    # PHẦN 2: JOIN (Nguyên nhân gây lỗi cũ)
    # ==========================================
    print("\n--- [2] JOIN OPERATIONS ---")

    # Tạo bảng nhỏ metadata
    meta_data = [("Drake", "CA"), ("Kanye West", "US"), ("Ed Sheeran", "UK"), ("Rihanna", "BB")]
    artist_df = spark.createDataFrame(meta_data, ["artist_name", "country"])

    # Join thường (Không dùng Broadcast để test độ ổn định)
    joined_df = tracks_df.join(artist_df, "artist_name", "inner")

    print("Join Result (Top 5):")
    safe_show(joined_df.select("track_name", "artist_name", "country"), n=5)

    # ==========================================
    # PHẦN 3: STORAGE
    # ==========================================
    print("\n--- [3] STORAGE ---")
    output_path = "s3a://warehouse/processed_data_v2"
    joined_df.write.mode("overwrite").parquet(output_path)
    print(f"✅ Data saved to: {output_path}")

    # ==========================================
    # PHẦN 4: MLlib (Demo)
    # ==========================================
    print("\n--- [4] MLlib RECOMMENDATION ---")
    from pyspark.ml.recommendation import ALS
    from pyspark.ml.feature import StringIndexer
    from pyspark.ml import Pipeline

    rating_df = tracks_df.groupBy("playlist_id", "track_uri").count().withColumnRenamed("count", "rating")

    indexer_user = StringIndexer(inputCol="playlist_id", outputCol="user_id").setHandleInvalid("skip")
    indexer_item = StringIndexer(inputCol="track_uri", outputCol="item_id").setHandleInvalid("skip")

    pipeline = Pipeline(stages=[indexer_user, indexer_item])
    model_input = pipeline.fit(rating_df).transform(rating_df)

    als = ALS(maxIter=3, regParam=0.1, userCol="user_id", itemCol="item_id", ratingCol="rating", implicitPrefs=True)
    model = als.fit(model_input)

    print("Generating recommendations...")
    safe_show(model.recommendForAllUsers(1), n=1, truncate=False)

    print("\n🎉 SUCCESS! CODE CHẠY HẾT TỪ ĐẦU ĐẾN CUỐI!")

except Exception as e:
    print(f"\n❌ LỖI: {e}")
    import traceback
    traceback.print_exc()