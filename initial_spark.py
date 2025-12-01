from pyspark.sql import SparkSession
import os

# Provide AWS region and credentials in process environment so JVM/SDK on driver
# and executors can pick them up when creating S3 clients.
os.environ.setdefault("AWS_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "minioadmin")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "minioadmin")
from pyspark.sql.functions import col, explode, current_timestamp

# Cấu hình kết nối
spark = SparkSession.builder \
    .appName("Spotify Local Demo") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "rest") \
    .config("spark.sql.catalog.my_catalog.uri", "http://iceberg:8181") \
    .config("spark.sql.catalog.my_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.my_catalog.s3.endpoint", "http://minio:9000") \
    .config("spark.sql.catalog.my_catalog.s3.path-style-access", "true") \
    .config("spark.sql.catalog.my_catalog.client.region", "us-east-1") \
    .config("spark.sql.catalog.my_catalog.s3.region", "us-east-1") \
    .config("spark.driver.extraJavaOptions", "-Daws.region=us-east-1 -Daws.accessKeyId=minioadmin -Daws.secretKey=minioadmin") \
    .config("spark.executor.extraJavaOptions", "-Daws.region=us-east-1 -Daws.accessKeyId=minioadmin -Daws.secretKey=minioadmin") \
    .config("spark.executorEnv.AWS_REGION", "us-east-1") \
    .config("spark.executorEnv.AWS_ACCESS_KEY_ID", "minioadmin") \
    .config("spark.executorEnv.AWS_SECRET_ACCESS_KEY", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

print("✅ Spark Session OK!")

# Tạo Database
try:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS my_catalog.spotify_db")
except:
    pass

# Đọc dữ liệu (Test 1 file trước)
# Vì MinIO đã mount folder spotify-raw-data làm bucket, nên đường dẫn là:
s3_path = "s3a://spotify-raw-data/mpd.slice.0-999.json"

print(f"⏳ Reading: {s3_path}")

try:
    df = spark.read.option("multiline", "true").json(s3_path)
    
    # Xử lý đơn giản (Bronze Layer)
    if "playlists" in df.columns:
        bronze = df.select(explode(col("playlists")).alias("p")).select(
            col("p.pid").alias("playlist_id"),
            col("p.name").alias("playlist_name"),
            col("p.num_tracks"),
            current_timestamp().alias("ingestion_time")
        )
        
        print("💾 Writing to Iceberg...")
        # Ghi vào bảng
        bronze.writeTo("my_catalog.spotify_db.spotify_bronze") \
              .partitionedBy(col("ingestion_time")) \
              .createOrReplace()
              
        print("🎉 SUCCESS! Data Preview:")
        spark.sql("SELECT * FROM my_catalog.spotify_db.spotify_bronze LIMIT 5").show()
    else:
        print("⚠️ Schema mismatch")

except Exception as e:
    print(f"❌ Error: {e}")