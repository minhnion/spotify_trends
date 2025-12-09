import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from spark_jobs.utils.spark_session import create_spark_session
from pyspark.sql.functions import from_json, col, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def run_streaming_job():
    """
    Job Spark Structured Streaming để đọc sự kiện từ Kafka và ghi ra MinIO.
    """
    spark = create_spark_session(app_name="SpotifyStreaming")

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "playlist_events") \
        .option("startingOffsets", "latest") \
        .load()

    event_schema = StructType([
        StructField("event_type", StringType(), True),
        StructField("pid", IntegerType(), True),
        StructField("track_uri", StringType(), True),
        StructField("playlist_name", StringType(), True), # Thêm cột mới
        StructField("timestamp", StringType(), True)
    ])

    parsed_df = kafka_df.select(from_json(col("value").cast("string"), event_schema).alias("data")) \
                        .select("data.*")
    
    output_df = parsed_df.withColumn("event_timestamp", col("timestamp").cast("timestamp")) \
                         .withColumn("year", year(col("event_timestamp"))) \
                         .withColumn("month", month(col("event_timestamp"))) \
                         .withColumn("day", dayofmonth(col("event_timestamp")))

    output_path = "s3a://spotify-streaming-data/"
    checkpoint_path = "s3a://spotify-streaming-checkpoint/"

    query = output_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .partitionBy("year", "month", "day") \
        .start()

    print(f"Streaming job started. Writing data to {output_path}")
    print("Press Ctrl+C to stop.")
    
    query.awaitTermination()

if __name__ == "__main__":
    run_streaming_job()