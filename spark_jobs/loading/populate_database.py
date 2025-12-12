import struct
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))


from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

from spark_jobs.utils.spark_session import create_spark_session
from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv
from urllib.parse import urlparse, unquote

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise ValueError("Thiếu MONGO_URI trong file .env!")
DATABASE = os.getenv("MONGO_DATABASE", "spotify_db")

def connect_db(MONGO_URI=MONGO_URI, DATABASE=DATABASE):
    try:
        client = MongoClient(MONGO_URI) 

        client.admin.command('ping')
        print("Connected")

        db = client[DATABASE] 

        return db
    except ConnectionFailure:
        print("Cannot connect to MongoDB Server")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def spark_session_with_mongo():

    base_path = Path(__file__).resolve().parent

    project_root = base_path.parent.parent.parent
    print(f"Project root: {project_root}")

    sys.path.insert(0, str(project_root))

    jars_dir = project_root / "spotify_trends" / "jars"

    if not jars_dir.exists():
        raise FileNotFoundError(f"Directory does not exist: {jars_dir}")

    jars_list = [str(p) for p in jars_dir.glob("*.jar")]

    print(jars_list)

    spark = SparkSession.builder \
        .appName("Spotify → MongoDB Load") \
        .config("spark.jars", ",".join(jars_list)) \
        .config("spark.driver.memory", "4g") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400") \
        .config("spark.hadoop.fs.s3a.multipart.purge", "true") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "600000") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "600000") \
        .config("spark.hadoop.fs.s3a.attempts.maximum", "100") \
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60") \
        .config("spark.hadoop.fs.s3a.max.total.tasks", "100") \
        .config("spark.hadoop.fs.s3a.multipart.size", "104857600") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("Spark Connector Ready!")
    return spark
    


def run_db_population_job():
    connect_db(MONGO_URI, DATABASE)
    spark = spark_session_with_mongo()
    input_path = "s3a://spotify-processed-data/spotify_tracks" 

    print(f"\nĐang đọc dữ liệu từ: {input_path}")
    df = spark.read.parquet(input_path).cache()
    print(df.head())
    total_rows = df.count()
    print(f"Đã đọc thành công {total_rows:,} dòng")
    df.printSchema()

    from pyspark.sql.functions import col, struct

    artist_df = df.select('artist_uri', 'artist_name').distinct()
    artist_final = artist_df.withColumn('_id', col('artist_uri'))

    album_df = df.select('album_uri', 'album_name').distinct()
    album_final = album_df.withColumn('_id', col('album_uri'))  

    track_df = df.select('track_uri', 'track_name', 'artist_uri', 'album_uri', 'duration_ms').distinct()
    track_final = track_df.withColumn('_id', col('track_uri'))

    playlist_df= df.select('pid', 'playlist_name').distinct()
    playlist_final = playlist_df.withColumn('_id', col('pid')) 

    playlist_track_df = df.select('pid', 'track_uri').distinct()
    playlist_track_final = playlist_track_df.withColumn('_id', struct(col('pid'), col('track_uri')))

    try:
        print("Saving 'artists'...")
        artist_final.write \
            .format("mongodb") \
            .mode("overwrite") \
            .option("spark.mongodb.write.connection.uri", MONGO_URI) \
            .option("spark.mongodb.write.database", DATABASE) \
            .option("spark.mongodb.write.collection", 'Artists') \
            .save()
        print("Saving 'albums'...")
        album_final.write \
            .format("mongodb") \
            .mode("overwrite") \
            .option("spark.mongodb.write.connection.uri", MONGO_URI) \
            .option("spark.mongodb.write.database", DATABASE) \
            .option("spark.mongodb.write.collection", 'Albums') \
            .save()
        print("Saving 'tracks'...")
        track_final.write \
            .format("mongodb") \
            .mode("overwrite") \
            .option("spark.mongodb.write.connection.uri", MONGO_URI) \
            .option("spark.mongodb.write.database", DATABASE) \
            .option("spark.mongodb.write.collection", 'Tracks') \
            .save()
        print("Saving 'playlists'...")
        playlist_final.write \
            .format("mongodb") \
            .mode("overwrite") \
            .option("spark.mongodb.write.connection.uri", MONGO_URI) \
            .option("spark.mongodb.write.database", DATABASE) \
            .option("spark.mongodb.write.collection", 'Playlists') \
            .save()
        print("Saving 'playlist_tracks'...")
        playlist_track_final.write \
            .format("mongodb") \
            .mode("overwrite") \
            .option("spark.mongodb.write.connection.uri", MONGO_URI) \
            .option("spark.mongodb.write.database", DATABASE) \
            .option("spark.mongodb.write.collection", 'Playlist_Tracks') \
            .save()
        print("Saved to MongoDB!")
    except Exception as e:
        print(f"Error saving to MongoDB: {e}")

if __name__ == "__main__":
    run_db_population_job()
    
    # connect_db()
    # spark_session_with_mongo()
