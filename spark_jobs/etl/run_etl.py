import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col

def create_spark_session():
    load_dotenv(dotenv_path='../../.env')

    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")

    spark = (
        SparkSession.builder.appName("SpotifyETL")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
        .getOrCreate()
    )
    return spark

def main():
    spark = create_spark_session()
    
    input_path = "s3a://spotify-raw-data/*.json"
    output_path = "s3a://spotify-processed-data/"

    print(f"Reading data from {input_path}...")
    df = spark.read.json(input_path)

    df_playlists = df.select(explode("playlists").alias("playlist"))

    df_tracks = df_playlists.select(
        col("playlist.pid").alias("pid"),
        col("playlist.name").alias("playlist_name"),
        explode("playlist.tracks").alias("track")
    )

    final_df = df_tracks.select(
        "pid",
        "playlist_name",
        col("track.track_name").alias("track_name"),
        col("track.track_uri").alias("track_uri"),
        col("track.artist_name").alias("artist_name"),
        col("track.album_name").alias("album_name"),
        col("track.duration_ms").alias("duration_ms")    
    )
    
    final_df.printSchema()
    print(f"Total rows after transformation: {final_df.count()}")

    final_df.write.mode("overwrite").parquet(output_path)
    print("ETL job completed successfully!")
    spark.stop()

if __name__=="__main__":
    main()