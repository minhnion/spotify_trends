from pyspark.sql.functions import explode, col
from spark_jobs.utils.spark_session import create_spark_session

def run_etl_job():
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
    run_etl_job()