import sys
from pathlib import Path
import traceback

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from pyspark.sql.functions import explode, col
from spark_jobs.utils.spark_session import create_spark_session


def run_etl_job():
    spark = None
    try:
        print("=" * 80)
        print("Starting Spotify ETL Job...")
        print("=" * 80)
        
        # Create Spark session
        spark = create_spark_session()
        
        # Define pathsa
        input_path = "s3a://spotify-raw-data/*.json"
        output_path = "s3a://spotify-processed-data/spotify_tracks"
        
        print("\n" + "=" * 80)
        print("DEBUG - Path Information:")
        print("=" * 80)
        print(f"input_path: '{input_path}'")
        print(f"input_path type: {type(input_path)}")
        print(f"input_path length: {len(input_path)}")
        print(f"\noutput_path: '{output_path}'")
        print(f"output_path type: {type(output_path)}")
        print(f"output_path length: {len(output_path)}")
        print("=" * 80 + "\n")
        
        # Read data
        print(f"Step 1: Reading data from {input_path}...")
        df = spark.read.option("multiLine", "true").json(input_path)
        print(f"✓ Successfully read data")
        print(f"  - Rows in raw data: {df.count()}")
        
        # Explode playlists
        print("\nStep 2: Exploding playlists...")
        df_playlists = df.select(explode("playlists").alias("playlist"))
        print(f"✓ Playlists exploded")
        print(f"  - Total playlists: {df_playlists.count()}")
        
        # Explode tracks
        print("\nStep 3: Exploding tracks...")
        df_tracks = df_playlists.select(
            col("playlist.pid").alias("pid"),
            col("playlist.name").alias("playlist_name"),
            explode("playlist.tracks").alias("track")
        )
        print(f"✓ Tracks exploded")
        
        # Select final columns
        print("\nStep 4: Selecting final columns...")
        final_df = df_tracks.select(
            "pid",
            "playlist_name",
            col("track.track_name").alias("track_name"),
            col("track.track_uri").alias("track_uri"),
            col("track.artist_name").alias("artist_name"),
            col("track.album_name").alias("album_name"),
            col("track.duration_ms").alias("duration_ms"),
            col("track.artist_uri").alias("artist_uri"),
            col("track.album_uri").alias("album_uri"),
        )
        
        # Print schema
        print("\nFinal DataFrame Schema:")
        final_df.printSchema()
        
        # Count rows
        row_count = final_df.count()
        print(f"\n✓ Total rows after transformation: {row_count}")
        
        # Show sample data
        print("\nSample data (first 5 rows):")
        final_df.show(5, truncate=False)
        
        print("\n" + "=" * 80)
        print("DEBUG - Before Writing:")
        print("=" * 80)
        print(f"output_path value: '{output_path}'")
        print(f"output_path is None: {output_path is None}")
        print(f"output_path is empty: {output_path == ''}")
        print(f"output_path length: {len(output_path)}")
        print("=" * 80 + "\n")
        
        # Write to S3
        print(f"Step 5: Writing data to {output_path}...")
        print("  - Mode: overwrite")
        print("  - Format: parquet")
        
        final_df.write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(output_path)
        
        print("\n" + "=" * 80)
        print("✅ ETL JOB COMPLETED SUCCESSFULLY!")
        print("=" * 80)
        print(f"✓ Processed {row_count} rows")
        print(f"✓ Data written to: {output_path}")
        print("=" * 80 + "\n")
        
    except Exception as e:
        print("\n" + "=" * 80)
        print("❌ ERROR IN ETL JOB")
        print("=" * 80)
        print(f"Error type: {type(e).__name__}")
        print(f"Error message: {str(e)}")
        print("\nFull traceback:")
        print("-" * 80)
        traceback.print_exc()
        print("=" * 80 + "\n")
        raise
        
    finally:
        if spark:
            print("Stopping Spark session...")
            spark.stop()
            print("✓ Spark session stopped\n")


if __name__ == "__main__":
    run_etl_job()