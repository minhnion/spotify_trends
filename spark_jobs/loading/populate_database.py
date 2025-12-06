import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from spark_jobs.utils.spark_session import create_spark_session
import os
from dotenv import load_dotenv
from urllib.parse import urlparse, unquote

def run_db_population_job():
    """
    Read processed data from MinIO and write to PostgreSQL.
    """
    print("\n" + "="*80)
    print("STARTING DB POPULATION JOB")
    print("="*80 + "\n")
    
    spark = create_spark_session()
    
    # DEBUG: Print all S3A configs
    print("\n" + "="*80)
    print("SPARK S3A CONFIGURATION CHECK:")
    print("="*80)
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    
    s3a_configs = [
        "fs.s3a.endpoint",
        "fs.s3a.access.key",
        "fs.s3a.secret.key",
        "fs.s3a.path.style.access",
        "fs.s3a.impl",
        "fs.s3a.aws.credentials.provider",
        "fs.s3a.connection.timeout",
        "fs.s3a.connection.establish.timeout"
    ]
    
    for config in s3a_configs:
        value = hadoop_conf.get(config)
        if "key" in config.lower() and value:
            # Only show first 5 chars of keys
            print(f"{config}: {value[:5]}...")
        else:
            print(f"{config}: {value}")
    print("="*80 + "\n")
    
    input_path = "s3a://spotify-processed-data/spotify_tracks"
    
    print(f"Attempting to read from: {input_path}")
    print("="*80 + "\n")
    
    try:
        processed_df = spark.read.parquet(input_path).cache()
        row_count = processed_df.count()
        print(f"\n✓ Successfully read {row_count} records from MinIO")
        print("\nSchema:")
        processed_df.printSchema()
        
    except Exception as e:
        print(f"\n❌ FAILED TO READ FROM MINIO")
        print(f"Error: {e}")
        print("\n" + "="*80)
        import traceback
        traceback.print_exc()
        print("="*80 + "\n")
        raise
    
    # Continue with DB operations...
    load_dotenv(dotenv_path='../../.env')
    
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL must be set in .env file")
    
    parsed_url = urlparse(database_url)
    jdbc_url = f"jdbc:postgresql://{parsed_url.hostname}:{parsed_url.port}{parsed_url.path}"
    
    connection_properties = {
        "user": parsed_url.username,
        "password": unquote(parsed_url.password),
        "driver": "org.postgresql.Driver"
    }
    
    def write_to_db(df, table_name):
        print(f"\nWriting to table '{table_name}'...")
        count = df.count()
        print(f"  Rows to write: {count}")
        df.write.jdbc(
            url=jdbc_url,
            table=table_name,
            mode="overwrite",
            properties=connection_properties
        )
        print(f"  ✓ Finished writing to '{table_name}'")
    
    # Check required columns exist
    print("\n" + "="*80)
    print("CHECKING REQUIRED COLUMNS:")
    print("="*80)
    available_cols = processed_df.columns
    print(f"Available columns: {available_cols}")
    
    required_cols = ["artist_uri", "artist_name", "track_uri", "track_name", 
                     "album_uri", "duration_ms", "pid", "playlist_name"]
    
    missing_cols = [col for col in required_cols if col not in available_cols]
    if missing_cols:
        print(f"\n❌ Missing required columns: {missing_cols}")
        print("Please re-run ETL job with all required columns")
        raise ValueError(f"Missing columns: {missing_cols}")
    
    print("✓ All required columns present")
    print("="*80 + "\n")
    
    # Table artists
    print("\n=== Processing Artists Table ===")
    artists_df = processed_df.select("artist_uri", "artist_name").distinct()
    write_to_db(artists_df, "artists")
    
    # Table tracks
    print("\n=== Processing Tracks Table ===")
    tracks_df = processed_df.select("track_uri", "track_name", "artist_uri", 
                                    "album_uri", "duration_ms").distinct()
    write_to_db(tracks_df, "tracks")
    
    # Table playlists
    print("\n=== Processing Playlists Table ===")
    playlists_df = processed_df.select("pid", "playlist_name").distinct()
    write_to_db(playlists_df, "playlists")
    
    # Table playlist_tracks
    print("\n=== Processing Playlist_Tracks Table ===")
    playlist_tracks_df = processed_df.select("pid", "track_uri")
    write_to_db(playlist_tracks_df, "playlist_tracks")
    
    processed_df.unpersist()
    
    print("\n" + "="*80)
    print("✅ DATABASE POPULATION JOB COMPLETED SUCCESSFULLY!")
    print("="*80 + "\n")
    
    spark.stop()

if __name__ == "__main__":
    run_db_population_job()