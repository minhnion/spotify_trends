import sys
import os
from pathlib import Path
import traceback
from urllib.parse import urlparse

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from pyspark.sql.functions import explode, col

if os.getenv("KUBERNETES_SERVICE_HOST"):
    print("🚀 Running in Kubernetes environment")
    from spark_jobs.utils.spark_session_k8s import create_spark_session
else:
    print("💻 Running in local environment")
    from spark_jobs.utils.spark_session import create_spark_session

# Use MinIO client (already in requirements)
from minio import Minio
from dotenv import load_dotenv

def normalize_endpoint(endpoint: str):
    if not endpoint:
        return None, False
    endpoint = endpoint.strip()
    secure = True
    if endpoint.startswith("http://"):
        secure = False
        endpoint = endpoint[len("http://"):]
    elif endpoint.startswith("https://"):
        endpoint = endpoint[len("https://"):]
        secure = True
    # strip trailing slash
    endpoint = endpoint.rstrip("/")
    return endpoint, secure

def ensure_s3_bucket_exists(endpoint: str, access_key: str, secret_key: str, bucket_name: str, secure: bool = False):
    if not endpoint or not access_key or not secret_key or not bucket_name:
        raise ValueError("Missing parameters for ensure_s3_bucket_exists")
    client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
    try:
        exists = client.bucket_exists(bucket_name)
    except Exception as e:
        raise RuntimeError(f"Unable to check bucket existence for '{bucket_name}' on '{endpoint}': {e}")
    if not exists:
        print(f"Bucket '{bucket_name}' does not exist on '{endpoint}' — creating it.")
        try:
            client.make_bucket(bucket_name)
            print(f"✓ Bucket '{bucket_name}' created.")
        except Exception as e:
            raise RuntimeError(f"Failed to create bucket '{bucket_name}': {e}")
    else:
        print(f"✓ Bucket '{bucket_name}' already exists.")

def extract_bucket_from_s3a_path(s3a_path: str):
    # Expect s3a://bucket_name/optional/path...
    if not s3a_path.startswith("s3a://"):
        raise ValueError("Path must start with s3a://")
    without_scheme = s3a_path[len("s3a://"):]
    parts = without_scheme.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    return bucket, prefix

def load_env_if_needed():
    # If MINIO env vars are missing, try loading project .env
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")
    if not access_key or not secret_key:
        dotenv_path = os.path.join(str(project_root), ".env")
        if os.path.exists(dotenv_path):
            load_dotenv(dotenv_path=dotenv_path)
            access_key = os.getenv("MINIO_ACCESS_KEY")
            secret_key = os.getenv("MINIO_SECRET_KEY")
    return access_key, secret_key

def run_etl_job():
    spark = None
    try:
        print("=" * 80)
        print("Starting Spotify ETL Job...")
        print("=" * 80)
        
        # Create Spark session
        spark = create_spark_session()
        
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        print("\n=== FINAL HADOOP CONFIG CHECK ===")
        print(f"fs.s3a.endpoint: {hadoop_conf.get('fs.s3a.endpoint')}")
        print(f"fs.s3a.access.key: {hadoop_conf.get('fs.s3a.access.key')}")
        print(f"fs.s3a.path.style.access: {hadoop_conf.get('fs.s3a.path.style.access')}")
        print("=" * 80 + "\n")
        
        # Define paths
        input_path = "s3a://spotify-raw-data/*.json"
        output_path = "s3a://spotify-processed-data/spotify_tracks"
        
        # Ensure buckets exist (MinIO) before writing
        endpoint_cfg = hadoop_conf.get("fs.s3a.endpoint")
        access_key = hadoop_conf.get("fs.s3a.access.key") or os.getenv("MINIO_ACCESS_KEY")
        secret_key = hadoop_conf.get("fs.s3a.secret.key") or os.getenv("MINIO_SECRET_KEY")

        # If env vars still missing, attempt to load .env
        if not access_key or not secret_key:
            env_access, env_secret = load_env_if_needed()
            access_key = access_key or env_access
            secret_key = secret_key or env_secret

        endpoint, secure = normalize_endpoint(endpoint_cfg)
        if not endpoint:
            raise RuntimeError("S3A endpoint is not configured in Spark Hadoop configuration.")

        # Extract and ensure both input and output buckets exist
        try:
            in_bucket, _ = extract_bucket_from_s3a_path(input_path)
            out_bucket, _ = extract_bucket_from_s3a_path(output_path)
            ensure_s3_bucket_exists(endpoint, access_key, secret_key, in_bucket, secure=secure)
            ensure_s3_bucket_exists(endpoint, access_key, secret_key, out_bucket, secure=secure)
        except Exception as e:
            print(f"Warning: Could not ensure buckets exist: {e}")
            # decide whether to continue or fail; we will fail since write will also fail
            raise
        
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