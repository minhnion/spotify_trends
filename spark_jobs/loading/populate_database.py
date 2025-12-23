import sys
import os
from pathlib import Path
import traceback

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from pyspark.sql.functions import col, struct

# Check environment and import appropriate spark session creator
if os.getenv("KUBERNETES_SERVICE_HOST"):
    print("Running in Kubernetes environment")
    from spark_jobs.utils.spark_session_k8s import create_spark_session_with_mongo
else:
    print("Running in local environment")
    from spark_jobs.utils.spark_session import create_spark_session_with_mongo


def test_mongodb_connection(mongo_uri, database):
    """Test MongoDB connection before processing"""
    try:
        print("\n=== Testing MongoDB Connection ===")
        client = MongoClient(mongo_uri)
        client.admin.command('ping')
        print(f"✓ Connected to MongoDB")
        print(f"✓ Database: {database}")
        print("="*50 + "\n")
        return True
    except ConnectionFailure:
        print("Cannot connect to MongoDB Server")
        return False
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return False


def run_db_population_job():
    spark = None
    try:
        print("=" * 80)
        print("Starting Database Population Job...")
        print("=" * 80)
        
        # Create Spark session and get MongoDB config
        spark, mongo_uri, mongo_database = create_spark_session_with_mongo()
        
        # Test MongoDB connection
        if not test_mongodb_connection(mongo_uri, mongo_database):
            raise ConnectionError("Failed to connect to MongoDB")
        
        # Verify Hadoop configuration
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        print("\n=== FINAL HADOOP CONFIG CHECK ===")
        print(f"fs.s3a.endpoint: {hadoop_conf.get('fs.s3a.endpoint')}")
        print(f"fs.s3a.access.key: {hadoop_conf.get('fs.s3a.access.key')}")
        print(f"fs.s3a.path.style.access: {hadoop_conf.get('fs.s3a.path.style.access')}")
        print("=" * 80 + "\n")
        
        # Define paths
        input_path = "s3a://spotify-processed-data/spotify_tracks"
        
        # Read data
        print(f"Step 1: Reading data from {input_path}...")
        df = spark.read.parquet(input_path).cache()
        total_rows = df.count()
        print(f"✓ Successfully read {total_rows:,} rows")
        
        print("\nDataFrame Schema:")
        df.printSchema()
        
        print("\nSample data (first 3 rows):")
        df.show(3, truncate=False)
        
        # Prepare collections
        print("\nStep 2: Preparing collections...")
        
        print("  - Creating Artists collection...")
        artist_df = df.select('artist_uri', 'artist_name').distinct()
        artist_final = artist_df.withColumn('_id', col('artist_uri'))
        artist_count = artist_final.count()
        print(f"    ✓ {artist_count:,} unique artists")
        
        print("  - Creating Albums collection...")
        album_df = df.select('album_uri', 'album_name').distinct()
        album_final = album_df.withColumn('_id', col('album_uri'))
        album_count = album_final.count()
        print(f"    ✓ {album_count:,} unique albums")
        
        print("  - Creating Tracks collection...")
        track_df = df.select('track_uri', 'track_name', 'artist_uri', 
                             'album_uri', 'duration_ms').distinct()
        track_final = track_df.withColumn('_id', col('track_uri'))
        track_count = track_final.count()
        print(f"    ✓ {track_count:,} unique tracks")
        
        print("  - Creating Playlists collection...")
        playlist_df = df.select('pid', 'playlist_name').distinct()
        playlist_final = playlist_df.withColumn('_id', col('pid'))
        playlist_count = playlist_final.count()
        print(f"    ✓ {playlist_count:,} unique playlists")
        
        print("  - Creating Playlist_Tracks collection...")
        playlist_track_df = df.select('pid', 'track_uri').distinct()
        playlist_track_final = playlist_track_df.withColumn(
            '_id', struct(col('pid'), col('track_uri'))
        )
        playlist_track_count = playlist_track_final.count()
        print(f"    ✓ {playlist_track_count:,} playlist-track relationships")
        
        # Write to MongoDB
        print("\nStep 3: Writing to MongoDB...")
        print(f"  - MongoDB URI: {mongo_uri[:50]}...")
        print(f"  - Database: {mongo_database}")
        print(f"  - Mode: overwrite")
        
        print("\n  Writing 'Artists'...")
        artist_final.write \
            .format("mongodb") \
            .mode("overwrite") \
            .option("spark.mongodb.write.connection.uri", mongo_uri) \
            .option("spark.mongodb.write.database", mongo_database) \
            .option("spark.mongodb.write.collection", 'Artists') \
            .save()
        print("    ✓ Artists saved")
        
        print("  Writing 'Albums'...")
        album_final.write \
            .format("mongodb") \
            .mode("overwrite") \
            .option("spark.mongodb.write.connection.uri", mongo_uri) \
            .option("spark.mongodb.write.database", mongo_database) \
            .option("spark.mongodb.write.collection", 'Albums') \
            .save()
        print("    ✓ Albums saved")
        
        print("  Writing 'Tracks'...")
        track_final.write \
            .format("mongodb") \
            .mode("overwrite") \
            .option("spark.mongodb.write.connection.uri", mongo_uri) \
            .option("spark.mongodb.write.database", mongo_database) \
            .option("spark.mongodb.write.collection", 'Tracks') \
            .save()
        print("    ✓ Tracks saved")
        
        print("  Writing 'Playlists'...")
        playlist_final.write \
            .format("mongodb") \
            .mode("overwrite") \
            .option("spark.mongodb.write.connection.uri", mongo_uri) \
            .option("spark.mongodb.write.database", mongo_database) \
            .option("spark.mongodb.write.collection", 'Playlists') \
            .save()
        print("    ✓ Playlists saved")
        
        print("  Writing 'Playlist_Tracks'...")
        playlist_track_final.write \
            .format("mongodb") \
            .mode("overwrite") \
            .option("spark.mongodb.write.connection.uri", mongo_uri) \
            .option("spark.mongodb.write.database", mongo_database) \
            .option("spark.mongodb.write.collection", 'Playlist_Tracks') \
            .save()
        print("    ✓ Playlist_Tracks saved")
        
        print("\n" + "=" * 80)
        print("✅ DATABASE POPULATION COMPLETED SUCCESSFULLY!")
        print("=" * 80)
        print(f"✓ Processed {total_rows:,} rows")
        print(f"✓ Created {artist_count:,} artists")
        print(f"✓ Created {album_count:,} albums")
        print(f"✓ Created {track_count:,} tracks")
        print(f"✓ Created {playlist_count:,} playlists")
        print(f"✓ Created {playlist_track_count:,} playlist-track relationships")
        print(f"✓ Data written to MongoDB: {mongo_database}")
        print("=" * 80 + "\n")
        
    except Exception as e:
        print("\n" + "=" * 80)
        print("❌ ERROR IN DATABASE POPULATION JOB")
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
    run_db_population_job()