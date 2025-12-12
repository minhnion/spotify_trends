import sys
import os
import psycopg2
from pathlib import Path
from urllib.parse import urlparse, unquote
from pyspark.sql.functions import col

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from spark_jobs.utils.spark_session import create_spark_session
from dotenv import load_dotenv

def run_db_population_job():
    """
    Read processed data from MinIO and write to PostgreSQL (Supabase).
    Synchronized with Jupyter Notebook logic.
    """
    print("\n" + "="*80)
    print("STARTING DB POPULATION JOB")
    print("="*80 + "\n")
    
    # 1. LOAD CONFIG
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL must be set in .env file")

    parsed = urlparse(database_url)
    
    # [FIX 1] Thêm prepareThreshold=0 để tránh lỗi PgBouncer của Supabase
    jdbc_url = f"jdbc:postgresql://{parsed.hostname}:{parsed.port}{parsed.path}?prepareThreshold=0"
    
    connection_properties = {
        "user": parsed.username,
        "password": unquote(parsed.password),
        "driver": "org.postgresql.Driver",
        "sslmode": "require"
    }

    # 2. CLEAN UP OLD DATA (TRUNCATE)
    # Phần này phải chạy trước khi Spark khởi động ghi để tránh lỗi khóa ngoại
    print("🧹 Cleaning up old data in Database...")
    try:
        conn = psycopg2.connect(
            host=parsed.hostname, 
            port=parsed.port, 
            database=parsed.path[1:], # Bỏ dấu /
            user=parsed.username, 
            password=unquote(parsed.password), 
            sslmode='require'
        )
        cur = conn.cursor()
        # [FIX 2] Xóa theo thứ tự và Cascade
        cur.execute("TRUNCATE TABLE playlist_tracks, tracks, playlists, albums, artists CASCADE;")
        conn.commit()
        print("✅ Database cleaned successfully!")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"⚠️ Warning during cleanup: {e}")
        print("Continuing with Spark job...")

    # 3. START SPARK
    spark = create_spark_session()
    
    # [FIX 3] Đọc từ đường dẫn chứa Full Schema (có album, uri...)
    input_path = "s3a://warehouse/spotify_full_schema"
    
    print(f"Attempting to read from: {input_path}")
    
    try:
        full_df = spark.read.parquet(input_path)
        row_count = full_df.count()
        print(f"\n✓ Successfully read {row_count} records from MinIO")
    except Exception as e:
        print(f"\n❌ FAILED TO READ FROM MINIO: {e}")
        spark.stop()
        raise

    # Hàm ghi tiện ích
    def write_to_db(df, table_name):
        print(f"\nWriting to table '{table_name}'...")
        try:
            df.write.jdbc(
                url=jdbc_url,
                table=table_name,
                mode="append", # Dùng append vì đã Truncate ở trên
                properties=connection_properties
            )
            print(f"  ✓ Finished writing to '{table_name}'")
        except Exception as e:
            print(f"  ❌ Error writing to '{table_name}': {e}")
            raise # Dừng luôn nếu lỗi để debug

    # --- 4. GHI DỮ LIỆU (THEO THỨ TỰ BẢNG CHA -> CON) ---
    
    try:
        # Table 1: Artists
        print("\n=== Processing Artists Table ===")
        artists_df = full_df.select("artist_uri", "artist_name").distinct()
        write_to_db(artists_df, "artists")
        
        # Table 2: Albums (Code cũ thiếu phần này)
        print("\n=== Processing Albums Table ===")
        albums_df = full_df.select("album_uri", "album_name").distinct()
        write_to_db(albums_df, "albums")
        
        # Table 3: Playlists
        print("\n=== Processing Playlists Table ===")
        # [FIX 4] Alias playlist_id -> pid
        playlists_df = full_df.select(
            col("playlist_id").alias("pid"), 
            col("playlist_name")
        ).distinct()
        write_to_db(playlists_df, "playlists")
        
        # Table 4: Tracks (Phụ thuộc Artist & Album)
        print("\n=== Processing Tracks Table ===")
        tracks_df = full_df.select(
            "track_uri", "track_name", "artist_uri", "album_uri", "duration_ms"
        ).distinct()
        write_to_db(tracks_df, "tracks")
        
        # Table 5: Playlist_Tracks (Phụ thuộc Playlist & Track)
        print("\n=== Processing Playlist_Tracks Table ===")
        playlist_tracks_df = full_df.select(
            col("playlist_id").alias("pid"), 
            col("track_uri")
        ).distinct()
        write_to_db(playlist_tracks_df, "playlist_tracks")
        
        print("\n" + "="*80)
        print("✅ DATABASE POPULATION JOB COMPLETED SUCCESSFULLY!")
        print("="*80 + "\n")
        
    except Exception as e:
        print(f"\n❌ Job Failed: {e}")
    finally:
        full_df.unpersist()
        spark.stop()

if __name__ == "__main__":
    run_db_population_job()