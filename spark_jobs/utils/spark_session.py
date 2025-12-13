from dotenv import load_dotenv
from pyspark.sql import SparkSession
import os
from pathlib import Path

def create_spark_session():
    """Create and config Spark session with MinIO settings from .env file."""
    script_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(script_dir, "../../"))
    dotenv_path = os.path.join(project_root, '.env')
    
    load_dotenv(dotenv_path=dotenv_path)
    
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")
    
    if not access_key or not secret_key:
        raise ValueError("MINIO_ACCESS_KEY and MINIO_SECRET_KEY must be set in .env file")
    
    print(f"\n=== Creating Spark Session ===")
    print(f"Access Key: {access_key}")
    print(f"Secret Key: {secret_key[:5]}...")
    print(f"Endpoint: http://localhost:9000")
    print("="*50 + "\n")
    
    spark = (
        SparkSession.builder
        .appName("SpotifyETL")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    
    # QUAN TRỌNG: Force set config sau khi session được tạo
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", "http://localhost:9000")
    hadoop_conf.set("fs.s3a.access.key", access_key)
    hadoop_conf.set("fs.s3a.secret.key", secret_key)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.aws.credentials.provider", 
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    
    # Verify config
    print("=== Verifying Hadoop Configuration ===")
    print(f"fs.s3a.endpoint: {hadoop_conf.get('fs.s3a.endpoint')}")
    print(f"fs.s3a.access.key: {hadoop_conf.get('fs.s3a.access.key')}")
    print(f"fs.s3a.path.style.access: {hadoop_conf.get('fs.s3a.path.style.access')}")
    print("="*50 + "\n")
    
    return spark

def create_spark_session_with_mongo():
    """Create Spark session for local environment with MongoDB connector"""
    
    # Load .env file
    project_root = Path(__file__).parent.parent.parent
    env_file = project_root / '.env'
    if env_file.exists():
        load_dotenv(env_file)
        print(f"✓ Loaded .env from {env_file}")
    
    # Get credentials
    mongo_uri = os.getenv("MONGO_URI")
    mongo_database = os.getenv("MONGO_DATABASE", "spotify_db")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    
    if not mongo_uri:
        raise ValueError("MONGO_URI must be set in .env file")
    
    print(f"\n=== Creating Spark Session (Local Mode with MongoDB) ===")
    print(f"MongoDB URI: {mongo_uri[:50]}...")
    print(f"MongoDB Database: {mongo_database}")
    print(f"MinIO Endpoint: http://localhost:9000")
    print("="*50 + "\n")
    
    # Find JAR files
    jars_dir = project_root / "jars"
    if not jars_dir.exists():
        raise FileNotFoundError(f"JAR directory not found: {jars_dir}")
    
    jars_list = [str(p) for p in jars_dir.glob("*.jar")]
    if not jars_list:
        raise FileNotFoundError(f"No JAR files found in {jars_dir}")
    
    print(f"Found {len(jars_list)} JAR files")
    
    spark = (
        SparkSession.builder
        .appName("Spotify-MongoDB-Load")
        .config("spark.jars", ",".join(jars_list))
        .config("spark.driver.memory", "4g")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("✓ Spark session created successfully")
    print("="*50 + "\n")
    
    return spark, mongo_uri, mongo_database