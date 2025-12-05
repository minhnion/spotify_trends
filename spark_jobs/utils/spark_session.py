from dotenv import load_dotenv
from pyspark.sql import SparkSession
import os

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