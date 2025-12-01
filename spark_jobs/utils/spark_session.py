from dotenv import load_dotenv
from pyspark.sql import SparkSession
import os

def create_spark_session():
    script_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(script_dir, "../../"))
    dotenv_path = os.path.join(project_root, '.env')
    load_dotenv(dotenv_path=dotenv_path)
    
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")
    
    if not access_key or not secret_key:
        raise ValueError("MINIO_ACCESS_KEY and MINIO_SECRET_KEY must be set in .env file")
    
    spark = (
        SparkSession.builder
        .appName("SpotifyModelTraining")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.780")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Fix all timeout-related configs with numeric values
        .config("spark.hadoop.fs.s3a.connection.timeout", "200000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "3")
        .config("spark.hadoop.fs.s3a.retry.limit", "3")
        .config("spark.hadoop.fs.s3a.retry.interval", "500")
        .config("spark.hadoop.fs.s3a.retry.throttle.limit", "3")
        .config("spark.hadoop.fs.s3a.retry.throttle.interval", "1000")
        # The problematic keepalive timeout
        .config("spark.hadoop.fs.s3a.connection.keepalive", "60000")  # 60 seconds in milliseconds
        .config("spark.hadoop.fs.s3a.threads.max", "10")
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000")  # This is likely the culprit
        # Additional MinIO-specific configs
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )
    return spark