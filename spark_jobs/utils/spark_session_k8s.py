from pyspark.sql import SparkSession
import os

def create_spark_session():
    """Create Spark session for K8s - completely override all configs"""
    
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")
    
    if not access_key or not secret_key:
        raise ValueError("MINIO_ACCESS_KEY and MINIO_SECRET_KEY must be set")
    
    print(f"\n=== Creating Spark Session (K8s Mode) ===")
    print(f"Access Key: {access_key}")
    print(f"Secret Key: {secret_key[:5]}...")
    print("="*50 + "\n")
    
    spark = (
        SparkSession.builder
        .appName("SpotifyETL")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio-service:9000")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", "http://minio-service:9000")
    hadoop_conf.set("fs.s3a.access.key", access_key)
    hadoop_conf.set("fs.s3a.secret.key", secret_key)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.aws.credentials.provider", 
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    
    print("=== S3A Configuration Verification ===")
    print(f"fs.s3a.endpoint: {hadoop_conf.get('fs.s3a.endpoint')}")
    print(f"fs.s3a.access.key: {hadoop_conf.get('fs.s3a.access.key')}")
    print(f"fs.s3a.path.style.access: {hadoop_conf.get('fs.s3a.path.style.access')}")
    print(f"fs.s3a.impl: {hadoop_conf.get('fs.s3a.impl')}")
    
    try:
        print("\n=== Testing S3A Connection ===")
        test_buckets = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark.sparkContext._jsc.hadoopConfiguration()
        ).getUri()
        print(f"Filesystem URI: {test_buckets}")
    except Exception as e:
        print(f"Warning: Could not test connection: {e}")
    
    print("="*50 + "\n")
    
    return spark