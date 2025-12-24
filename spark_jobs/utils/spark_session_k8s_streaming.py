from pyspark.sql import SparkSession
import os

def create_spark_session_k8s(app_name: str = "Spotify-ETL"):
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")

    if not access_key or not secret_key:
        raise ValueError("MINIO_ACCESS_KEY and MINIO_SECRET_KEY must be set")

    # Định nghĩa các package cần thiết
    # Kafka + Hadoop AWS (để làm việc với MinIO)
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ]

    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.master", os.getenv("SPARK_MASTER", "k8s://https://kubernetes.default.svc"))
        .config("spark.kubernetes.container.image", os.getenv("SPARK_IMAGE", "spark-streaming-k8s:3.5.1"))
        .config("spark.kubernetes.namespace", os.getenv("SPARK_NAMESPACE", "spotify"))
        .config("spark.kubernetes.authenticate.driver.serviceAccountName", os.getenv("SPARK_SERVICE_ACCOUNT", "spark"))
        .config("spark.kubernetes.container.image.pullPolicy", "Never")
        
        # Jars & Ivy
        #.config("spark.jars.packages", ",".join(packages))
        .config("spark.jars.ivy", "/tmp/.ivy2")

        # Resources - Tăng nhẹ để ổn định hơn
        .config("spark.executor.instances", "1")
        .config("spark.executor.cores", "1")
        .config("spark.executor.memory", "1g") 
        .config("spark.driver.memory", "1g")
        .config("spark.memory.overhead.fraction", "0.2") # Tăng bộ nhớ đệm cho K8s pod
        # Tránh lỗi "Committer" khi ghi dữ liệu ra S3/MinIO
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.sql.streaming.checkpointFileManagerClass", "org.apache.spark.sql.execution.streaming.CheckpointFileManager$FileContextManager")
        # MinIO / S3A
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio-service:9000"))
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

        # Streaming & Stability
        .config("spark.sql.shuffle.partitions", "10") # Vì chỉ có 1 executor
        .config("spark.network.timeout", "600s")
        .config("spark.executor.heartbeatInterval", "60s")
        
        .getOrCreate()
    )

    return spark