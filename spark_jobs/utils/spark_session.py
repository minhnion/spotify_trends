import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession

def create_spark_session(app_name="SpotifyETL"):
    """
    Create and config Spark session with MinIO settings and Local JARs.
    """
    # 1. Xác định đường dẫn Project Root và thư mục JARs
    # Cấu trúc: project_root/spark_jobs/utils/spark_session.py -> đi ngược lên 3 cấp
    current_file = Path(__file__).resolve()
    project_root = current_file.parent.parent.parent
    dotenv_path = project_root / '.env'
    jars_dir = project_root / "jars"

    # 2. Load biến môi trường
    load_dotenv(dotenv_path=dotenv_path)
    
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin") # Fallback mặc định
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    
    print(f"\n=== Configuring Spark Session ===")
    print(f"📂 Project Root: {project_root}")
    print(f"📂 Jars Directory: {jars_dir}")
    
    # 3. Lấy danh sách file JAR trong thư mục jars/
    # Đây là bước QUAN TRỌNG NHẤT để fix lỗi BulkDelete
    jar_files = []
    if jars_dir.exists():
        jar_files = [str(f) for f in jars_dir.glob("*.jar")]
        print(f"✅ Found {len(jar_files)} JAR files locally.")
    else:
        print(f"⚠️ WARNING: Jars directory not found at {jars_dir}")
        # Nếu không thấy folder jars, code sẽ chạy bằng thư viện mặc định (dễ lỗi)

    # Nối danh sách file thành chuỗi, cách nhau bởi dấu phẩy
    jars_conf = ",".join(jar_files)

    # 4. Khởi tạo Spark Session
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.driver.memory", "4g") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.jars", jars_conf)  # <--- DÒNG QUAN TRỌNG: Ép dùng JAR local

    # Nếu không chạy trên Kubernetes, set master là local
    if "KUBERNETES_SERVICE_HOST" not in os.environ:
        builder = builder.master("local[*]")

    spark = builder.getOrCreate()
    
    # 5. Verify config (Optional but good for debug)
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    print("\n=== Verifying Hadoop Configuration ===")
    print(f"fs.s3a.endpoint: {hadoop_conf.get('fs.s3a.endpoint')}")
    print(f"Spark Jars Loaded: {len(jar_files)} files")
    print("="*50 + "\n")
    
    return spark