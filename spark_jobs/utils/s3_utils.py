import os
from urllib.parse import urlparse
from minio import Minio
from minio.error import S3Error

def ensure_s3_bucket_exists(s3_path: str):
    """
    Ensures that the S3 bucket specified in the s3_path exists using MinIO client.
    If the bucket does not exist, it attempts to create it.
    """
    parsed_url = urlparse(s3_path)
    if parsed_url.scheme != 's3a':
        print(f"Skipping bucket check for non-S3 path: {s3_path}")
        return

    bucket_name = parsed_url.netloc
    if not bucket_name:
        print(f"Invalid S3 path, no bucket name found: {s3_path}")
        return

    endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

    # Minio client expects endpoint without scheme
    secure = False
    if endpoint.startswith("http://"):
        endpoint = endpoint.replace("http://", "")
    elif endpoint.startswith("https://"):
        endpoint = endpoint.replace("https://", "")
        secure = True

    try:
        client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
            cert_check=False
        )
        
        if not client.bucket_exists(bucket_name):
            print(f"S3 bucket '{bucket_name}' does not exist. Creating it...")
            client.make_bucket(bucket_name)
            print(f"S3 bucket '{bucket_name}' created successfully.")
        else:
            print(f"S3 bucket '{bucket_name}' already exists.")
            
    except S3Error as e:
        print(f"Error checking/creating bucket '{bucket_name}': {e}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred with S3 bucket '{bucket_name}': {e}")
        raise