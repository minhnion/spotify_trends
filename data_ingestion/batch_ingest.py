import os 
import glob
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error

def run_batch_ingestion():
    """
    Main function to connect to MinIO and upload files.
    """
    load_dotenv()

    endpoint = os.getenv("MINIO_ENDPOINT")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")

    client = Minio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )

    bucket_name = "spotify-raw-data"
    local_data_path = "./data/"

    try:
        found = client.bucket_exists(bucket_name=bucket_name)
        if not found:
            print(f"Creating bucket '{bucket_name}'...")
            client.make_bucket(bucket_name)
        else:
            print(f"Bucket '{bucket_name}' already exists.")
    except S3Error as exc:
        print("Error occurred.", exc)
        return
    
    json_files = glob.glob(os.path.join(local_data_path, "*.json"))

    print(f"Found {len(json_files)} JSON files to upload.")

    for file_path in json_files:
        file_name = os.path.basename(file_path)
        try:
            client.fput_object(
                bucket_name=bucket_name,
                object_name=file_name,
                file_path=file_path,
            )
            print(f"Successfully uploaded {file_name}.")
        except S3Error as exc:
            print(f"Error uploading {file_name}.", exc)

if __name__ == "__main__":
    run_batch_ingestion()