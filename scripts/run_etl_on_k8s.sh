#!/bin/bash
set -e

echo "Submitting Spark ETL job to Kubernetes..."

# === SỬA LỖI KẾT NỐI TẠI ĐÂY ===
# Không dùng $(minikube ip) nữa vì máy host không ping được.
# Lấy trực tiếp địa chỉ server từ cấu hình kubectl đang hoạt động (ví dụ: https://127.0.0.1:35303)
MASTER_URI=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')
K8S_MASTER_URL="k8s://${MASTER_URI}"

echo "Connecting to Kubernetes Master at: $K8S_MASTER_URL"

# Xóa pod cũ để sạch sẽ
kubectl delete pod spotify-etl-driver -n spotify --ignore-not-found=true

spark-submit \
  --master $K8S_MASTER_URL \
  --deploy-mode cluster \
  --name spotify-etl \
  --conf spark.kubernetes.namespace=spotify \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --conf spark.kubernetes.container.image=spark-jobs:latest \
  --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent \
  \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio-service:9000 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  \
  --conf spark.kubernetes.driver.secretKeyRef.MINIO_ACCESS_KEY=minio-secret:MINIO_ROOT_USER \
  --conf spark.kubernetes.driver.secretKeyRef.MINIO_SECRET_KEY=minio-secret:MINIO_ROOT_PASSWORD \
  --conf spark.kubernetes.executor.secretKeyRef.MINIO_ACCESS_KEY=minio-secret:MINIO_ROOT_USER \
  --conf spark.kubernetes.executor.secretKeyRef.MINIO_SECRET_KEY=minio-secret:MINIO_ROOT_PASSWORD \
  \
  --packages org.apache.hadoop:hadoop-aws:3.3.4 \
  local:///opt/spark/work-dir/spark_jobs/etl/run_etl.py