#!/bin/bash
set -e

echo "Submitting Spark Streaming job to Kubernetes..."

# Lấy địa chỉ Master URI từ cấu hình kubectl hiện tại
MASTER_URI=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')
K8S_MASTER_URL="k8s://${MASTER_URI}"

echo "Connecting to Kubernetes Master at: $K8S_MASTER_URL"

# (Tùy chọn) Xóa driver pod cũ của job streaming nếu có, để tránh nhầm lẫn
kubectl delete pod -n spotify -l spark-app-selector=spark-app-spotify-streaming --ignore-not-found=true

# Lệnh spark-submit được điều chỉnh cho streaming job
spark-submit \
  --master $K8S_MASTER_URL \
  --deploy-mode cluster \
  --name spotify-streaming \
  --conf spark.kubernetes.namespace=spotify \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --conf spark.kubernetes.container.image=spark-jobs:latest \
  --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent \
  \
  --conf spark.executor.instances=2 \
  --conf spark.executor.memory=1g \
  --conf spark.driver.memory=1g \
  \
  --conf spark.kubernetes.driver.secretKeyRef.MINIO_ACCESS_KEY=minio-secret:MINIO_ROOT_USER \
  --conf spark.kubernetes.driver.secretKeyRef.MINIO_SECRET_KEY=minio-secret:MINIO_ROOT_PASSWORD \
  --conf spark.kubernetes.executor.secretKeyRef.MINIO_ACCESS_KEY=minio-secret:MINIO_ROOT_USER \
  --conf spark.kubernetes.executor.secretKeyRef.MINIO_SECRET_KEY=minio-secret:MINIO_ROOT_PASSWORD \
  \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  \
  local:///opt/spark/work-dir/spark_jobs/streaming/run_streaming.py

echo ""
echo "Streaming job submitted successfully!"
echo "A driver pod named 'spotify-streaming-...' will be created."
echo "Monitor with: kubectl logs -n spotify -l spark-app-selector=spark-app-spotify-streaming -f"