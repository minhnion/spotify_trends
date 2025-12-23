#!/bin/bash
set -e

echo "-------------------------------------------------------"
echo "Submitting Spark Streaming job to Kubernetes..."
echo "-------------------------------------------------------"

# 1. Lấy URI của Kubernetes Master
MASTER_URI=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')
K8S_MASTER_URL="k8s://${MASTER_URI}"

echo "Connecting to Kubernetes Master at: $K8S_MASTER_URL"

# 2. Xóa các pod driver cũ của streaming job (nếu có) để tránh xung đột
kubectl delete pod -n spotify -l spark-role=driver,app=spotify-streaming --ignore-not-found=true

# 3. Thực hiện spark-submit
# Lưu ý: Phiên bản package spark-sql-kafka nên khớp với phiên bản Spark trong image của bạn (ở đây giả định là 3.5.x)
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
  --conf spark.executor.memory=512m \
  --conf spark.driver.memory=512m \
  \
  --conf spark.kubernetes.driver.label.app=spotify-streaming \
  \
  --conf spark.kubernetes.driver.secretKeyRef.MINIO_ACCESS_KEY=minio-secret:MINIO_ROOT_USER \
  --conf spark.kubernetes.driver.secretKeyRef.MINIO_SECRET_KEY=minio-secret:MINIO_ROOT_PASSWORD \
  --conf spark.kubernetes.executor.secretKeyRef.MINIO_ACCESS_KEY=minio-secret:MINIO_ROOT_USER \
  --conf spark.kubernetes.executor.secretKeyRef.MINIO_SECRET_KEY=minio-secret:MINIO_ROOT_PASSWORD \
  \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  \
  local:///opt/spark/work-dir/spark_jobs/streaming/run_streaming.py

echo ""
echo "Streaming Job submitted successfully!"
echo "Monitor with: kubectl logs -n spotify -l app=spotify-streaming -f"