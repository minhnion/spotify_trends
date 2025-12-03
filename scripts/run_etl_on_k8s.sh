#!/bin/bash
set -e

echo "Submitting Spark ETL job to Kubernetes..."

# Lấy địa chỉ của Kubernetes master từ minikube
K8S_MASTER_URL="k8s://https://$(minikube ip):8443"

spark-submit \
  --master $K8S_MASTER_URL \
  --deploy-mode cluster \
  --name spotify-etl \
  --conf spark.kubernetes.namespace=spotify \
  --conf spark.kubernetes.container.image=spark-jobs:latest \
  --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio-service:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --packages org.apache.hadoop:hadoop-aws:3.4.1 \
  local:///opt/spark/work-dir/spark_jobs/etl/run_etl.py