#!/bin/bash
set -e

echo "Submitting Spark Model Training job to Kubernetes (Low Memory Mode)..."

MASTER_URI=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')
K8S_MASTER_URL="k8s://${MASTER_URI}"

echo "Connecting to Kubernetes Master at: $K8S_MASTER_URL"

kubectl delete pod -n spotify -l spark-app-name=spotify-model-training --ignore-not-found=true
kubectl delete pod -n spotify -l spark-role=executor --ignore-not-found=true

echo "Waiting for old pods to terminate..."
sleep 5

spark-submit \
  --master $K8S_MASTER_URL \
  --deploy-mode cluster \
  --name spotify-model-training \
  --conf spark.kubernetes.namespace=spotify \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --conf spark.kubernetes.container.image=spark-jobs:latest \
  --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent \
  \
  --conf spark.executor.instances=1 \
  --conf spark.executor.memory=512m \
  --conf spark.driver.memory=512m \
  --conf spark.executor.cores=1 \
  --conf spark.kubernetes.executor.request.cores=0.5 \
  --conf spark.kubernetes.driver.request.cores=0.5 \
  --conf spark.kubernetes.executor.limit.cores=1 \
  --conf spark.kubernetes.driver.limit.cores=1 \
  \
  --conf spark.kubernetes.driver.secretKeyRef.MINIO_ACCESS_KEY=minio-secret:MINIO_ROOT_USER \
  --conf spark.kubernetes.driver.secretKeyRef.MINIO_SECRET_KEY=minio-secret:MINIO_ROOT_PASSWORD \
  --conf spark.kubernetes.executor.secretKeyRef.MINIO_ACCESS_KEY=minio-secret:MINIO_ROOT_USER \
  --conf spark.kubernetes.executor.secretKeyRef.MINIO_SECRET_KEY=minio-secret:MINIO_ROOT_PASSWORD \
  \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  \
  local:///opt/spark/work-dir/spark_jobs/model/train_model.py

echo ""
echo "Job submitted successfully!"
echo ""
echo "Check executors: kubectl get pods -n spotify | grep executor"
echo "Monitor logs: kubectl logs -n spotify -l spark-app-name=spotify-model-training -f"