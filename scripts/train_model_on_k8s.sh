# #!/bin/bash
# set -e

# echo "Submitting Spark Model Training job to Kubernetes (Low Memory Mode)..."

# MASTER_URI=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')
# K8S_MASTER_URL="k8s://${MASTER_URI}"

# echo "Connecting to Kubernetes Master at: $K8S_MASTER_URL"

# kubectl delete pod -n spotify -l spark-app-name=spotify-model-training --ignore-not-found=true
# kubectl delete pod -n spotify -l spark-role=executor --ignore-not-found=true

# echo "Waiting for old pods to terminate..."
# sleep 5

# spark-submit \
#   --master $K8S_MASTER_URL \
#   --deploy-mode cluster \
#   --name spotify-model-training \
#   --conf spark.kubernetes.namespace=spotify \
#   --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
#   --conf spark.kubernetes.container.image=spark-jobs:latest \
#   --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent \
#   \
#   --conf spark.executor.instances=1 \
#   --conf spark.executor.memory=512m \
#   --conf spark.driver.memory=512m \
#   --conf spark.executor.cores=1 \
#   --conf spark.kubernetes.executor.request.cores=0.5 \
#   --conf spark.kubernetes.driver.request.cores=0.5 \
#   --conf spark.kubernetes.executor.limit.cores=1 \
#   --conf spark.kubernetes.driver.limit.cores=1 \
#   \
#   --conf spark.kubernetes.driver.secretKeyRef.MINIO_ACCESS_KEY=minio-secret:MINIO_ROOT_USER \
#   --conf spark.kubernetes.driver.secretKeyRef.MINIO_SECRET_KEY=minio-secret:MINIO_ROOT_PASSWORD \
#   --conf spark.kubernetes.executor.secretKeyRef.MINIO_ACCESS_KEY=minio-secret:MINIO_ROOT_USER \
#   --conf spark.kubernetes.executor.secretKeyRef.MINIO_SECRET_KEY=minio-secret:MINIO_ROOT_PASSWORD \
#   \
#   --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
#   --conf spark.jars.ivy=/tmp/.ivy2 \
#   \
#   local:///opt/spark/work-dir/spark_jobs/model/train_model.py

# echo ""
# echo "Job submitted successfully!"
# echo ""
# echo "Check executors: kubectl get pods -n spotify | grep executor"
# echo "Monitor logs: kubectl logs -n spotify -l spark-app-name=spotify-model-training -f"

#!/bin/bash
set -e

echo "Submitting Spark Model Training job to Kubernetes (Optimized Mode)..."

# Detect if running inside Kubernetes or external
if [ -n "$KUBERNETES_SERVICE_HOST" ]; then
    echo "Detected running inside Kubernetes..."
    K8S_MASTER_URL="k8s://https://$KUBERNETES_SERVICE_HOST:$KUBERNETES_SERVICE_PORT"
else
    # Fallback for local execution
    MASTER_URI=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')
    K8S_MASTER_URL="k8s://${MASTER_URI}"
fi

echo "Connecting to Kubernetes Master at: $K8S_MASTER_URL"

# Xóa các pod cũ để giải phóng tài nguyên trước khi chạy mới
kubectl delete pod -n spotify -l spark-app-name=spotify-model-training --ignore-not-found=true
kubectl delete pod -n spotify -l spark-role=executor --ignore-not-found=true

echo "Waiting for old pods to terminate..."
sleep 5

# Lệnh Spark Submit
spark-submit \
  --master $K8S_MASTER_URL \
  --deploy-mode cluster \
  --name spotify-model-training \
  \
  --conf spark.kubernetes.namespace=spotify \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --conf spark.kubernetes.container.image=spark-jobs:latest \
  --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent \
  \
  --conf spark.executor.instances=1 \
  --conf spark.executor.cores=1 \
  \
  --conf spark.driver.memory=512m \
  --conf spark.executor.memory=512m \
  --conf spark.kubernetes.memoryOverheadFactor=0.2 \
  \
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
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.connection.timeout=5000 \
  --conf spark.hadoop.fs.s3a.attempts.maximum=3 \
  \
  local:///opt/spark/work-dir/spark_jobs/model/train_model.py || exit 1

echo ""
echo "Job submitted successfully!"
echo ""
echo "Check pods status: kubectl get pods -n spotify"
echo "Monitor logs: kubectl logs -n spotify -l spark-app-name=spotify-model-training -f"