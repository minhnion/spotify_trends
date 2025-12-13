set -e

echo "=================================================="
echo "SUBMITTING DATABASE POPULATION JOB TO KUBERNETES"
echo "=================================================="

MASTER_URI=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')
K8S_MASTER_URL="k8s://${MASTER_URI}"
echo "Connecting to Kubernetes Master at: $K8S_MASTER_URL"

echo "Cleaning up old driver pods..."
kubectl delete pod -n spotify -l spark-role=driver,spark-app-name=spotify-db-population --ignore-not-found=true

echo "Checking MongoDB secret..."
if ! kubectl get secret mongodb-secret -n spotify &>/dev/null; then
    echo "WARNING: Secret 'mongodb-secret' not found. Creating it now..."
    kubectl create secret generic mongodb-secret -n spotify \
        --from-literal=MONGO_URI='mongodb+srv://nguyenhoangviethung_db_user:QPB5DBekdXmI68rn@cluster0.fuxyyc0.mongodb.net' \
        --from-literal=MONGO_DATABASE='spotify_trends'
    echo "✓ Secret created successfully!"
else
    echo "✓ Secret 'mongodb-secret' already exists."
fi

echo ""
echo "Submitting Spark job..."
echo ""

spark-submit \
    --master $K8S_MASTER_URL \
    --deploy-mode cluster \
    --name spotify-db-population \
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
    --conf spark.kubernetes.driver.secretKeyRef.MONGO_URI=mongodb-secret:MONGO_URI \
    --conf spark.kubernetes.driver.secretKeyRef.MONGO_DATABASE=mongodb-secret:MONGO_DATABASE \
    --conf spark.kubernetes.driver.env.KUBERNETES_SERVICE_HOST=kubernetes.default.svc \
    --conf spark.kubernetes.executor.secretKeyRef.MINIO_ACCESS_KEY=minio-secret:MINIO_ROOT_USER \
    --conf spark.kubernetes.executor.secretKeyRef.MINIO_SECRET_KEY=minio-secret:MINIO_ROOT_PASSWORD \
    --conf spark.kubernetes.executor.secretKeyRef.MONGO_URI=mongodb-secret:MONGO_URI \
    --conf spark.kubernetes.executor.secretKeyRef.MONGO_DATABASE=mongodb-secret:MONGO_DATABASE \
    --conf spark.kubernetes.executor.env.KUBERNETES_SERVICE_HOST=kubernetes.default.svc \
    \
    --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    \
    local:///opt/spark/work-dir/spark_jobs/loading/populate_database.py

echo ""
echo "=================================================="
echo "✅ Job submitted successfully!"
echo "=================================================="
echo ""
echo "Monitor job with:"
echo "  kubectl logs -n spotify -l spark-role=driver -f"
echo ""
echo "Or find the driver pod:"
echo "  kubectl get pods -n spotify -l spark-role=driver"
echo ""
echo "Get detailed pod info:"
echo "  kubectl describe pod -n spotify -l spark-role=driver"
echo ""