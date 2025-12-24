spark-submit \
  --master k8s://$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}') \
  --deploy-mode cluster \
  --name spotify-data-job \
  --conf spark.kubernetes.namespace=spotify \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --conf spark.kubernetes.container.image=spotify-spark:latest \
  --conf spark.kubernetes.container.image.pullPolicy=Never \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  \
  --conf spark.driver.memory=800m \
  --conf spark.executor.memory=1g \
  --conf spark.executor.instances=1 \
  \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio-service.spotify.svc.cluster.local:9000 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  \
  --conf spark.kubernetes.driver.secretKeyRef.MINIO_ACCESS_KEY=minio-secret:MINIO_ROOT_USER \
  --conf spark.kubernetes.driver.secretKeyRef.MINIO_SECRET_KEY=minio-secret:MINIO_ROOT_PASSWORD \
  --conf spark.kubernetes.executor.secretKeyRef.MINIO_ACCESS_KEY=minio-secret:MINIO_ROOT_USER \
  --conf spark.kubernetes.executor.secretKeyRef.MINIO_SECRET_KEY=minio-secret:MINIO_ROOT_PASSWORD \
  \
  --conf spark.kubernetes.driver.secretKeyRef.MONGO_URI=mongodb-secret:MONGO_URI \
  --conf spark.kubernetes.driver.secretKeyRef.MONGO_DATABASE=mongodb-secret:MONGO_DATABASE \
  --conf spark.kubernetes.executor.secretKeyRef.MONGO_URI=mongodb-secret:MONGO_URI \
  --conf spark.kubernetes.executor.secretKeyRef.MONGO_DATABASE=mongodb-secret:MONGO_DATABASE \
  \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.mongodb.spark:mongo-spark-connector_2.12:10.2.0 \
  \
  local:///opt/spark/work-dir/spark_jobs/streaming/streaming_to_db_k8s.py