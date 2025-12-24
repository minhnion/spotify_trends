kubectl delete pods -l spark-role=driver -n spark --force 2>/dev/null
kubectl delete pods -l spark-role=executor -n spark --force 2>/dev/null


spark-submit \
  --master k8s://$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}') \
  --deploy-mode cluster \
  --name spark-kafka \
  --conf spark.executor.instances=2 \
  --conf spark.kubernetes.namespace=spotify \
  --conf spark.kubernetes.container.image=spotify-spark:latest \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio-service.spotify.svc.cluster.local:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-aws:3.3.4 \
  local:////opt/spark/work-dir/spark_jobs/streaming/run_streaming_k8s.py