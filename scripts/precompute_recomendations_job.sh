#!/bin/bash

set -e

echo "Submitting Spark recommendation precomputation job..."

PROJECT_ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
echo "Project root directory: $PROJECT_ROOT_DIR"

# Zip the spark_jobs directory to send to workers (using Python to avoid dependency on 'zip' utility)
cd "$PROJECT_ROOT_DIR"
python3 -c "import shutil; shutil.make_archive('/tmp/spark_jobs', 'zip', '.', 'spark_jobs')"
echo "Zipped spark_jobs to /tmp/spark_jobs.zip"

spark-submit \
  --jars "$PROJECT_ROOT_DIR/jars/hadoop-aws-3.3.4.jar,$PROJECT_ROOT_DIR/jars/aws-java-sdk-bundle-1.12.262.jar" \
  --py-files /tmp/spark_jobs.zip \
  --conf spark.hadoop.fs.s3a.connection.timeout=200000 \
  --conf spark.hadoop.fs.s3a.connection.establish.timeout=5000 \
  "$PROJECT_ROOT_DIR/spark_jobs/serving/precompute_recommendations.py"