#!/bin/bash

set -e

echo "Submitting Spark model training job..."

PROJECT_ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
echo "Project root directory: $PROJECT_ROOT_DIR"

spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.780 \
  --conf spark.hadoop.fs.s3a.connection.timeout=200000 \
  --conf spark.hadoop.fs.s3a.connection.establish.timeout=5000 \
  "$PROJECT_ROOT_DIR/spark_jobs/model/train_model.py"