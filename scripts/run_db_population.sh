#!/bin/bash
set -e

echo "Submitting Spark Database Population job..."

PROJECT_ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "Project root directory: $PROJECT_ROOT_DIR"

spark-submit \
--packages org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.780,org.postgresql:postgresql:42.6.0 \
--conf spark.hadoop.fs.s3a.connection.timeout=200000 \
--conf spark.hadoop.fs.s3a.connection.establish.timeout=5000 \
"$PROJECT_ROOT_DIR/spark_jobs/loading/populate_database.py"

echo "Spark DB Population job submitted successfully."