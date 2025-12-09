set -e
echo "Submitting Spark Streaming job..."

PROJECT_ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.780,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
  "$PROJECT_ROOT_DIR/spark_jobs/streaming/run_streaming.py"