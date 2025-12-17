#!/bin/bash
set -e

echo "🚀 Submitting Spark ETL job (Using Local JARs)..."

PROJECT_ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
echo "📂 Project root directory: $PROJECT_ROOT_DIR"

JARS_DIR="$PROJECT_ROOT_DIR/jars"
JARS_LIST=""

if [ -d "$JARS_DIR" ]; then

    for jar in "$JARS_DIR"/*.jar; do
        if [ -z "$JARS_LIST" ]; then
            JARS_LIST="$jar"
        else
            JARS_LIST="$JARS_LIST,$jar"
        fi
    done
    echo "✅ Found local JARs. Loading them into Spark..."
else
    echo "❌ ERROR: Directory '$JARS_DIR' not found!"
    echo "👉 Please run the JAR download script first."
    exit 1
fi

spark-submit \
  --master "local[*]" \
  --driver-memory 4g \
  --jars "$JARS_LIST" \
  --conf spark.hadoop.fs.s3a.connection.timeout=200000 \
  --conf spark.hadoop.fs.s3a.connection.establish.timeout=5000 \
  "$PROJECT_ROOT_DIR/spark_jobs/etl/run_etl.py"

echo "🎉 Spark ETL job finished successfully."