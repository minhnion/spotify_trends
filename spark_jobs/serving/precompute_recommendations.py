import sys
import os
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from pyspark.ml import PipelineModel
from pyspark.sql.functions import col, collect_list, explode
from spark_jobs.utils.write_to_redis import write_to_redis

if os.getenv("KUBERNETES_SERVICE_HOST"):
    print("🚀 Running in Kubernetes environment")
    from spark_jobs.utils.spark_session_k8s import create_spark_session
else:
    print("💻 Running in local environment")
    from spark_jobs.utils.spark_session import create_spark_session


def run_precomputation_job():
    spark = None
    try:
        print("=" * 80)
        print("Starting Precomputation Job...")
        print("=" * 80)
        
        spark = create_spark_session()
        
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        print("\n=== FINAL HADOOP CONFIG CHECK ===")
        print(f"fs.s3a.endpoint: {hadoop_conf.get('fs.s3a.endpoint')}")
        print(f"fs.s3a.access.key: {hadoop_conf.get('fs.s3a.access.key')}")
        print("=" * 80 + "\n")
        
        model_path = "s3a://spotify-models/als_model"
        
        print(f"Step 1: Loading trained ALS model from {model_path}...")
        model = PipelineModel.load(model_path)
        print("✓ Model loaded successfully")
        
        print("\nStep 2: Generating top 10 recommendations for each playlist...")
        als_model = model.stages[-1]
        
        recommendations = als_model.recommendForAllUsers(10)
        print(f"✓ Generated recommendations for {recommendations.count()} playlists")
        
        print("\nStep 3: Mapping numeric IDs back to URIs...")
        
        # 1. Lấy model StringIndexer đã được huấn luyện từ pipeline
        pid_indexer_model = model.stages[0]
        track_indexer_model = model.stages[1]
        
        # 2. Tạo DataFrame chứa mapping từ ID số -> URI
        pid_labels = pid_indexer_model.labels
        pid_map_df = spark.createDataFrame(
            enumerate(pid_labels),
            ["pid_numeric", "pid"]
        )
        
        track_labels = track_indexer_model.labels
        track_map_df = spark.createDataFrame(
            enumerate(track_labels),
            ["track_numeric", "track_uri"]
        )
        
        # 3. Join để lấy pid gốc
        recs_with_pid = recommendations.join(
            pid_map_df, 
            recommendations.pid_numeric == pid_map_df.pid_numeric
        ).select("pid", "recommendations")
        
        # 4. Join để lấy track_uri gốc
        recs_with_tracks = recs_with_pid.withColumn("rec", explode("recommendations")) \
            .select("pid", col("rec.track_numeric").alias("track_numeric"))
        
        recs_with_track_uri = recs_with_tracks.join(
            track_map_df, 
            "track_numeric"
        ).select("pid", "track_uri")
        
        # 5. Gom các track_uri lại thành một danh sách cho mỗi pid
        final_recs = recs_with_track_uri.groupBy("pid").agg(
            collect_list("track_uri").alias("recommendations")
        )
        
        print(f"✓ Mapped {final_recs.count()} playlists with recommendations")
        
        # Show sample
        print("\nSample recommendations:")
        final_recs.show(5, truncate=False)
        
        print("\nStep 4: Writing recommendations to Redis...")
        final_recs.foreachPartition(write_to_redis)
        print("✓ All recommendations written to Redis")
        
        print("\n" + "=" * 80)
        print("✅ PRECOMPUTATION JOB COMPLETED SUCCESSFULLY!")
        print("=" * 80 + "\n")
        
    except Exception as e:
        print("\n" + "=" * 80)
        print("❌ ERROR IN PRECOMPUTATION JOB")
        print("=" * 80)
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
        
    finally:
        if spark:
            print("Stopping Spark session...")
            spark.stop()
            print("✓ Spark session stopped\n")


if __name__ == "__main__":
    run_precomputation_job()