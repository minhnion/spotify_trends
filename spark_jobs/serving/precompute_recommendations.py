import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from pyspark.ml import PipelineModel
from pyspark.sql.functions import col, collect_list
from pyspark.sql.functions import explode

from spark_jobs.utils.write_to_redis import write_to_redis
from spark_jobs.utils.spark_session import create_spark_session


def run_precomputation_job():
    spark = create_spark_session()

    model_path = "s3a://spotify-models/als_model"
    data_path = "s3a://spotify-processed-data/"

    print("Loading trained ALS model...")
    model = PipelineModel.load(model_path)

    print("Generating top 10 recommendations for each playlist...")
    als_model = model.stages[-1]

    # Tạo gợi ý cho tất cả các user (playlist)
    recommendations = als_model.recommendForAllUsers(10)

    # --- Ánh xạ ID số ngược lại thành URI ---

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
    recs_with_pid = recommendations.join(pid_map_df, recommendations.pid_numeric == pid_map_df.pid_numeric).select("pid", "recommendations")

    # 4. Join để lấy track_uri gốc
    recs_with_tracks = recs_with_pid.withColumn("rec", explode("recommendations")) \
        .select("pid", col("rec.track_numeric").alias("track_numeric"))
    
    recs_with_track_uri = recs_with_tracks.join(track_map_df, "track_numeric").select("pid", "track_uri")

    # 5. Gom các track_uri lại thành một danh sách cho mỗi pid
    final_recs = recs_with_track_uri.groupBy("pid").agg(collect_list("track_uri").alias("recommendations"))
    
    print("Writing recommendations to Redis...")
    final_recs.foreachPartition(write_to_redis)
    
    print("Precomputation job completed successfully!")
    spark.stop()

if __name__ == "__main__":
    run_precomputation_job()