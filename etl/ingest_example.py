# PySpark example: read MPD JSON and write Parquet (minimal example)
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col

def main():
    spark = SparkSession.builder.appName("mpd_ingest_example").getOrCreate()
    # Adjust path to your MPD JSON(s)
    input_path = "data/mpd_json/*.json"
    output_path = "data/curated/playlists_parquet"
    try:
        raw = spark.read.json(input_path)
        # Basic validation: keep playlists with tracks
        raw_filtered = raw.filter(col("num_tracks") > 0)
        # write parquet
        raw_filtered.write.mode("overwrite").parquet(output_path)
        print("Wrote parquet to", output_path)
    finally:
        spark.stop()

if __name__ == '__main__':
    main()
