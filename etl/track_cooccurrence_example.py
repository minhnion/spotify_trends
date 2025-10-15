# Minimal Spark job: compute track co-occurrence counts and write top-k per track
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, row_number
from pyspark.sql.window import Window

def main():
    spark = SparkSession.builder.appName("cooccurrence_example").getOrCreate()
    playlists = spark.read.parquet("data/curated/playlists_parquet")
    # assume playlists has 'pid' and 'tracks' (array of structs with 'track_uri')
    exploded = playlists.select(col("pid"), explode(col("tracks")).alias("t")).select(
        col("pid"),
        col("t.track_uri").alias("track_uri")
    )
    a = exploded.alias("a")
    b = exploded.alias("b")
    pairs = a.join(b, on="pid").where(col("a.track_uri") != col("b.track_uri")).select(
        col("a.track_uri").alias("t1"),
        col("b.track_uri").alias("t2")
    )
    counts = pairs.groupBy("t1", "t2").count()
    window = Window.partitionBy("t1").orderBy(col("count").desc())
    ranked = counts.withColumn("rn", row_number().over(window)).filter(col("rn") <= 50)
    ranked.write.mode("overwrite").parquet("data/curated/cooccurrence_top50")
    spark.stop()

if __name__ == '__main__':
    main()
