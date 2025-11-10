from pyspark.sql.functions import col, lit
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from spark_jobs.utils.spark_session import create_spark_session

def run_model_traning_job():
    spark = create_spark_session()

    input_path = "s3a://spotify-processed-data/"
    model_output_path = "s3a://spotify-models/als_model"

    print(f"Reading processed data from {input_path}...")
    df = spark.read.parquet(input_path)

    # Feature engineer
    pid_indexer = StringIndexer(inputCol="pid", outputCol="pid_numeric", handleInvalid="skip")
    track_indexer = StringIndexer(inputCol="track_uri", outputCol="track_numeric", handleInvalid="skip")
    df_with_rating = df.withColumn("rating", lit(1.0))

    # Spilit
    (training_data, validation_data) = df_with_rating.randomSplit([0.8, 0.2], seed=42)
    training_data.cache()
    validation_data.cache()

    # Model
    als = ALS(
        userCol="pid_numeric",
        itemCol="track_numeric",
        ratingCol="rating",
        coldStartStrategy="drop",
        implicitPrefs=True,
        rank=10,
        maxIter=10,
        regParam=0.1
    )

    pipeline = Pipeline(stages=[pid_indexer, track_indexer, all])

    model = pipeline.fit(training_data)

    predictions = model.transform(validation_data)
    evaluator = RegressionEvaluator(
        metricName="rmse",
        labelCol="rating",
        predictionCol="prediction"
    )
    mse = evaluator.evaluate(predictions)

    model.write().overwrite().save(model_output_path)

    print("Model training job completed successfully!")
    
    training_data.unpersist()
    validation_data.unpersist()
    spark.stop()

if __name__=="__main__":
    run_model_traning_job()