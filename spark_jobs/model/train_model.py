import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from pyspark.sql.functions import col, lit
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline, PipelineModel
from spark_jobs.utils.spark_session import create_spark_session


def run_model_training_job():
    spark = None
    try:
        print("=" * 80)
        print("Starting Model Training Job...")
        print("=" * 80)
        
        spark = create_spark_session()
        
        input_path = "s3a://spotify-processed-data/spotify_tracks"
        model_output_path = "s3a://spotify-models/als_model"
        
        print(f"\nStep 1: Reading processed data from {input_path}...")
        df = spark.read.parquet(input_path)
        print(f"✓ Loaded {df.count()} rows")
        df.printSchema()
        
        # Feature engineering
        print("\nStep 2: Setting up feature indexers...")
        pid_indexer = StringIndexer(
            inputCol="pid", 
            outputCol="pid_numeric", 
            handleInvalid="skip"
        )
        track_indexer = StringIndexer(
            inputCol="track_uri", 
            outputCol="track_numeric", 
            handleInvalid="skip"
        )
        
        # Add rating column (implicit feedback = 1.0)
        print("\nStep 3: Adding rating column...")
        df_with_rating = df.withColumn("rating", lit(1.0))
        print(f"✓ Rating column added")
        
        # Split data
        print("\nStep 4: Splitting data (80/20)...")
        (training_data, validation_data) = df_with_rating.randomSplit([0.8, 0.2], seed=42)
        
        # Cache for performance
        training_data.cache()
        validation_data.cache()
        
        train_count = training_data.count()
        val_count = validation_data.count()
        print(f"✓ Training data: {train_count} rows")
        print(f"✓ Validation data: {val_count} rows")
        
        # Configure ALS model
        print("\nStep 5: Training ALS model...")
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
        
        pipeline = Pipeline(stages=[pid_indexer, track_indexer, als])
        
        # Train model
        print("  - Training in progress...")
        model = pipeline.fit(training_data)
        print("✓ Model trained successfully")
        
        # Make predictions
        print("\nStep 6: Evaluating model...")
        predictions = model.transform(validation_data)
        
        # Evaluate
        evaluator = RegressionEvaluator(
            metricName="rmse",
            labelCol="rating",
            predictionCol="prediction"
        )
        rmse = evaluator.evaluate(predictions)
        print(f"✓ Model RMSE: {rmse:.4f}")
        
        print(f"\nStep 7: Saving model to {model_output_path}...")
        model.write().overwrite().save(model_output_path)
        print("✓ Model saved successfully")
        
        # Show sample predictions
        print("\nSample predictions:")
        predictions.select("pid", "track_uri", "rating", "prediction").show(10, truncate=False)
        
        print("\n" + "=" * 80)
        print("✅ MODEL TRAINING JOB COMPLETED SUCCESSFULLY!")
        print("=" * 80)
        print(f"✓ Training samples: {train_count}")
        print(f"✓ Validation samples: {val_count}")
        print(f"✓ RMSE: {rmse:.4f}")
        print(f"✓ Model saved to: {model_output_path}")
        print("=" * 80 + "\n")
        
    except Exception as e:
        print("\n" + "=" * 80)
        print("❌ ERROR IN MODEL TRAINING JOB")
        print("=" * 80)
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
        
    finally:
        # Cleanup
        if spark:
            try:
                training_data.unpersist()
                validation_data.unpersist()
            except:
                pass
            print("Stopping Spark session...")
            spark.stop()
            print("✓ Spark session stopped\n")


if __name__ == "__main__":
    run_model_training_job()