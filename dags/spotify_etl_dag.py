from airflow.sdk.definitions.dag import dag
from airflow.providers.standard.operators.bash import BashOperator
from pendulum import datetime, timezone
from datetime import timedelta

LOCAL_TZ = timezone("Asia/Ho_Chi_Minh")

@dag(
    dag_id="spotify_ml_pipeline",
    start_date=datetime(2025, 1, 1, tz=LOCAL_TZ),
    schedule=timedelta(hours=1),
    catchup=False,
    tags=["spotify", "ml"],
)
def spotify_ml_pipeline():

    ping = BashOperator(
        task_id="ping",
        bash_command="""
        echo "=============================="
        date
        echo "AIRFLOW PIPELINE IS RUNNING 🚀"
        echo "=============================="
        """
    )

    compact = BashOperator(
        task_id="compact",
        bash_command="""
        echo "Starting compaction job..."
        cd /opt/scripts && bash compact.sh
        """
    )

    train_model = BashOperator(
        task_id="train_model",
        bash_command="""
        echo "Starting training job..."
        cd /opt/scripts && bash train_model_job.sh
        """
    )

    precompute_recommendations = BashOperator(
        task_id="precompute_recommendations",
        bash_command="""
        echo "Starting precompute job..."
        cd /opt/scripts && bash precompute_recomendations_job.sh
        """
    )

    ping >> compact >> train_model >> precompute_recommendations


spotify_ml_pipeline()
