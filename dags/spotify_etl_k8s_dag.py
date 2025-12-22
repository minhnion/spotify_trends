from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from pendulum import datetime, timezone
from datetime import timedelta

LOCAL_TZ = timezone("Asia/Ho_Chi_Minh")

@dag(
    dag_id="spotify_ml_pipeline_k8s",
    start_date=datetime(2025, 1, 1, tz=LOCAL_TZ),
    schedule=timedelta(hours=1),
    catchup=False,
    tags=["spotify", "ml", "k8s"],
)
def spotify_ml_pipeline_k8s():

    # Task 1: Train Model
    # Note: Added trailing space to bash_command to prevent Jinja template error
    train_model = BashOperator(
        task_id="train_model",
        bash_command="cd /opt/scripts && ./train_model_on_k8s.sh "
    )

    # Task 2: Precompute Recommendations
    # Note: Added trailing space to bash_command to prevent Jinja template error
    precompute_recommendations = BashOperator(
        task_id="precompute_recommendations",
        bash_command="cd /opt/scripts && ./precompute_recomendations_on_k8s.sh "
    )

    train_model >> precompute_recommendations

spotify_ml_pipeline_k8s()