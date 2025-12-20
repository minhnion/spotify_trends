from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from pendulum import datetime, timezone
from datetime import timedelta
import os

LOCAL_TZ = timezone("Asia/Ho_Chi_Minh")

# Define path to scripts
SCRIPTS_DIR = "/opt/scripts"

@dag(
    dag_id="spotify_ml_pipeline_k8s",
    start_date=datetime(2025, 1, 1, tz=LOCAL_TZ),
    schedule=None, # Trigger manually only usually for K8s jobs locally
    catchup=False,
    tags=["spotify", "ml", "k8s"],
)
def spotify_ml_pipeline_k8s():

    ping = BashOperator(
        task_id="ping",
        bash_command="""
        echo "=============================="
        date
        echo "KUBERNETES PIPELINE IS RUNNING 🚀"
        echo "=============================="
        """
    )

    # Note: For this to work, the Airflow Worker container must have:
    # 1. 'kubectl' installed
    # 2. A valid KUBECONFIG accessible (e.g. mounted volume)
    # 3. Network access to the K8s API server
    train_model_k8s = BashOperator(
        task_id="train_model_k8s",
        bash_command=f"""
        echo "Starting K8s training job..."
        cd {SCRIPTS_DIR} && bash train_model_on_k8s.sh
        """
    )

    precompute_recommendations_k8s = BashOperator(
        task_id="precompute_recommendations_k8s",
        bash_command=f"""
        echo "Starting K8s precompute job..."
        cd {SCRIPTS_DIR} && bash precompute_recomendations_on_k8s.sh
        """
    )

    ping >> train_model_k8s >> precompute_recommendations_k8s


spotify_ml_pipeline_k8s()
