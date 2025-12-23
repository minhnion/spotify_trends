from airflow.sdk.definitions.dag import dag
from airflow.providers.standard.operators.bash import BashOperator
from pendulum import datetime, timezone
from datetime import timedelta

LOCAL_TZ = timezone("Asia/Ho_Chi_Minh")

@dag(
    dag_id="spotify_compaction_pipeline",
    start_date=datetime(2025, 1, 1, tz=LOCAL_TZ),
    schedule=timedelta(minutes=15),
    catchup=False,
    tags=["spotify", "compaction"],
)
def spotify_compaction_pipeline():

    ping = BashOperator(
        task_id="ping",
        bash_command="""
        echo "=============================="
        date
        echo "COMPACTION PIPELINE IS RUNNING 🚀"
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

    ping >> compact


spotify_compaction_pipeline()
