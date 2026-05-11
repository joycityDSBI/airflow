from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

with DAG(
    "cleanup_logs",
    schedule="0 3 * * 0",  # 매주 일요일 03시
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["maintenance"],
) as dag:
    cleanup = BashOperator(
        task_id="delete_old_logs",
        bash_command=(
            "find /home/devadmin/airflow/logs/ -type f -mtime +14 -delete && "
            "find /home/devadmin/airflow/logs/ -type d -empty -delete"
        ),
    )