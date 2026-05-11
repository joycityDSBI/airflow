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
            "LOG_DIR=${AIRFLOW__LOGGING__BASE_LOG_FOLDER:-/opt/airflow/logs} && "
            "echo \"로그 디렉토리: $LOG_DIR\" && "
            "find \"$LOG_DIR\" -type f -mtime +14 -delete && "
            "find \"$LOG_DIR\" -type d -empty -not -path \"$LOG_DIR\" -delete && "
            "echo \"로그 정리 완료\""
        ),
    )