from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
import pendulum
from datetime import datetime, timedelta, timezone
import os
import json
import base64
import uuid
from google.cloud import tasks_v2
from google.api_core.exceptions import AlreadyExists


PROJECT_ID = "aibi-service"
QUEUE_ID = "bq-notion-etl-queue"
LOCATION = "us-central1"
WORKER_URL = "https://bq-notion-etl-346375337876.us-central1.run.app"
SERVICE_ACCOUNT_EMAIL = "aibi-service-etl01@aibi-service.iam.gserviceaccount.com"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

with DAG(
    dag_id = 'dags_aibi_service',
    default_args = default_args,
    description = 'A simple DAG to test BigQueryInsertJobOperator',
    schedule = None,
    start_date = pendulum.datetime(2025, 9, 15, tz="Asia/Seoul"),
    catchup = False,
    tags = ['example']
) as dag:
    
    client = tasks_v2.CloudTasksClient()


    def _queue_path() -> str:
        return client.queue_path(PROJECT_ID, LOCATION, QUEUE_ID)
    
    def _create_task(payload: dict, task_name_hint: str | None = None):
    # 
    # 태스크 생성.
    # - 멱등성: task_name_hint가 있으면 name에 사용 (예: Pub/Sub messageId)
    # - 없으면 UTC초 + 짧은 UUID 사용
    
        if task_name_hint:
            task_id = f"task-{task_name_hint}"
        else:
            dt_str = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            task_id = f"task-{dt_str}-{uuid.uuid4().hex[:8]}"

        task = {
            "name": f"{_queue_path()}/tasks/{task_id}",
            "http_request": {
                "http_method": tasks_v2.HttpMethod.POST,
                "url": WORKER_URL,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps(payload).encode(),
                "oidc_token": {
                    "service_account_email": SERVICE_ACCOUNT_EMAIL
                    # 필요 시 audience를 명시적으로 지정:
                    # , "audience": WORKER_URL
                },
            },
        }

        try:
            resp = client.create_task(parent=_queue_path(), task=task)
            print(f"✅ Task created: {resp.name}", flush=True)
        except AlreadyExists:
            # 같은 messageId로 중복 생성 시도 등
            print(f"ℹ️ Task already exists (id={task_id}) — treated as success.", flush=True)


    def main():
        """Pub/Sub → Cloud Run A → Cloud Tasks (항상 생성, 큐는 직렬 처리)"""
        try:
            print("🚀 Trigger 진입", flush=True)
            payload = {"example": "data"}
            message_id = str(uuid.uuid4())
        except Exception as e:
            print(f"🔥 디코딩/파싱 오류: {e}", flush=True)
            return "OK"

        try:
            _create_task(payload, task_name_hint=message_id)
        except Exception as e:
            print(f"🔥 Task 생성 실패: {e}", flush=True)

        return "OK"
    
    send_email_task = EmailOperator (
        task_id='send_email_task',
        conn_id = 'conn_smtp_gmail',
        to='65e43b85.joycity.com@kr.teams.ms',
        from_email='ds_bi@joycity.com',
        subject='Airflow 성공메일',
        html_content = 'AIBI Airflow 작업이 완료되었습니다.'
    )

    main_function_task = PythonOperator(
        task_id='main_function_task',
        python_callable=main
    )


    main_function_task >> send_email_task


