# Airflow function
from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from google.cloud import bigquery
import logging
from datetime import datetime, timedelta
import pytz
import json
from google.oauth2 import service_account
import os


logger = logging.getLogger(__name__)

PROJECT_ID = "datahub-478802"
LOCATION = "us-central1"

def get_var(key: str, default: str = None) -> str:
    """환경 변수 또는 Airflow Variable 조회"""
    return os.environ.get(key) or Variable.get(key, default_var=default)



default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
}

dag = DAG(
    dag_id='postgres_dag_run_monitoring',
    default_args=default_args,
    description='DAG run statistics query and email',
    schedule='20 01 * * *',  # 매일 오전 10시 20분 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
)