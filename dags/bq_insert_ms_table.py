from airflow import DAG, Dataset
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
import os
from airflow.models import Variable
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

bq_create_ms_table = Dataset('bq_create_ms_table')

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='bq_insert_ms_table',
    default_args=default_args,
    description='BigQuery 테이블 생성 예시',
    schedule=[bq_create_ms_table],
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['bigquery', 'create_table'],
) as dag: