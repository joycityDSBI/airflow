from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator

with DAG(
    dag_id = 'dag_b',
    schedule = None,
    start_date = pendulum.datetime(2025, 9, 30, tz="Asia/Seoul"),
    catchup = False
    ) as dag:

    task = PythonOperator(...)