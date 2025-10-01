from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator

with DAG(
    dag_id = 'dags_b',
    schedule = None,
    start_date = pendulum.datetime(2025, 9, 30, tz="Asia/Seoul"),
    catchup = False
    ) as dag:

    task = PythonOperator(
        task_id='task_python',
        python_callable=lambda: print("Hello from DAG B")
    )