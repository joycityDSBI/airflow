from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG('dag_b', ...) as dag:
    task = PythonOperator(...)