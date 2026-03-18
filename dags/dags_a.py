from airflow import DAG
import pendulum
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id = 'dags_a',
    schedule = '0 12 * * *',
    start_date = pendulum.datetime(2025, 9, 30, tz="Asia/Seoul"),
    catchup = False
    ) as dag:

    trigger_dag_b = TriggerDagRunOperator(
        task_id='trigger_dag_b',
        trigger_dag_id='dag_b',
        wait_for_completion=True
    )

