from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG('dag_a', ...) as dag:
    trigger_dag_b = TriggerDagRunOperator(
        task_id='trigger_dag_b',
        trigger_dag_id='dag_b',
        wait_for_completion=True
    )

