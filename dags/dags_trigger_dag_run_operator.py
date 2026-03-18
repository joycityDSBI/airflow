from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum

with DAG(
    dag_id = 'dags_trigger_dag_run_operator',
    start_date = pendulum.datetime(2025, 9, 30, tz="Asia/Seoul"),
    schedule = '30 6 * * *',
    catchup = False
) as dag:
    
    start_task = BashOperator(
        task_id='start_task',
        bash_command='echo "Starting the DAG"',
    )

    trigger_dag_task = TriggerDagRunOperator(
        task_id='trigger_dag_task',
        trigger_dag_id='dags_python_operator',
        trigger_run_id=None,
        conf={'execution_date': '{{ data_interval_start }}'},
        wait_for_completion=False,  # Wait for the triggered DAG to complete
        poke_interval=30,  # Check every 30 seconds
    )


start_task >> trigger_dag_task