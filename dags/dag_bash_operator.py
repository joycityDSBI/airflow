import datetime

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="dag_bash_operator",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2025, 9, 24, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=20),
    tags=["dimension", "marketing"]
) as dag:
    
    bash_t1 = BashOperator(
        task_id="bash_t1",
        bash_command="echo whoami",
    )

    bash_t2 = BashOperator(
    task_id="bash_t2",
    bash_command="echo $HOSTNAME",
    )

    bash_t1 >> bash_t2