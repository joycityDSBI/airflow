from airflow import DAG
import pendulum
import datetime
from airflow.operators.bash import BashOperator

with DAG (
    dag_id = "dags_bash_select_fruit",
    schedule = "10 0 * * 6#1",
    start_date = pendulum.datetime(2025, 9, 29, tz="Asia/Seoul"),
    catchup = False
) as dag:
    
    t1_orange = BashOperator(
        task_id = "t1_orange",
        bash_command = "/opt/airflow/plugins/shell/select_fruit.sh ORANGE"
    )

    t1_avodado = BashOperator(
    task_id = "t1_avocado",
    bash_command = "/opt/airflow/plugins/shell/select_fruit.sh AVOCADE"
    )

t1_orange >> t1_avodado