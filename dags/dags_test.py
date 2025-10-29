from airflow import DAG
import pendulum
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="dags_conn_test",
    schedule=None,
    start_date = pendulum.datetime(2025, 9, 26, tz="Asia/Seoul"),
    catchup = False
) as dag:
    
    t1 = EmptyOperator(
        task_id = "t1"
    )

    t2 = EmptyOperator(
        task_id = "t2"
    )

t2 >> t1