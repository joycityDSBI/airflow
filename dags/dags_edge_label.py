from airflow.utils.edgemodifier import Label
from airflow import DAG
import pendulum
from airflow.operators.empty import EmptyOperator

empty_1 = EmptyOperator(
    task_id = 'empty_1'
)

empty_2 = EmptyOperator(
    task_id = 'empty_2'
)

empty_1 >> Label("1과 2 사이") >> empty_2


