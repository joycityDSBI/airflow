from airflow import DAG
import pendulum
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator

with DAG(
    dag_id = "dags_branch_python_operator",
    schedule = None,
    start_date = pendulum.datetime(2025, 9, 30, tz="Asia/Seoul"),
    catchup = False
) as dag:
    
    def select_random():
        import random

        item_lst = ['A', 'B', 'C']
        selected_item = random.choice(item_lst)
        if selected_item == 'A':
            return 'task_A'
        elif selected_item in ['B', 'C']:
            return ['task_B', 'task_C']
        
    python_branch_task = BranchPythonOperator(
        task_id = 'python_branch_task',
        python_callable = select_random
    )

    def common_func(**kwargs):
        print(kwargs['selected_item'])


    task_A = PythonOperator(
        task_id = 'task_A',
        python_callable = common_func,
        op_kwargs = {'selected_item':'A'}
    )

    task_B = PythonOperator(
        task_id = 'task_B',
        python_callable = common_func,
        op_kwargs = {'selected_item':'B'}
    )

    task_C = PythonOperator(
        task_id = 'task_C',
        python_callable = common_func,
        op_kwargs = {'selected_item':'C'}
    )

    python_branch_task >> [task_A, task_B, task_C]