from airflow import DAG
import pendulum

with DAG(
    dag_id = "dags_python_task_decorator",
    schedule = "0 2 * * 1",
    start_date = pendulum.datetime(2025, 9, 30, tz="Asia/Seoul"),
    catchup = False
) as dag:
    
    @task(task_id='python_task_1')
    def print_context(some_input):
        print(some_input)

    run_this = print_context()