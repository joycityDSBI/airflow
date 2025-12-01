from airflow.decorators import task_group
from airflow.decorators import task
from airflow import DAG
import pendulum
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator

with DAG(
    dag_id = "dags_task_group_decorator",
    schedule = "30 6 * * *",
    start_date = pendulum.datetime(2025, 9, 30, tz="Asia/Seoul"),
    catchup = False
) as dag:
    
    def inner_func(**kwargs):
        msg = kwargs.get('msg') or ''
        print(msg)
    
    @task_group(group_id="first_group")
    def group_1():
        ''' task_group 데코레이터를 이용한 첫번째 그룹입니다'''

        @task(task_id = 'inner_function1')
        def inner_funci(**kwargs):
            print("첫 번째 taskgroup 내 첫 번째 task 입니다")

        inner_function2 = PythonOperator(
            task_id = 'inner_function2',
            python_callable = inner_func,
            op_kwargs = {'msg':'첫 번째 taskgroup 내 두 번째 task 입니다'}
        )
        inner_funci() >> inner_function2


    with TaskGroup(group_id="outer_group", tooltip="두 번째 그룹입니다") as group_2:
        ''' TaskGroup 클래스를 이용한 두번째 그룹입니다'''

        @task(task_id = 'inner_function3')
        def inner_funci(**kwargs):
            print("두 번째 taskgroup 내 첫 번째 task 입니다")

        inner_function4 = PythonOperator(
            task_id = 'inner_function4',
            python_callable = inner_func,
            op_kwargs = {'msg':'두 번째 taskgroup 내 두 번째 task 입니다'}
        )
        inner_funci() >> inner_function4

    group_1() >> group_2