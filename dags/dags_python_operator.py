from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
import random

with DAG (
    dag_id = "dags_python_operator",
    schedule = "0 8 1 * *",
    start_date = pendulum.datetime(2025, 9, 29, tz="Asia/Seoul"),
    catchup = False
) as dag:
    def select_fruit():
        fruit = ['APPLE', 'BANANA', 'ORANGE', 'AVOCADO']
        rand_int = random.randint(0,3)
        print(fruit[rand_int])

    def my_favorite_fruit():
        fruit = ['APPLE',  'BANANA', 'ORANGE', 'AVOCADO']
        rand_int = random.randint(0,3)
        print(f"my favorite fruit is {fruit[rand_int]}!!!")

    py_t1 = PythonOperator(
        task_id = 'py_t1',
        python_callable = select_fruit
    )

    py_t2 = PythonOperator(
        task_id = 'py_t2',
        python_callable = my_favorite_fruit
    )

    py_t1 >> py_t2