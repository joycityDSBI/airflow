from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
from common.common_func import get_sftp, regist2

with DAG(
    dag_id = "dags_python_with_op_kwargs",
    schedule = "30 6 * * *",
    start_date = pendulum.datetime(2025, 9, 30, tz="Asia/Seoul"),
    catchup = False
) as dag:
    
    regist2_t1 = PythonOperator(
        task_id = 'regist_t1',
        python_callable = regist2,
        op_args = ['홍길동', '남자', '30세', '서울시 강남구', '010-1234-5678'],
        op_kwargs = {'email':'honggildong@example.com', 'phone':'010-9876-5432'}
    )

    regist2_t1
    