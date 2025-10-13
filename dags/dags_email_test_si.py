from airflow import DAG
import pendulum
import datetime
from airflow.operators.email import EmailOperator

with DAG (
    dag_id = "dags_email_test_si",
    schedule = "0 8 1 * *",
    start_date = pendulum.datetime(2025, 9, 29, tz="Asia/Seoul"),
    catchup = False
) as dag:
    send_email_task = EmailOperator (
        task_id='send_email_task',
        conn_id = 'conn_smtp_outlook',
        to='seongin@joycity.com',
        subject='Airflow 성공메일',
        html_content = 'Airflow 작업이 완료되었습니다.'
    )
