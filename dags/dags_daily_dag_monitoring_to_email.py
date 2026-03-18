from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.operators.email import EmailOperator
from airflow import DAG
import pendulum
from contextlib import closing
import pandas as pd
from airflow.models import Variable


email_str = Variable.get("email_target")
email_list = [email.strip() for email in email_str.split(',')]

with DAG(
    dag_id='dags_daily_dag_monitoring_to_email',
    start_date=pendulum.datetime(2023, 5, 1, tz="Asia/Seoul"),
    schedule='0 9 * * *',
    catchup=False
) as dag:
    @task(task_id='get_daily_monitoring_data')
    def get_daily_monitoring_data(**kwargs):
        postgres_hook = PostgresHook(postgres_conn_id='conn-db-postgres-airflow')
        with closing(postgres_hook.get_conn()) as conn:
            with closing(conn.cursor()) as cursor:
                with open('/opt/airflow/dags/sql/daily_dag_monitoring.sql', 'r', encoding='utf-8') as sql_file:
                    cursor.execute("'SET TIME ZONE 'Asia/Seoul';")
                    sql = '\n'.join(sql_file.readlines())
                    cursor.execute(sql)
                    rslt = cursor.fetchall()
                    rslt = pd.DataFrame(rslt)
                    rslt.columns = ['dag_id', 'run_cnt', 'success_cnt', 'failed_cnt']
                    html_content = ''

                    # 1) 실패 대상
                    failed_df = rslt.query('failed_cnt > 0')
                    html_content += "<h3 style='color: red;'>❗️ 실패한 DAG 목록</h3>"
                    if not failed_df.empty:
                        for ids, row in failed_df.iterrows():
                            html_content += f"DAG: {row['dag_id']}\n"
                    
        