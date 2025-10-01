from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
import json

with DAG(
    dag_id = 'tb_station_info',
    schedule = None,
    start_date = pendulum.datetime(2025, 9, 30, tz="Asia/Seoul"),
    catchup = False
) as dag:
    
    tb_cycle_station_info = SimpleHttpOperator(
        task_id = 'tb_cycle_station_info',
        http_conn_id = 'openapi.seoul.go.kr',
        endpoint = '{{var.value.apikey_openapi_seoul_go_kr}}/json/CardSubwayStatsNew/1/5/20250925/9호선/김포공항',
        method = 'GET',
        headers = {
            'Content-Type': 'application/json',
            'Accept': '*/*',
            'charset': 'utf-8'
        }
    )

    @task(task_id='python_2')
    def python_2(**kwargs):
        ti = kwargs['ti']
        response = ti.xcom_pull(task_ids='tb_cycle_station_info')
        import json
        from pprint import pprint

        pprint("🚀", json.loads(response))

    tb_cycle_station_info >> python_2()
