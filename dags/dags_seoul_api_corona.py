from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator
from airflow import DAG
import pendulum

with DAG(
    dag_id = 'dags_seoul_api_corona',
    schedule = '0 6 * * *',
    start_date = pendulum.datetime(2025, 9, 25, tz="Asia/Seoul"),
    catchup = False
) as dag:

    tb_corona19_count_status = SeoulApiToCsvOperator(
        task_id = 'tb_corona19_count_status',
        dataset_nm = 'TbCorona19CountStatus',
        path = '/opt/airflow/files/TbCorona19CountStatus/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name = 'TbCorona19CountStatus.csv'
    )

    tb_corona19_vaccine_stat_new = SeoulApiToCsvOperator(
        task_id = 'tb_corona19_vaccine_stat_new',
        dataset_nm = 'TbCorona19VaccineStatNew',
        path = '/opt/airflow/files/TbCorona19VaccineStatNew/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name = 'TbCorona19VaccineStatNew.csv'
    )

   