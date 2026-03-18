from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
import pendulum
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

def send_simple_summary(**context):
    """간단한 DAG 실행 결과"""
    dag_run = context['dag_run']
    dag_id = context['dag'].dag_id
    ti = context['ti']
    
    # 현재 Task의 시작/종료 시간
    start_date = dag_run.start_date
    end_date = timezone.utcnow()
    duration = end_date - start_date
    
    # DAG 객체에서 Task 목록 가져오기
    dag = context['dag']
    all_tasks = dag.tasks
    
    task_rows = ''
    for task in all_tasks:
        task_rows += f'''
            <tr>
                <td>{task_id}</td>
                <td>완료 대기</td>
                <td>-</td>
            </tr>
        '''
    
    html_content = f'''
        <div style="font-family: Arial;">
            <h2 style="color: #4CAF50;">✅ DAG 실행 완료</h2>
            
            <h3>전체 실행 정보</h3>
            <table border="1" cellpadding="8" style="border-collapse: collapse;">
                <tr>
                    <td><b>DAG ID</b></td>
                    <td>{dag_id}</td>
                </tr>
                <tr>
                    <td><b>실행 시작</b></td>
                    <td>{start_date}</td>
                </tr>
                <tr>
                    <td><b>리포트 생성</b></td>
                    <td>{end_date}</td>
                </tr>
                <tr style="background-color: #e8f5e9;">
                    <td><b>경과 시간</b></td>
                    <td>{duration}</td>
                </tr>
                <tr>
                    <td><b>총 Task 수</b></td>
                    <td>{len(all_tasks)}개</td>
                </tr>
            </table>
            
            <h3>✅ 모든 작업이 성공적으로 완료되었습니다!</h3>
        </div>
    '''
    EmailOperator(
        task_id='dags_email_detail_operator',
        to='65e43b85.joycity.com@kr.teams.ms',
        subject=f'✅ DAG 완료: {dag_id}',
        html_content=html_content,
        conn_id='conn_smtp_gmail'
    ).execute(context=context)
    
with DAG(
    dag_id = 'dags_test_bigquery',
    default_args = default_args,
    description = 'A simple DAG to test BigQueryInsertJobOperator',
    schedule = None,
    start_date = pendulum.datetime(2025, 9, 15, tz="Asia/Seoul"),
    catchup = False,
    tags = ['example']
) as dag:

    start_datekey = '2022-06-28'
    task_id = 'insert_query_task'

    insert_query_job = BigQueryInsertJobOperator(
        task_id = task_id,
        configuration = {
            "query": {
                "query": f"""
                        DECLARE start_date DATE DEFAULT '{start_datekey}';

                        insert into `third-technique-389105.0506_01.testvision`
                        (date, users, sales, salecount)

                        WITH parsed_data AS (
                        SELECT 
                            date(parse_datetime('%Y-%m-%d %H:%M:%S', substr(stat_time_kst, 1, 19))) as datekey,
                            dau_count
                        FROM `third-technique-389105.0506_01.instant_dau`
                        WHERE length(stat_time_kst) >= 19
                            AND safe.parse_datetime('%Y-%m-%d %H:%M:%S', substr(stat_time_kst, 1, 19)) IS NOT NULL
                            AND safe.parse_datetime('%Y-%m-%d %H:%M:%S', substr(stat_time_kst, 1, 19)) >= DATETIME('{start_datekey}')
                            AND safe.parse_datetime('%Y-%m-%d %H:%M:%S', substr(stat_time_kst, 1, 19)) < DATETIME_ADD(DATETIME('{start_datekey}'), interval 1 day)
                        )
                        SELECT 
                        datekey,
                        SUM(CAST(dau_count AS INT64)) as users,
                        0 as sales,
                        0 as salecount
                        FROM parsed_data
                        GROUP BY datekey
                """,
                "useLegacySql": False,
            }
        },
        location='US'
    )

    send_summary = PythonOperator(
        task_id='send_summary',
        python_callable=send_simple_summary
    )

    insert_query_job >> send_summary