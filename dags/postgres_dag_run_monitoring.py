from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.smtp.operators.smtp import EmailOperator
from datetime import datetime, timedelta
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import os
from airflow.models import Variable


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='postgres_dag_run_monitoring',
    default_args=default_args,
    description='airflow Postgres DAG Run 모니터링 및 이메일 보고',
    schedule='20 0 * * *',  # 매일 아침 9시 20분 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['ETL', 'postgres', 'monitoring'],
)


def get_var(key: str, default: str = None) -> str:
    """환경 변수 또는 Airflow Variable 조회"""
    return os.environ.get(key) or Variable.get(key, default_var=default)


def query_dag_stats_and_send_email():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    # 1. 데이터 조회
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    sql = """
    select dag_id, state, start_date,
    ROUND(EXTRACT(EPOCH from (end_date - start_date)) / 60 , 2) as minutes_diff,
    cnt as job_cnt
    from
    (
        select dag_id, state, min(start_date) as start_date, min(end_date) as end_date, count(1) as cnt
        from dag_run
        where logical_date >= CURRENT_DATE
        and run_type = 'scheduled'
        and dag_id not like '%sync%'
        group by 1,2
    ) TS
    """
    
    df = hook.get_pandas_df(sql=sql)
    print(df)
    
    # 2. HTML 테이블로 변환
    html_table = df.to_html(index=False, border=1)
    
    # 3. 이메일 본문 작성
    email_body = f"""
    <html>
        <body>
            <h2>DAG Run Statistics Report</h2>
            <p>실행 일시: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            {html_table}
            <br>
            <p>총 {len(df)}개의 DAG 실행 결과입니다.</p>
        </body>
    </html>
    """
    
    # 4. SMTP 메일 발송
    send_email_via_smtp(
        subject='DAG Run Statistics Report',
        html_content=email_body,
        recipients=['fc748c69.joycity.com@kr.teams.ms']
    )
    
    return df.to_json()

def send_email_via_smtp(subject, html_content, recipients):
    """SMTP를 통한 이메일 발송"""
    
    # Airflow Variables에서 SMTP 설정 가져오기
    smtp_host = get_var('SMTP_HOST', default='smtp.gmail.com')
    smtp_port = int(get_var('SMTP_PORT', default=587))
    sender_email = get_var('EMAIL_FROM', default='ds_bi@joycity.com')
    sender_password = get_var('SMTP_PASSWORD')
    
    # 이메일 구성
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = ', '.join(recipients)
    
    # HTML 부분
    part = MIMEText(html_content, 'html')
    msg.attach(part)
    
    # SMTP 발송
    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, recipients, msg.as_string())
        print(f"이메일 발송 성공: {recipients}")
    except Exception as e:
        print(f"이메일 발송 실패: {str(e)}")
        raise

# Task 정의
query_and_email_task = PythonOperator(
    task_id='query_dag_stats_and_send_email',
    python_callable=query_dag_stats_and_send_email,
    dag=dag,
)

query_and_email_task
