from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import logging
from sqlalchemy import create_engine
from airflow.models import Variable
import os

logger = logging.getLogger(__name__)


def get_var(key: str, default: str = None) -> str:
    """환경 변수 → Airflow Variable 순서로 조회
    
    Args:
        key: 변수 이름
        default: 기본값 (없으면 None)
    
    Returns:
        환경 변수 값 또는 Airflow Variable 값
    """
    # 1단계: 환경 변수 확인
    env_value = os.environ.get(key)
    if env_value:
        print(f"✓ 환경 변수에서 {key} 로드됨")
        return env_value
    
    # 2단계: Airflow Variable 확인
    try:
        var_value = Variable.get(key, default_var=None)
        if var_value:
            print(f"✓ Airflow Variable에서 {key} 로드됨")
            return var_value
    except Exception as e:
        print(f"⚠️  Variable.get({key}) 오류: {str(e)}")
    
    # 3단계: 기본값 반환
    if default is not None:
        print(f"ℹ️  기본값으로 {key} 설정됨")
        return default
    
    raise ValueError(f"필수 설정 {key}을(를) 찾을 수 없습니다. "
                     f"환경 변수 또는 Airflow Variable에서 설정하세요.")


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
}

dag = DAG(
    dag_id='postgres_dag_run_monitoring',
    default_args=default_args,
    description='DAG run statistics query and email',
    schedule='10 0 * * *',  # 매일 자정 30분에 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

def query_dag_stats_and_send_email():
    """PostgreSQL에서 DAG 통계를 조회하고 이메일로 발송"""
    try:
        
        
        # PostgreSQL 연결 문자열
        # Docker 같은 네트워크에서 'postgres'로 접근
        db_host = get_var('DB_HOST', 'postgres')
        db_port = int(get_var('DB_PORT', '5432'))
        db_user = get_var('DB_USER', 'airflow')
        db_password = get_var('DB_PASSWORD', 'airflow')
        db_name = get_var('DB_NAME', 'airflow')
        
        db_conn_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
        
        logger.info(f"PostgreSQL 연결 시도: {db_host}:{db_port}/{db_name}")
        engine = create_engine(db_conn_string)
        
        # DAG 실행 통계 조회
        sql = """
        select dag_id, state, start_date,
        ROUND(EXTRACT(EPOCH from (end_date - start_date)) / 60, 2) as minutes_diff,
        cnt as job_cnt
        from
        (
            select dag_id, state, min(start_date) as start_date, max(end_date) as end_date, count(1) as cnt
            from dag_run
            where logical_date >= CURRENT_DATE
            and run_type = 'scheduled'
            and dag_id not like '%sync%'
            group by 1, 2
        ) TS
        order by minutes_diff desc
        """
        
        df = pd.read_sql(sql, engine)
        logger.info(f"조회 결과: {len(df)}개 행")
        print(df)
        
        if len(df) == 0:
            logger.warning("조회 결과가 없습니다")
            return "No data"
        
        # HTML 테이블로 변환
        html_table = df.to_html(index=False, border=1, classes='table table-striped')
        
        # 이메일 본문 작성
        email_body = f"""
        <html>
            <head>
                <style>
                    body {{ font-family: Arial, sans-serif; }}
                    table {{ border-collapse: collapse; margin-top: 10px; }}
                    th, td {{ padding: 8px; text-align: left; border: 1px solid #ddd; }}
                    th {{ background-color: #4CAF50; color: white; }}
                    tr:nth-child(even) {{ background-color: #f2f2f2; }}
                </style>
            </head>
            <body>
                <h2>DAG Run Statistics Report</h2>
                <p><strong>실행 일시:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p><strong>조회 기준:</strong> 오늘 scheduled run 중 'sync' 제외</p>
                {html_table}
                <br>
                <p>총 <strong>{len(df)}</strong>개의 DAG 실행 결과입니다.</p>
            </body>
        </html>
        """
        
        # SMTP 설정
        smtp_host = get_var('SMTP_HOST', 'smtp.gmail.com')
        smtp_port = int(get_var('SMTP_PORT', '587'))
        sender_email = get_var('SENDER_EMAIL')
        sender_password = get_var('SENDER_PASSWORD')
        recipients_str = get_var('RECIPIENTS', 'fc748c69.joycity.com@kr.teams.ms')
        
        if not sender_email or not sender_password:
            logger.warning("SMTP 설정이 incomplete합니다. 이메일을 발송하지 않습니다.")
            return df.to_json()
        
        recipients = [r.strip() for r in recipients_str.split(',') if r.strip()]
        
        if not recipients:
            logger.warning("수신자가 지정되지 않았습니다.")
            return df.to_json()
        
        # 이메일 구성
        msg = MIMEMultipart('alternative')
        msg['Subject'] = '[Airflow] DAG Run Statistics Report'
        msg['From'] = sender_email
        msg['To'] = ', '.join(recipients)
        
        # HTML 부분
        part = MIMEText(email_body, 'html')
        msg.attach(part)
        
        # SMTP 발송
        logger.info(f"이메일 발송 시작: {recipients}")
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, recipients, msg.as_string())
        
        logger.info(f"이메일 발송 성공: {recipients}")
        return df.to_json()
        
    except Exception as e:
        logger.error(f"에러 발생: {str(e)}", exc_info=True)
        raise

# Task 정의
query_and_email_task = PythonOperator(
    task_id='query_dag_stats_and_send_email',
    python_callable=query_dag_stats_and_send_email,
    dag=dag,
)

query_and_email_task