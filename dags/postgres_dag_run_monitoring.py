from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dag_run_stats_email',
    default_args=default_args,
    description='DAG run statistics query and email',
    schedule='30 0 * * *',  # 매일 자정 30분에 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

def query_dag_stats_and_send_email():
    """PostgreSQL에서 DAG 통계를 조회하고 이메일로 발송"""
    try:
        from sqlalchemy import create_engine
        from airflow.models import Variable
        
        # PostgreSQL 연결 문자열
        # Docker 같은 네트워크에서 'postgres'로 접근
        db_host = Variable.get('DB_HOST', default='postgres')
        db_port = Variable.get('DB_PORT', default='5432')
        db_user = Variable.get('DB_USER', default='airflow')
        db_password = Variable.get('DB_PASSWORD', default='airflow')
        db_name = Variable.get('DB_NAME', default='airflow')
        
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
        smtp_host = Variable.get('SMTP_HOST', default='smtp.gmail.com')
        smtp_port = int(Variable.get('SMTP_PORT', default='587'))
        sender_email = Variable.get('SENDER_EMAIL')
        sender_password = Variable.get('SENDER_PASSWORD')
        recipients_str = Variable.get('RECIPIENTS', default='fc748c69.joycity.com@kr.teams.ms')
        
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