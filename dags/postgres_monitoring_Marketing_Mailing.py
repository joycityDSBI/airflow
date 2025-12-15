from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import logging
import os
from airflow.models import Variable
from sqlalchemy import create_engine, text



logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
}

dag = DAG(
    dag_id='postgres_monitoring_Marketing_Mailing',
    default_args=default_args,
    description='DAG run marketing_mailing query and email',
    schedule='15 5 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

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


def query_dag_stats_and_send_email():
    """PostgreSQL에서 DAG 통계를 조회하고 이메일로 발송"""
    try:
        
        # PostgreSQL 연결 문자열
        db_host = get_var('DB_HOST', 'postgres')
        db_port = int(get_var('DB_PORT', '5432'))
        db_user = get_var('DB_USER', 'airflow')
        db_password = get_var('DB_PASSWORD', 'airflow')
        db_name = get_var('DB_NAME', 'airflow')
        
        db_conn_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
        
        logger.info(f"PostgreSQL 연결 시도: {db_host}:{db_port}/{db_name}")
        engine = create_engine(db_conn_string)
        
        # 연결 테스트
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            logger.info("✓ PostgreSQL 연결 성공")
        
        # DAG 실행 통계 조회 쿼리
        sql_query = """
        SELECT 
            dag_id, 
            state, 
            start_date + INTERVAL '9 hours' AS start_date,
            ROUND(EXTRACT(EPOCH FROM (end_date - start_date)) / 60, 2) AS minutes_diff,
            cnt AS job_cnt
        FROM
        (
            SELECT 
                dag_id, 
                state, 
                MIN(start_date) AS start_date, 
                MAX(end_date) AS end_date, 
                COUNT(1) AS cnt
            FROM dag_run
            WHERE start_date + INTERVAL '9 hours' >= CURRENT_DATE
            AND run_type not like '%manual%'
            where dag_id like '%Marketing_Mailing%'
            GROUP BY dag_id, state
        ) AS ts
        ORDER BY minutes_diff desc
        """
        
        logger.info("쿼리 실행 중...")
        
        # SQLAlchemy 2.x 호환성: text() 래퍼 사용
        with engine.connect() as conn:
            result = conn.execute(text(sql_query))
            rows = result.fetchall()
            columns = result.keys()
            df = pd.DataFrame(rows, columns=columns)
        
        logger.info(f"✓ 조회 결과: {len(df)}개 행")
        print("\n" + "="*60)
        print("DAG Run Statistics")
        print("="*60)
        print(df.to_string(index=False))
        print("="*60 + "\n")
        
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
                    body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 20px; }}
                    table {{ border-collapse: collapse; margin-top: 15px; width: 100%; }}
                    th, td {{ padding: 12px; text-align: left; border: 1px solid #ddd; }}
                    th {{ background-color: #2196F3; color: white; font-weight: bold; }}
                    tr:nth-child(even) {{ background-color: #f9f9f9; }}
                    tr:hover {{ background-color: #f0f0f0; }}
                    .header {{ color: #333; border-bottom: 3px solid #2196F3; margin-bottom: 20px; }}
                    .info-row {{ margin: 10px 0; }}
                    .footer {{ margin-top: 30px; color: #666; font-size: 12px; border-top: 1px solid #ddd; padding-top: 15px; }}
                </style>
            </head>
            <body>
                <div class="header">
                    <h2>📊 Marketing Mailing Report</h2>
                </div>
                
                <div class="info-row">
                    <strong>📅 실행 일시:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                </div>
                <div class="info-row">
                    <strong>🔍 조회 기준:</strong> run_id 중 'Marketing_Mailing'을 포함한 경우
                </div>
                
                <h3>📈 실행 결과</h3>
                {html_table}
                
                <div class="footer">
                    <p>✓ 총 <strong>{len(df)}</strong>개의 DAG 실행 결과</p>
                    <p>컬럼 설명:</p>
                    <ul>
                        <li><strong>dag_id:</strong> DAG 이름</li>
                        <li><strong>state:</strong> 실행 상태 (success, failed, running 등)</li>
                        <li><strong>start_date:</strong> 실행 시작 시간</li>
                        <li><strong>minutes_diff:</strong> 실행 소요 시간 (분)</li>
                        <li><strong>job_cnt:</strong> 실행 횟수</li>
                    </ul>
                    <p style="margin-top: 20px; color: #999;">
                        이 메일은 Airflow에서 자동으로 생성되었습니다.
                    </p>
                </div>
            </body>
        </html>
        """
        
        # SMTP 설정
        smtp_host = get_var('SMTP_HOST', 'smtp.gmail.com')
        smtp_port = int(get_var('SMTP_PORT', '587'))
        sender_email = get_var('SENDER_EMAIL', 'ds_bi@joycity.com')
        sender_password = get_var('SMTP_PASSWORD')
        recipients_str = get_var('RECIPIENTS', 'fc748c69.joycity.com@kr.teams.ms')

        if not sender_email or not sender_password:
            logger.warning("SMTP 설정이 incomplete합니다.")
            logger.warning(f"SENDER_EMAIL: {bool(sender_email)}, SENDER_PASSWORD: {bool(sender_password)}")
            logger.info("이메일을 발송하지 않고 데이터만 반환합니다.")
            return df.to_json()
        
        recipients = [r.strip() for r in recipients_str.split(',') if r.strip()]
        
        if not recipients:
            logger.warning("수신자가 지정되지 않았습니다. RECIPIENTS 변수를 확인하세요.")
            logger.info("이메일을 발송하지 않고 데이터만 반환합니다.")
            return df.to_json()
        
        # 이메일 구성
        msg = MIMEMultipart('alternative')
        msg['Subject'] = '[Airflow] DAG Run Statistics Report - ' + datetime.now().strftime('%Y-%m-%d')
        msg['From'] = sender_email
        msg['To'] = ', '.join(recipients)
        
        # HTML 부분
        part = MIMEText(email_body, 'html')
        msg.attach(part)
        
        # SMTP 발송
        logger.info(f"이메일 발송 시작: {recipients}")
        try:
            with smtplib.SMTP(smtp_host, smtp_port) as server:
                server.starttls()
                server.login(sender_email, sender_password)
                server.sendmail(sender_email, recipients, msg.as_string())
            
            logger.info(f"✓ 이메일 발송 성공: {recipients}")
        except smtplib.SMTPAuthenticationError as e:
            logger.error(f"❌ SMTP 인증 실패: {str(e)}")
            logger.error("SENDER_EMAIL과 SENDER_PASSWORD를 확인하세요")
            raise
        except smtplib.SMTPException as e:
            logger.error(f"❌ SMTP 오류: {str(e)}")
            raise
        
        return df.to_json()
        
    except Exception as e:
        logger.error(f"❌ 에러 발생: {str(e)}", exc_info=True)
        raise

# Task 정의
query_and_email_task = PythonOperator(
    task_id='query_dag_stats_and_send_email',
    python_callable=query_dag_stats_and_send_email,
    dag=dag,
)

query_and_email_task