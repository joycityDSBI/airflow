# Airflow function
import smtplib
from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from google.cloud import bigquery
import logging
from datetime import datetime, timedelta

import json
from google.oauth2 import service_account
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


logger = logging.getLogger(__name__)

PROJECT_ID = "datahub-478802"
LOCATION = "us-central1"

def get_var(key: str, default: str = None) -> str:
    """환경 변수 또는 Airflow Variable 조회"""
    return os.environ.get(key) or Variable.get(key, default_var=default)

def get_gcp_credentials():
    """Airflow Variable에서 GCP 자격 증명을 로드합니다."""
    credentials_json = Variable.get('GOOGLE_CREDENTIAL_JSON')
    cred_dict = json.loads(credentials_json)
    if 'private_key' in cred_dict:
        cred_dict['private_key'] = cred_dict['private_key'].replace('\\n', '\n')
    
    # [수정] 스코프(Scopes)를 명시적으로 여러 개 추가합니다.
    SCOPES = [
        "https://www.googleapis.com/auth/cloud-platform",       # 기본 전체 권한
        "https://www.googleapis.com/auth/bigquery"             # BigQuery 권한
    ]
    
    return service_account.Credentials.from_service_account_info(
        cred_dict,
        scopes=SCOPES
    )


def init_clients():
    """Task 내부에서 실행되어 필요한 클라이언트들을 생성하여 반환합니다."""
    creds = get_gcp_credentials()
    
    # 1. GCP Clients
    bq_client = bigquery.Client(project=PROJECT_ID, credentials=creds)
    
    return {
        "bq_client": bq_client
    }

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
}

with DAG(
    dag_id='postgres_dag_run_monitoring',
    default_args=default_args,
    description='DAG run statistics query and email',
    schedule='50 00 * * *',  # 매일 오전 09시 50분 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['ETL', 'monitoring', 'bigquery'],
) as dag:

    def query_datahub_status_send_email():
    
        try:
            client = init_clients()
            bq_client = client["bq_client"]
            
            # DAG 실행 통계 조회 쿼리
            sql_query = """
            WITH TA AS (
            select joyple_game_code, datekey, 
                count(distinct auth_account_name) as dau,
                count(distinct CASE WHEN RU =1 THEN auth_account_name END) as dru, 
                CAST(sum(daily_total_rev) AS int64) as total_rev,
                CAST(sum(daily_iaa_rev) AS int64) as total_iaa_rev
            from `datahub-478802.datahub.f_user_map`
            -- where datekey >= date_add(current_date('Asia/Seoul'), interval -4 day)
            where datekey >= '2025-12-28'
            group by 1,2
            order by 1,2
            )
            ,

            TB AS (
            select a.joyple_game_code, datekey,
                count(distinct auth_account_name) as dau,
                count(distinct CASE WHEN RU =1 THEN auth_account_name END) as dru, 
                CAST(sum(daily_total_rev) AS int64) as total_rev,
                CAST(sum(daily_iaa_rev) AS int64) as total_iaa_rev
            from `datahub-478802.datahub.f_user_map_char` as a
            -- where datekey >= date_add(current_date('Asia/Seoul'), interval -4 day)
            where datekey >= '2025-12-28'
            group by 1,2
            order by 1,2
            ),

            TC as (
            select joyple_game_code, datekey, sum(revenue) as total_rev
            from `datahub-478802.datahub.f_common_payment`
            -- where datekey >= date_add(current_date('Asia/Seoul'), interval -4 day)
            where datekey >= '2025-12-28'
            group by 1,2
            order by 1,2
            ),

            TD as (
            select joyple_game_code, datekey, 
                count(distinct if(access_type_id = 1,auth_account_name,null)) as dau, 
                count(distinct if(reg_datediff = 0, auth_account_name, null)) as dru
            from `datahub-478802.datahub.f_common_access`
            -- where datekey >= date_add(current_date('Asia/Seoul'), interval -4 day)
            where datekey >= '2025-12-28'
            group by 1,2
            order by 1,2
            )

            SELECT ta.*
                , round(TA.total_rev - TB.total_rev,0) as usermap_usermapChar_rev
                , round(TA.total_rev - TC.total_rev,0) as usermap_fPayment_rev
                , round(TA.dau - TB.dau,0) as usermap_usermapChar_dau
                , round(TA.dau - TD.dau,0) as usermap_fAccess_dau
                , round(TA.dru - TB.dru,0) as usermap_usermapChar_dru
                , round(TA.dru - TD.dru,0) as usermap_fAccess_dru
                , round(TA.total_iaa_rev - TB.total_iaa_rev,0) as usermap_usermapChar_iaa_rev
            FROM TA 
            LEFT JOIN TB ON TA.joyple_game_code = TB.joyple_game_code AND TA.datekey = TB.datekey
            LEFT JOIN TC ON TA.joyple_game_code = TC.joyple_game_code AND TA.datekey = TC.datekey
            LEFT JOIN TD ON TA.joyple_game_code = TD.joyple_game_code AND TA.datekey = TD.datekey
            order by joyple_game_code, datekey
            
            """
            
            logger.info("쿼리 실행 중...")
            
            query_job = client.query(sql_query)
            result = query_job.result()
            df = result.to_dataframe()


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
                        <h2>🐌 Airflow ETL Fact Report</h2>
                    </div>
                    
                    <div class="info-row">
                        <strong>📅 실행 일시:</strong> {datetime.now('Asia/Seoul').strftime('%Y-%m-%d %H:%M:%S')}
                    </div>
                    <div class="info-row">
                        <strong>🔍 조회 기준:</strong> f_user_map, f_user_map_char, f_common_payment, f_common_access의 최근 4일치 데이터
                    </div>
                    </div>
                    
                    <h3>📈 실행 결과</h3>
                    {html_table}
                    
                    <div class="footer">
                        <p>✓ 총 <strong>{len(df)}</strong>개의 DAG 실행 결과</p>
                        <p>컬럼 설명:</p>
                        <ul>
                            <li><strong>dau:</strong> f_user_map 기준의 count(distinct auth_account_name)</li>
                            <li><strong>dru:</strong> f_user_map 기준의 RU=1 인 경우의 count(distinct auth_account_name)</li>
                            <li><strong>total_rev:</strong> f_user_map 기준의 sum(daily_total_rev)</li>
                            <li><strong>total_iaa_rev:</strong> f_user_map_char 기준의 sum(daily_iaa_rev)</li>
                            <li><strong>usermap_usermapChar_rev:</strong> f_user_map과 f_user_map_char 의 매출액 차이(daily_iaa_rev)</li>
                            <li><strong>usermap_fPayment_rev:</strong> f_user_map과 f_payment 의 매출액 차이(daily_iaa_rev)</li>
                            <li><strong>usermap_usermapChar_dau:</strong> f_user_map과 f_user_map_char 의 DAU 차이</li>
                            <li><strong>usermap_fAccess_dau:</strong> f_user_map과 f_access 의 DAU 차이</li>
                            <li><strong>usermap_usermapChar_dru:</strong> f_user_map과 f_user_map_char 의 DRU 차이</li>
                            <li><strong>usermap_fAccess_dru:</strong> f_user_map과 f_access 의 DRU 차이</li>
                            <li><strong>usermap_usermapChar_iaa_rev:</strong> f_user_map과 f_user_map_char 의 IAA 매출액 차이(daily_iaa_rev)</li>
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
            msg['Subject'] = '****TEST*****[Airflow] Daily ETL Fact Report - ' + datetime.now('Asia/Seoul').strftime('%Y-%m-%d')
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
query_datahub_status_and_send_email_task = PythonOperator(
    task_id='query_datahub_status_and_send_email',
    python_callable=query_datahub_status_send_email,
    dag=dag,
)

query_datahub_status_and_send_email_task