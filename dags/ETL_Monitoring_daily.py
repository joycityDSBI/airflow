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
import pytz


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
    dag_id='ETL_Fact_Monitoring_daily',
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

            # [추가 1] 조건부 서식 적용 함수 정의
            def format_outliers(val):
                """
                값이 숫자이고 1 초과 또는 -1 미만인 경우 붉은색/굵게 처리
                """
                try:
                    # 값이 없거나 숫자가 아닌 경우 패스
                    if val is None:
                        return val
                    
                    float_val = float(val)
                    if float_val > 1 or float_val < -1:
                        return f'<span style="color: red; font-weight: bold;">{val}</span>'
                    return val
                except (ValueError, TypeError):
                    return val

            # 서식을 적용할 대상 컬럼 리스트
            target_cols = [
                'um_umC_dru', 'um_fA_dru', 
                'um_ummC_rev', 'um_fP_rev', 
                'um_umC_dau', 'um_fA_dau'
            ]
            
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
            where datekey >= '2026-01-01'
            AND joyple_game_code IN (131, 133, 159, 1590, 30001, 30003, 60009)
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
            where datekey >= '2026-01-01'
            AND joyple_game_code IN (131, 133, 159, 1590, 30001, 30003, 60009)
            group by 1,2
            order by 1,2
            ),

            TC as (
            select joyple_game_code, datekey, sum(revenue) as total_rev
            from `datahub-478802.datahub.f_common_payment`
            -- where datekey >= date_add(current_date('Asia/Seoul'), interval -4 day)
            where datekey >= '2026-01-01'
            AND joyple_game_code IN (131, 133, 159, 1590, 30001, 30003, 60009)
            group by 1,2
            order by 1,2
            ),

            TD as (
            select joyple_game_code, datekey, 
                count(distinct if(access_type_id = 1,auth_account_name,null)) as dau, 
                count(distinct if(reg_datediff = 0, auth_account_name, null)) as dru
            from `datahub-478802.datahub.f_common_access`
            -- where datekey >= date_add(current_date('Asia/Seoul'), interval -4 day)
            where datekey >= '2026-01-01'
            AND joyple_game_code IN (131, 133, 159, 1590, 30001, 30003, 60009)
            group by 1,2
            order by 1,2
            )

            SELECT ta.joyple_game_code, TA.datekey
                , round(TA.total_rev - TB.total_rev,0) as um_ummC_rev
                , round(TA.total_rev - TC.total_rev,0) as um_fP_rev
                , round(TA.dau - TB.dau,0) as um_umC_dau
                , round(TA.dau - TD.dau,0) as um_fA_dau
            FROM TA 
            LEFT JOIN TB ON TA.joyple_game_code = TB.joyple_game_code AND TA.datekey = TB.datekey
            LEFT JOIN TC ON TA.joyple_game_code = TC.joyple_game_code AND TA.datekey = TC.datekey
            LEFT JOIN TD ON TA.joyple_game_code = TD.joyple_game_code AND TA.datekey = TD.datekey
            order by joyple_game_code, datekey
            
            """
            
            logger.info("쿼리 실행 중...")
            
            query_job = bq_client.query(sql_query)
            result = query_job.result()
            df = result.to_dataframe()

            if len(df) == 0:
                html_table = "<p>No Data</p>"
            else:
                # [추가 2] df1에 서식 적용
                for col in target_cols:
                    if col in df.columns:
                        df[col] = df[col].apply(format_outliers)
                
                # [수정] escape=False 추가 (HTML 태그 렌더링 허용)
                html_table = df.to_html(index=False, border=1, classes='table table-striped', escape=False)
            

            sql_query_2 = """
            WITH TA AS (
            select joyple_game_code, datekey, 
                count(distinct auth_account_name) as dau,
                count(distinct CASE WHEN RU =1 THEN auth_account_name END) as dru, 
                CAST(sum(daily_total_rev) AS int64) as total_rev,
                CAST(sum(daily_iaa_rev) AS int64) as total_iaa_rev
            from `datahub-478802.datahub.f_user_map`
            -- where datekey >= date_add(current_date('Asia/Seoul'), interval -4 day)
            where datekey >= '2026-01-01'
            AND joyple_game_code IN (131, 133, 159, 1590, 30001, 30003, 60009)
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
            where datekey >= '2026-01-01'
            AND joyple_game_code IN (131, 133, 159, 1590, 30001, 30003, 60009)
            group by 1,2
            order by 1,2
            ),

            TC as (
            select joyple_game_code, datekey, sum(revenue) as total_rev
            from `datahub-478802.datahub.f_common_payment`
            -- where datekey >= date_add(current_date('Asia/Seoul'), interval -4 day)
            where datekey >= '2026-01-01'
            AND joyple_game_code IN (131, 133, 159, 1590, 30001, 30003, 60009)
            group by 1,2
            order by 1,2
            ),

            TD as (
            select joyple_game_code, datekey, 
                count(distinct if(access_type_id = 1,auth_account_name,null)) as dau, 
                count(distinct if(reg_datediff = 0, auth_account_name, null)) as dru
            from `datahub-478802.datahub.f_common_access`
            -- where datekey >= date_add(current_date('Asia/Seoul'), interval -4 day)
            where datekey >= '2026-01-01'
            AND joyple_game_code IN (131, 133, 159, 1590, 30001, 30003, 60009)
            group by 1,2
            order by 1,2
            )

            SELECT ta.joyple_game_code, TA.datekey
                , round(TA.dru - TB.dru,0) as um_umC_dru
                , round(TA.dru - TD.dru,0) as um_fA_dru
                , round(TA.total_iaa_rev - TB.total_iaa_rev,0) as um_umC_iaa
            FROM TA 
            LEFT JOIN TB ON TA.joyple_game_code = TB.joyple_game_code AND TA.datekey = TB.datekey
            LEFT JOIN TC ON TA.joyple_game_code = TC.joyple_game_code AND TA.datekey = TC.datekey
            LEFT JOIN TD ON TA.joyple_game_code = TD.joyple_game_code AND TA.datekey = TD.datekey
            order by joyple_game_code, datekey
            
            """

            logger.info("2번째 쿼리 실행 중...")
            
            query_job = bq_client.query(sql_query_2)
            result = query_job.result()
            df2 = result.to_dataframe()


            if len(df2) == 0:
                html_table2 = "<p>No Data</p>"
            else:
                # [추가 3] df2에 서식 적용
                for col in target_cols:
                    if col in df2.columns:
                        df2[col] = df2[col].apply(format_outliers)

                # [수정] escape=False 추가
                html_table2 = df2.to_html(index=False, border=1, classes='table table-striped', escape=False)
            

            kst = pytz.timezone('Asia/Seoul')
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
                        <strong>📅 실행 일시:</strong> {datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')}
                    </div>
                    <div class="info-row">
                        <strong>🔍 조회 기준:</strong> f_user_map, f_user_map_char, f_common_payment, f_common_access의 최근 4일치 데이터
                    </div>
                    </div>
                    
                    <h3>📈 실행 결과</h3>
                    {html_table}
                    {html_table2}
                    
                    <div class="footer">
                        <p>✓ 총 <strong>{len(df)} / {len(df2)}</strong>개의 DAG 실행 결과</p>
                        <p>컬럼 설명:</p>
                        <ul>
                            <li><strong>um_ummC_rev:</strong> f_user_map과 f_user_map_char 의 매출액 차이(daily_iaa_rev)</li>
                            <li><strong>um_fP_rev:</strong> f_user_map과 f_payment 의 매출액 차이(daily_iaa_rev)</li>
                            <li><strong>um_umC_dau:</strong> f_user_map과 f_user_map_char 의 DAU 차이</li>
                            <li><strong>um_fA_dau:</strong> f_user_map과 f_access 의 DAU 차이</li>
                            <li><strong>um_umC_dru:</strong> f_user_map과 f_user_map_char 의 DRU 차이</li>
                            <li><strong>um_fA_dru:</strong> f_user_map과 f_access 의 DRU 차이</li>
                            <li><strong>um_umC_iaa:</strong> f_user_map과 f_user_map_char 의 IAA 매출액 차이(daily_iaa_rev)</li>
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
            msg['Subject'] = '****TEST*****[Airflow] Daily ETL Fact Report - ' + datetime.now(kst).strftime('%Y-%m-%d')
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
            raise e

# Task 정의
query_datahub_status_and_send_email_task = PythonOperator(
    task_id='query_datahub_status_and_send_email',
    python_callable=query_datahub_status_send_email,
    dag=dag,
)

query_datahub_status_and_send_email_task