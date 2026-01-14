import logging
import pandas as pd
import os
import json
from airflow.models import Variable
from google.oauth2 import service_account

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import gspread
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine

from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator

fsf2_cbt_pre_register_etl = Dataset('fsf2_cbt_pre_register_etl')

def get_var(key: str, default: str = None) -> str:
    """환경 변수 또는 Airflow Variable 조회"""
    return os.environ.get(key) or Variable.get(key, default_var=default)

# ==========================================
# 1. 설정
# ==========================================
# DB 접속 정보
DATABASE_URL = "postgresql://airflow:airflow@postgres:5432/airflow"

# 구글 시트 설정
CREDENTIALS_JSON = get_var('GOOGLE_CREDENTIAL_JSON')

GOOGLE_SHEET_ID = "1wB6_RhpTPanaONtqQD93Kd1PCWvpIv7MCfq7JCypc5k" # 시트 URL 중간의 긴 문자열
TARGET_SHEET_NAME = "Sheet1" # 데이터를 넣을 시트 탭 이름

# 이메일 수신자
EMAIL_RECIPIENT = ["seongin@joycity.com"]

# SMTP 설정
SMTP_SERVER = "61.43.45.137"
SMTP_PORT = 25
SENDER_EMAIL = 'ds_bi@joycity.com'
SENDER_PASSWORD = get_var('SMTP_PASSWORD')


logger = logging.getLogger("airflow.task")

# ==========================================
# 2. 함수 정의
# ==========================================

def get_country_stats():
    """
    [DB 조회] 전체 기간에 대해 국가별 가입자 수 집계
    """
    engine = create_engine(DATABASE_URL)
    
    # 요청하신 쿼리: 전체 기간, 국가별 Group By, Count Distinct Email
    sql = """
    SELECT 
        country,
        COUNT(DISTINCT email) as user_count,
        MAX(synced_at) as last_updated
    FROM fsf2_beta_testers
    GROUP BY country
    ORDER BY user_count DESC;
    """
    
    try:
        df = pd.read_sql(sql, engine)
        logger.info(f"Fetched {len(df)} rows from DB.")
        return df
    except Exception as e:
        logger.error(f"DB Query Failed: {e}")
        raise

def update_google_sheet(df):
    """
    [구글 시트] 기존 내용 지우고 데이터프레임 내용 붙여넣기
    """
    if df is None or df.empty:
        logger.info("No data to update to Google Sheet.")
        return

    logger.info("Connecting to Google Sheets...")
    
    # GCP 인증
    cred_dict = json.loads(CREDENTIALS_JSON)

    # 2. private_key 줄바꿈 문자 처리 (필수 체크)
    if 'private_key' in cred_dict:
            # 만약 키 값에 \\n 문자가 그대로 들어있다면 실제 줄바꿈으로 변경
        if '\\n' in cred_dict['private_key']:
            cred_dict['private_key'] = cred_dict['private_key'].replace('\\n', '\n')

    # 3. 명시적으로 Service Account Credentials 생성 (google.auth.default 아님!)
    credentials = service_account.Credentials.from_service_account_info(
        cred_dict,
        scopes=["https://www.googleapis.com/auth/cloud-platform", 'https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    )
    
    try:
        client = gspread.authorize(credentials)
        
        # 시트 열기
        sh = client.open_by_key(GOOGLE_SHEET_ID)
        worksheet = sh.worksheet(TARGET_SHEET_NAME)
        
        # 기존 데이터 클리어 (헤더 포함 전체 삭제)
        worksheet.clear()
        
        # 데이터 준비 (헤더 + 내용)
        # gspread는 리스트의 리스트 형태로 데이터를 받습니다.
        header = df.columns.values.tolist()
        data = df.values.tolist()
        final_data = [header] + data
        
        # 데이터 업데이트 (A1 셀부터 시작)
        worksheet.update(values=final_data, range_name='A1')
        logger.info(f"Successfully updated Google Sheet. ({len(final_data)} rows)")
        
    except Exception as e:
        logger.error(f"Google Sheet Update Failed: {e}")
        raise

def send_stats_email(df):
    """
    [이메일] 집계 결과를 HTML 테이블로 변환하여 발송
    """
    if df is None or df.empty:
        logger.info("No data to send via email.")
        return

    # 1. HTML 본문 생성
    # Pandas의 to_html 기능을 사용하여 스타일이 적용된 표를 만듭니다.
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    html_table = df.to_html(index=False, border=1, justify='center')
    
    email_content = f"""
    <h3>[FSF2] CBT 사전예약 국가별 현황 리포트</h3>
    <p><strong>발송 시간:</strong> {current_time}</p>
    <p><strong>총 가입자 수:</strong> {df['user_count'].sum():,} 명</p>
    <br>
    {html_table}
    <br>
    <p>※ 이 메일은 Airflow에서 자동으로 발송되었습니다.</p>
    """

    # 메일 제목
    subject = f"[FSF2] CBT 가입자 현황 ({datetime.now().strftime('%Y-%m-%d')})"
    
    # 이메일 발송
    logger.info("📧 이메일 발송 중...")

    server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=10)
    server.set_debuglevel(0)  # 디버그 모드 끄기
    
    # # 인증이 필요하면
    # if SENDER_PASSWORD:
    #     server.login(SENDER_EMAIL, SENDER_PASSWORD)
    current_time = datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d %H:%M:%S")

    msg = MIMEMultipart()
    msg['From'] = SENDER_EMAIL
    msg['To'] = ', '.join(EMAIL_RECIPIENT)
    msg['Subject'] = f"{subject}: {current_time}"
    msg.attach(MIMEText(email_content, 'html'))
    
    server.sendmail(SENDER_EMAIL, EMAIL_RECIPIENT, msg.as_string())
    server.quit()
    print("메일 발송 성공")

# ==========================================
# 3. 메인 로직 및 DAG
# ==========================================
def etl_process(**kwargs):
    # 1. DB 조회
    df = get_country_stats()
    
    # 2. 구글 시트 업데이트
    update_google_sheet(df)
    
    # 3. 이메일 발송
    send_stats_email(df)

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='fsf2_cbt_pre_register_report',
    default_args=default_args,
    description='국가별 가입자 통계 -> 구글시트 & 이메일 발송',
    schedule=[fsf2_cbt_pre_register_etl],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['fsf2', 'report'],
) as dag:

    report_task = PythonOperator(
        task_id='report_task',
        python_callable=etl_process
    )