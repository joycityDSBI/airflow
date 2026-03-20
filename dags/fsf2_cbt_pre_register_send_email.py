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
from airflow.providers.standard.operators.python import PythonOperator

fsf2_cbt_pre_register_etl = Dataset('fsf2_cbt_pre_register_etl')

def get_var(key: str, default: str | None = None) -> str:
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
EMAIL_RECIPIENT = ["3b590d59.joycity.com@kr.teams.ms"] # FSF2 프로젝트 룸

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
        DATE(created_at) as datekey,
        country,
        "countryCode", 
        COUNT(DISTINCT CASE WHEN platform = 'PlayStation 5' THEN email end) as ps5_user_count,
        COUNT(DISTINCT CASE WHEN platform = 'Xbox Series X|S' THEN email end) as xbox_user_count
    FROM fsf2_beta_testers
    GROUP BY 1,2,3
    ORDER BY 1,2,3 DESC;
    """
    
    try:
        df = pd.read_sql(sql, engine)
        logger.info(f"Fetched {len(df)} rows from DB.")
        return df
    except Exception as e:
        logger.error(f"DB Query Failed: {e}")
        raise

def get_country_mail():
    """
    [DB 조회] 전체 기간에 대해 국가별 가입자 수 집계
    """
    engine = create_engine(DATABASE_URL)
    
    # 요청하신 쿼리: 전체 기간, 국가별 Group By, Count Distinct Email
    sql = """
    SELECT 
        "countryCode", 
        COUNT(DISTINCT CASE WHEN platform = 'PlayStation 5' THEN email end) as ps5_user_count,
        COUNT(DISTINCT CASE WHEN platform = 'Xbox Series X|S' THEN email end) as xbox_user_count
    FROM fsf2_beta_testers
    GROUP BY "countryCode"
    ORDER BY 2 DESC;
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
        
        # ========================================================
        # [수정] 컬럼별 데이터 타입 처리
        # ========================================================
        # 원본 데이터 보존을 위해 복사본 생성
        df_upload = df.copy()
        
        # 숫자로 유지할 컬럼 리스트 정의
        numeric_target_cols = ['ps5_user_count', 'xbox_user_count']

        for col in df_upload.columns:
            if col in numeric_target_cols:
                # 1. 숫자형 컬럼: 숫자로 강제 변환 후, NaN(빈값)은 0으로 채움
                # (JSON은 NaN을 지원하지 않으므로 0 처리가 안전합니다)
                df_upload[col] = pd.to_numeric(df_upload[col], errors='coerce').fillna(0)
            else:
                # 2. 나머지 컬럼: Timestamp 오류 방지를 위해 문자열로 변환
                df_upload[col] = df_upload[col].astype(str)
                # (선택사항) 문자열 "nan"이 보기 싫다면 빈 문자열로 치환 가능
                # df_upload[col] = df_upload[col].replace('nan', '')

        # 데이터 준비
        header = df_upload.columns.values.tolist()
        data = df_upload.values.tolist()
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
    current_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # User Count 합계 계산
    ps5_count = df['ps5_user_count'].sum() if 'ps5_user_count' in df.columns else 0
    xbox_count = df['xbox_user_count'].sum() if 'xbox_user_count' in df.columns else 0

    sheet_url = f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/edit#gid=0"
    
    html_table = df.to_html(index=False, border=1, justify='center')
    
    style = """
    <style>
        body { font-family: 'Malgun Gothic', sans-serif; }
        .report-container { font-size: 13px; color: #333; }
        h3 { font-size: 18px; margin-bottom: 10px; color: #2c3e50; }
        .meta-info { margin-bottom: 20px; font-size: 13px; color: #555; }
        
        /* 테이블 스타일 */
        .report-table {
            width: 100%;
            max-width: 600px;
            border-collapse: collapse; /* 테두리 겹침 방지 (선을 얇게 만듦) */
            font-size: 12px;
            margin-top: 10px;
        }
        .report-table th {
            background-color: #f2f2f2;
            border-bottom: 2px solid #ddd; /* 헤더 아래는 조금 진하게 */
            border-top: 1px solid #ddd;
            padding: 8px;
            text-align: center;
            font-weight: bold;
            color: #333;
        }
        .report-table td {
            border-bottom: 1px solid #eee; /* 셀 아래는 연하게 */
            padding: 8px;
            text-align: center;
            color: #555;
        }
        /* 짝수 행 배경색 (가독성 향상) */
        .report-table tr:nth-child(even) {
            background-color: #fafafa;
        }
    </style>
    """

    email_content = f"""
    <html>
    <head>{style}</head>
    <body>
        <div class="report-container">
            <h3>📊 [FSF2] CBT 사전예약 국가별 현황</h3>
            
            <div class="meta-info">
                <p style="margin: 5px 0;"><strong>📅 발송 시간:</strong> {current_time_str}</p>
                <p style="margin: 5px 0;"><strong>🧑‍🤝‍🧑 PS5 가입자 수:</strong> <span style="color: #d35400; font-weight: bold;">{ps5_count:,} 명</span></p>
                <p style="margin: 5px 0;"><strong>🧑‍🤝‍🧑 Xbox 가입자 수:</strong> <span style="color: #d35400; font-weight: bold;">{xbox_count:,} 명</span></p>
                <p style="margin: 5px 0;"><strong>👥 총 가입자 수:</strong> <span style="color: #d35400; font-weight: bold;">{xbox_count + ps5_count:,} 명</span></p>
            </div>
            
            {html_table}
            
            <br>
            <p style="font-size: 11px; color: #999;">원본 데이터는 하기 스프레드 시트에서 확인 가능합니다.</p>
            <p style="font-size: 11px; color: #999;">{sheet_url}</p>
            <br>
            <p style="font-size: 11px; color: #999;">※ 이 메일은 Airflow에서 자동으로 발송되었습니다.</p>
        </div>
    </body>
    </html>
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
    df_mail = get_country_mail()
    
    # 2. 구글 시트 업데이트
    update_google_sheet(df)
    
    # 3. 이메일 발송
    send_stats_email(df_mail)

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