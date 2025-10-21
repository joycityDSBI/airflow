"""
Airflow DAG: BigQuery Metadata → Notion ETL with Change Detection
- 5분마다 메타데이터 변경 확인
- 변경 시에만 Notion 동기화 실행
- 변경 없으면 즉시 종료 (워커 효율적 사용)
- Airflow 3.0+ 버전 최적화 (TaskFlow API)
"""

import hashlib
import logging
import os
import smtplib
import time
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict

import pandas as pd
from airflow.sdk.dag import dag
from airflow.sdk.task import task
from airflow.sdk import Variable
from google.cloud import bigquery

# notion_utils 모듈 임포트 (같은 디렉토리에 위치해야 함)
from notion_utils import update_notion_databases

# ===== 이메일 설정 =====
# 환경 변수 또는 Airflow Variable에서 설정값 가져오기
def get_config(key: str, default: str = None) -> str:
    """환경 변수 또는 Airflow Variable에서 설정값 가져오기"""
    # 1순위: 환경 변수
    env_value = os.environ.get(key)
    if env_value:
        return env_value
    
    # 2순위: Airflow Variable (Airflow 3.0+ SDK)
    try:
        return Variable.get(key, default)
    except KeyError:
        return default

SMTP_HOST = get_config("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(get_config("SMTP_PORT", "587"))
SMTP_USER = get_config("SMTP_USER")
SMTP_PASSWORD = get_config("SMTP_PASSWORD")
EMAIL_TO = get_config("EMAIL_TO")
EMAIL_FROM = get_config("EMAIL_FROM", SMTP_USER)

# ===== 설정 =====
TARGET_PROJECT = "aibi-service"
TARGET_DATASET = "Service_Set"
METADATA_HASH_VAR = "bq_metadata_hash"  # Airflow Variable 키

# BigQuery 인증 (선택: 서비스 계정 키 사용 시)
GOOGLE_CREDENTIALS_PATH = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
if GOOGLE_CREDENTIALS_PATH:
    logging.info(f"📝 BigQuery 인증 파일: {GOOGLE_CREDENTIALS_PATH}")
else:
    logging.info("📝 BigQuery 인증: VM의 기본 Service Account 사용")

# ===== BigQuery 메타데이터 쿼리 =====
COLUMN_QUERY = f"""
SELECT
  table_catalog AS project_id,
  table_schema  AS dataset_id,
  table_name,
  column_name,
  data_type,
  is_nullable,
  CONCAT(table_catalog, '.', table_schema, '.', table_name) AS full_table_id
FROM `{TARGET_PROJECT}.{TARGET_DATASET}.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name IN (
  SELECT table_name
  FROM `{TARGET_PROJECT}.{TARGET_DATASET}.INFORMATION_SCHEMA.TABLES`
  WHERE table_type IN ('VIEW', 'MATERIALIZED VIEW')
)
ORDER BY full_table_id, column_name
"""


def send_email_notification(
    subject: str,
    body: str,
    recipients: str | list[str],
    html: bool = True
) -> bool:
    """
    SMTP를 통한 이메일 발송
    
    Args:
        subject: 이메일 제목
        body: 이메일 본문
        recipients: 수신자 이메일 (문자열 또는 리스트)
        html: HTML 형식 여부
        
    Returns:
        성공 여부
    """
    if not all([SMTP_USER, SMTP_PASSWORD, EMAIL_TO]):
        logging.warning("⚠️ 이메일 환경 변수가 설정되지 않았습니다. 이메일을 건너뜁니다.")
        return False
    
    try:
        # 수신자 리스트 처리
        if isinstance(recipients, str):
            recipients = [r.strip() for r in recipients.split(',')]
        
        # 이메일 메시지 생성
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = EMAIL_FROM or SMTP_USER
        msg['To'] = ', '.join(recipients)
        
        # 본문 추가
        if html:
            msg.attach(MIMEText(body, 'html'))
        else:
            msg.attach(MIMEText(body, 'plain'))
        
        # SMTP 연결 및 발송
        logging.info(f"📧 이메일 발송 시도: {SMTP_HOST}:{SMTP_PORT}")
        
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()  # TLS 보안 연결
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)
        
        logging.info(f"✅ 이메일 발송 완료: {', '.join(recipients)}")
        return True
        
    except Exception as e:
        logging.error(f"🔥 이메일 발송 실패: {e}")
        return False


def create_email_body(sync_result: Dict[str, Any]) -> str:
    """
    동기화 결과 이메일 본문 생성 (HTML)
    
    Args:
        sync_result: 동기화 결과 딕셔너리
        
    Returns:
        HTML 형식의 이메일 본문
    """
    success = sync_result.get('success', False)
    rows_processed = sync_result.get('rows_processed', 0)
    hash_updated = sync_result.get('hash_updated', 'N/A')
    duration = sync_result.get('duration_seconds', 0)
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    status_emoji = "✅" if success else "❌"
    status_text = "성공" if success else "실패"
    status_color = "#4CAF50" if success else "#F44336"
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
            .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
            .header {{ background-color: {status_color}; color: white; padding: 20px; text-align: center; border-radius: 5px 5px 0 0; }}
            .content {{ background-color: #f9f9f9; padding: 20px; border: 1px solid #ddd; border-top: none; border-radius: 0 0 5px 5px; }}
            .info-row {{ margin: 10px 0; padding: 10px; background-color: white; border-left: 4px solid {status_color}; }}
            .label {{ font-weight: bold; color: #555; }}
            .value {{ color: #333; }}
            .footer {{ text-align: center; margin-top: 20px; color: #888; font-size: 12px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h2>{status_emoji} BigQuery → Notion 동기화 {status_text}</h2>
            </div>
            <div class="content">
                <div class="info-row">
                    <span class="label">실행 시간:</span>
                    <span class="value">{timestamp}</span>
                </div>
                <div class="info-row">
                    <span class="label">처리된 행 수:</span>
                    <span class="value">{rows_processed:,} rows</span>
                </div>
                <div class="info-row">
                    <span class="label">소요 시간:</span>
                    <span class="value">{duration} 초</span>
                </div>
                <div class="info-row">
                    <span class="label">메타데이터 해시:</span>
                    <span class="value">{hash_updated}</span>
                </div>
                <div class="info-row">
                    <span class="label">대상 프로젝트:</span>
                    <span class="value">{TARGET_PROJECT}.{TARGET_DATASET}</span>
                </div>
            </div>
            <div class="footer">
                <p>이 메시지는 Airflow DAG에서 자동으로 발송되었습니다.</p>
                <p>DAG ID: bq_notion_metadata_sync</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    return html


# ===== DAG 정의 (TaskFlow API 사용) =====
@dag(
    dag_id='bq_notion_metadata_sync',
    description='BigQuery 메타데이터를 Notion에 동기화 (5분마다 변경 체크)',
    schedule='*/5 * * * *',  # 5분마다 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['bigquery', 'notion', 'metadata', 'etl'],
    default_args={
        'owner': 'data-team',
        'retries': 2,
        'retry_delay': timedelta(minutes=2),
    },
    doc_md=__doc__,
)
def bq_notion_metadata_sync():
    """
    BigQuery 메타데이터를 Notion에 동기화하는 DAG
    
    TaskFlow:
    1. Check: 메타데이터 변경 확인 (해시 비교)
    2. Extract: BigQuery에서 메타데이터 추출 (변경 시에만)
    3. Load: Notion에 동기화 및 해시 업데이트
    4. Notify: 이메일 알림 발송
    """
    
    @task(task_id='check_metadata_change')
    def check_metadata_change() -> Dict[str, Any]:
        """
        메타데이터 변경 확인 (해시 비교)
        변경 없으면 'skip' 반환, 있으면 메타데이터 반환
        """
        start_ts = time.time()
        logging.info("🔍 BigQuery 메타데이터 변경 감지 시작")
        
        try:
            # BigQuery 클라이언트 생성
            bq_client = bigquery.Client(project=TARGET_PROJECT)
            
            # 메타데이터 조회
            query_job = bq_client.query(COLUMN_QUERY)
            results = query_job.result()
            df = results.to_dataframe(create_bqstorage_client=False)
            
            logging.info(
                f"✅ 조회 완료: rows={len(df)}, "
                f"tables={df['table_name'].nunique()}, "
                f"columns={df['column_name'].nunique()}"
            )
            
            # 데이터를 정렬된 문자열로 변환 (해시 계산용)
            rows = []
            for _, row in df.iterrows():
                row_dict = row.to_dict()
                rows.append(str(sorted(row_dict.items())))
            
            # 전체 데이터의 해시값 계산
            data_str = "".join(sorted(rows))
            current_hash = hashlib.sha256(data_str.encode()).hexdigest()
            
            logging.info(f"📊 현재 메타데이터 해시: {current_hash[:16]}...")
            
            # 이전 해시값 가져오기
            try:
                previous_hash = Variable.get(METADATA_HASH_VAR)
                logging.info(f"📋 이전 메타데이터 해시: {previous_hash[:16]}...")
            except KeyError:
                previous_hash = None
                logging.info("📋 이전 해시 없음 (초기 실행)")
            
            # 해시 비교
            if current_hash != previous_hash:
                logging.info("✅ 변경 감지됨 → ETL 실행")
                took = time.time() - start_ts
                return {
                    'changed': True,
                    'current_hash': current_hash,
                    'records': df.to_dict('records'),
                    'row_count': len(df),
                    'table_count': int(df['table_name'].nunique()),
                    'column_count': int(df['column_name'].nunique()),
                    'check_duration': round(took, 2),
                }
            else:
                logging.info("⏸️ 변경 없음 → 종료")
                took = time.time() - start_ts
                return {
                    'changed': False,
                    'current_hash': current_hash,
                    'check_duration': round(took, 2),
                }
                
        except Exception as e:
            logging.exception(f"🔥 메타데이터 체크 실패: {e}")
            raise
    
    @task(task_id='sync_to_notion')
    def sync_to_notion(check_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Notion 데이터베이스 동기화 (변경 있을 때만 실행)
        """
        # 변경 없으면 스킵
        if not check_result.get('changed'):
            logging.info("⏭️ 변경 없음, Notion 동기화 건너뜀")
            return {'skipped': True, 'reason': 'no_change'}
        
        start_ts = time.time()
        logging.info("🧠 Notion 동기화 시작")
        
        try:
            metadata_records = check_result.get('records')
            
            if not metadata_records:
                raise ValueError("메타데이터가 비어있습니다")
            
            # dict → DataFrame 변환
            df = pd.DataFrame(metadata_records)
            logging.info(
                f"📦 DataFrame 로드: {len(df)} rows "
                f"(tables: {check_result.get('table_count')}, "
                f"columns: {check_result.get('column_count')})"
            )
            
            # Notion 동기화 실행
            update_notion_databases(df)
            
            # 성공 시 해시 업데이트
            current_hash = check_result.get('current_hash')
            if current_hash:
                Variable.set(METADATA_HASH_VAR, current_hash)
                logging.info(f"💾 해시 업데이트: {current_hash[:16]}...")
            
            took = time.time() - start_ts
            logging.info(f"🎉 Notion 동기화 완료 (⏱️ {took:.1f}s)")
            
            return {
                'success': True,
                'skipped': False,
                'rows_processed': len(df),
                'table_count': check_result.get('table_count'),
                'column_count': check_result.get('column_count'),
                'hash_updated': current_hash[:16] if current_hash else None,
                'duration_seconds': round(took, 2),
            }
            
        except Exception as e:
            logging.exception(f"🔥 Notion 동기화 실패: {e}")
            return {
                'success': False,
                'skipped': False,
                'error': str(e),
                'duration_seconds': round(time.time() - start_ts, 2),
            }
    
    @task(task_id='send_email_notification')
    def send_notification(sync_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        동기화 완료 후 이메일 알림 발송 (변경 있을 때만)
        """
        # 스킵된 경우 이메일 안 보냄
        if sync_result.get('skipped'):
            logging.info("⏭️ 변경 없어서 이메일 건너뜀")
            return {'email_sent': False, 'status': 'skipped', 'reason': 'no_change'}
        
        logging.info("📧 이메일 알림 발송 시작")
        
        try:
            success = sync_result.get('success', False)
            status = "성공" if success else "실패"
            rows = sync_result.get('rows_processed', 0)
            
            subject = f"[Airflow] BigQuery → Notion 동기화 {status} ({rows:,} rows)"
            body = create_email_body(sync_result)
            
            email_sent = send_email_notification(
                subject=subject,
                body=body,
                recipients=EMAIL_TO or SMTP_USER,
                html=True
            )
            
            if email_sent:
                logging.info("✅ 이메일 알림 발송 완료")
                return {'email_sent': True, 'status': 'success'}
            else:
                logging.warning("⚠️ 이메일 발송을 건너뜀")
                return {'email_sent': False, 'status': 'skipped'}
                
        except Exception as e:
            logging.error(f"🔥 이메일 발송 중 오류: {e}")
            return {'email_sent': False, 'status': 'failed', 'error': str(e)}
    
    # Task 의존성 정의
    check_result = check_metadata_change()
    sync_result = sync_to_notion(check_result)
    email_result = send_notification(sync_result)


# DAG 인스턴스 생성
dag_instance = bq_notion_metadata_sync()