"""
Airflow DAG: BigQuery Metadata → Notion ETL (Sensor 방식 - 수정됨)
- 한 번만 시작하여 계속 실행
- Sensor가 5분마다 변경 감지
- 변경 시 ETL 실행 후 다시 Sensor로 복귀
- Airflow 2.x/3.x 호환
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
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from google.cloud import bigquery

# notion_utils 모듈 임포트
from notion_utils import update_notion_databases

# ===== 설정 =====
def get_config(key: str, default: str = None) -> str:
    """환경 변수 또는 Airflow Variable에서 설정값 가져오기"""
    env_value = os.environ.get(key)
    if env_value:
        return env_value
    
    try:
        # Airflow 버전 호환성
        return Variable.get(key, default_var=default)
    except TypeError:
        # Airflow 3.0+
        return Variable.get(key, default)
    except KeyError:
        return default

# 이메일 설정
SMTP_HOST = get_config("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(get_config("SMTP_PORT", "587"))
SMTP_USER = get_config("SMTP_USER")
SMTP_PASSWORD = get_config("SMTP_PASSWORD")
EMAIL_TO = get_config("EMAIL_TO")
EMAIL_FROM = get_config("EMAIL_FROM", SMTP_USER)

# BigQuery 설정
TARGET_PROJECT = "aibi-service"
TARGET_DATASET = "Service_Set"
METADATA_HASH_VAR = "bq_metadata_hash"

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


# ===== Custom Sensor =====
class BigQueryMetadataChangeSensor(BaseSensorOperator):
    """BigQuery 메타데이터 변경 감지 Sensor"""
    
    template_fields = ('project_id', 'query', 'hash_variable')
    
    def __init__(
        self,
        *,
        project_id: str,
        query: str,
        hash_variable: str,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.project_id = project_id
        self.query = query
        self.hash_variable = hash_variable
    
    def poke(self, context: Dict[str, Any]) -> bool:
        """메타데이터 변경 감지"""
        logging.info("🔍 BigQuery 메타데이터 변경 감지 시작")
        
        try:
            bq_client = bigquery.Client(project=self.project_id)
            query_job = bq_client.query(self.query)
            results = query_job.result()
            
            # 해시 계산
            rows = []
            for row in results:
                row_dict = dict(row)
                rows.append(str(sorted(row_dict.items())))
            
            data_str = "".join(sorted(rows))
            current_hash = hashlib.sha256(data_str.encode()).hexdigest()
            
            logging.info(f"📊 현재 해시: {current_hash[:16]}...")
            
            # 이전 해시 가져오기
            try:
                previous_hash = Variable.get(self.hash_variable)
                logging.info(f"📋 이전 해시: {previous_hash[:16]}...")
            except KeyError:
                previous_hash = None
                logging.info("📋 이전 해시 없음 (초기 실행)")
            
            # 변경 감지
            if current_hash != previous_hash:
                logging.info("✅ 변경 감지됨 → ETL 실행")
                context['ti'].xcom_push(key='current_hash', value=current_hash)
                return True
            else:
                logging.info("⏸️ 변경 없음 → 대기")
                return False
                
        except Exception as e:
            logging.error(f"🔥 Sensor 에러: {e}")
            return False


# ===== Task 함수들 =====
def extract_bq_metadata(**context):
    """BigQuery 메타데이터 추출"""
    start_ts = time.time()
    logging.info("🚀 BigQuery 메타데이터 추출 시작")
    
    try:
        bq_client = bigquery.Client(project=TARGET_PROJECT)
        df = bq_client.query(COLUMN_QUERY).result().to_dataframe(
            create_bqstorage_client=False
        )
        
        logging.info(
            f"✅ 조회 완료: rows={len(df)}, "
            f"tables={df['table_name'].nunique()}, "
            f"columns={df['column_name'].nunique()}"
        )
        
        # XCom에 저장
        context['ti'].xcom_push(key='metadata_df', value=df.to_dict('records'))
        context['ti'].xcom_push(key='row_count', value=len(df))
        context['ti'].xcom_push(key='table_count', value=int(df['table_name'].nunique()))
        context['ti'].xcom_push(key='column_count', value=int(df['column_name'].nunique()))
        
        took = time.time() - start_ts
        logging.info(f"⏱️ 추출 소요: {took:.1f}s")
        
    except Exception as e:
        logging.exception(f"🔥 메타데이터 추출 실패: {e}")
        raise


def sync_to_notion(**context):
    """Notion 동기화"""
    start_ts = time.time()
    logging.info("🧠 Notion 동기화 시작")
    
    try:
        ti = context['ti']
        metadata_records = ti.xcom_pull(task_ids='extract_metadata', key='metadata_df')
        
        if not metadata_records:
            raise ValueError("메타데이터가 비어있습니다")
        
        df = pd.DataFrame(metadata_records)
        logging.info(f"📦 DataFrame 로드: {len(df)} rows")
        
        # Notion 동기화
        update_notion_databases(df)
        
        # 해시 업데이트
        current_hash = ti.xcom_pull(task_ids='detect_metadata_change', key='current_hash')
        if current_hash:
            Variable.set(METADATA_HASH_VAR, current_hash)
            logging.info(f"💾 해시 업데이트: {current_hash[:16]}...")
        
        took = time.time() - start_ts
        logging.info(f"🎉 Notion 동기화 완료 (⏱️ {took:.1f}s)")
        
        # 결과 저장
        ti.xcom_push(key='success', value=True)
        ti.xcom_push(key='duration', value=round(took, 2))
        
    except Exception as e:
        logging.exception(f"🔥 Notion 동기화 실패: {e}")
        context['ti'].xcom_push(key='success', value=False)
        context['ti'].xcom_push(key='error', value=str(e))
        raise


def send_email_notification(**context):
    """이메일 알림 발송"""
    if not all([SMTP_USER, SMTP_PASSWORD, EMAIL_TO]):
        logging.warning("⚠️ 이메일 설정 누락")
        return
    
    ti = context['ti']
    success = ti.xcom_pull(task_ids='sync_to_notion', key='success')
    rows = ti.xcom_pull(task_ids='extract_metadata', key='row_count')
    duration = ti.xcom_pull(task_ids='sync_to_notion', key='duration')
    hash_val = ti.xcom_pull(task_ids='detect_metadata_change', key='current_hash')
    
    status = "성공" if success else "실패"
    status_color = "#4CAF50" if success else "#F44336"
    status_emoji = "✅" if success else "❌"
    
    subject = f"[Airflow] BigQuery → Notion 동기화 {status} ({rows:,} rows)"
    
    body = f"""
    <!DOCTYPE html>
    <html>
    <head><meta charset="UTF-8"></head>
    <body style="font-family: Arial, sans-serif;">
        <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
            <div style="background-color: {status_color}; color: white; padding: 20px; text-align: center;">
                <h2>{status_emoji} BigQuery → Notion 동기화 {status}</h2>
            </div>
            <div style="background-color: #f9f9f9; padding: 20px; border: 1px solid #ddd;">
                <p><strong>실행 시간:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p><strong>처리된 행 수:</strong> {rows:,} rows</p>
                <p><strong>소요 시간:</strong> {duration} 초</p>
                <p><strong>메타데이터 해시:</strong> {hash_val[:16] if hash_val else 'N/A'}...</p>
                <p><strong>대상 프로젝트:</strong> {TARGET_PROJECT}.{TARGET_DATASET}</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = EMAIL_FROM or SMTP_USER
        msg['To'] = EMAIL_TO
        msg.attach(MIMEText(body, 'html'))
        
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)
        
        logging.info("✅ 이메일 발송 완료")
        
    except Exception as e:
        logging.error(f"🔥 이메일 발송 실패: {e}")


# ===== DAG 정의 =====
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='bq_notion_metadata_sync',
    default_args=default_args,
    description='BigQuery 메타데이터를 Notion에 동기화 (5분마다 체크)',
    schedule_interval='*/5 * * * *',  # 5분마다 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['bigquery', 'notion', 'metadata', 'etl'],
) as dag:
    
    # Task 1: 변경 감지 Sensor
    detect_change = BigQueryMetadataChangeSensor(
        task_id='detect_metadata_change',
        project_id=TARGET_PROJECT,
        query=COLUMN_QUERY,
        hash_variable=METADATA_HASH_VAR,
        poke_interval=60,   # 1분마다 체크
        timeout=300,        # 5분 타임아웃 (DAG 주기와 일치)
        mode='poke',        # poke 모드
    )
    
    # Task 2: 메타데이터 추출
    extract_metadata = PythonOperator(
        task_id='extract_metadata',
        python_callable=extract_bq_metadata,
    )
    
    # Task 3: Notion 동기화
    sync_notion = PythonOperator(
        task_id='sync_to_notion',
        python_callable=sync_to_notion,
    )
    
    # Task 4: 이메일 알림
    send_email = PythonOperator(
        task_id='send_email_notification',
        python_callable=send_email_notification,
        trigger_rule=TriggerRule.ALL_DONE,  # 성공/실패 모두 실행
    )
    
    # 의존성
    detect_change >> extract_metadata >> sync_notion >> send_email