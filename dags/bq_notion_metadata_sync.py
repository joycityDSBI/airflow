"""
수정된 부분:
1. Variable.get() 에러 처리 개선 (KeyError, VARIABLE_NOT_FOUND)
2. Airflow 3.0+ 호환성 처리
3. 초기 실행 시 Variable 자동 생성
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
import json
from google.oauth2 import service_account

# notion_utils 모듈 임포트
from notion_utils import update_notion_databases


# notion_utils 모듈 임포트
try:
    from notion_utils import update_notion_databases
except ImportError:
    logging.warning("⚠️ notion_utils 모듈을 찾을 수 없습니다. 플러그인 경로를 확인하세요.")




# ===== 설정 =====
def get_config(key: str, default: str = None) -> str:
    """환경 변수 또는 Airflow Variable에서 설정값 가져오기"""
    env_value = os.environ.get(key)
    if env_value:
        return env_value
    
    try:
        # Airflow 3.0+ 호환성
        try:
            from airflow.sdk import Variable as SDKVariable
            return SDKVariable.get(key, default)
        except ImportError:
            # Airflow 2.x
            return Variable.get(key, default_var=default)
    except Exception:
        return default

def get_var(key: str, default: str = None) -> str:
    """환경 변수 또는 Airflow Variable 조회"""
    return os.environ.get(key) or Variable.get(key, default_var=default)

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
CREDENTIALS_JSON = get_var('GOOGLE_CREDENTIAL_JSON')

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


# ===== Custom Sensor (수정됨) =====
class BigQueryMetadataChangeSensor(BaseSensorOperator):
    """BigQuery 메타데이터 변경 감지 Sensor"""
    template_fields = ('project_id', 'query', 'hash_variable')
    
    def __init__(self, *, project_id: str, query: str, hash_variable: str, **kwargs):
        super().__init__(**kwargs)
        self.project_id = project_id
        self.query = query
        self.hash_variable = hash_variable
    
    def _get_variable_safely(self, key: str, default: str = None) -> str:
        # (기존 코드와 동일)
        try:
            try:
                from airflow.sdk import Variable as SDKVariable
                return SDKVariable.get(key, default)
            except ImportError:
                return Variable.get(key, default_var=default)
        except Exception:
            return default
    
    def _set_variable_safely(self, key: str, value: str) -> bool:
        """Variable을 안전하게 저장"""
        try:
            Variable.set(key, value)
            logging.info(f"💾 Variable 저장 성공: {key}")
            return True
        except Exception as e:
            logging.error(f"🔥 Variable 저장 실패: {type(e).__name__} - {e}")
            return False
    
    def poke(self, context: Dict[str, Any]) -> bool:
        logging.info("🔍 BigQuery 메타데이터 변경 감지 시작")
        
        try:
            # Credentials 처리 (기존 유지)
            cred_dict = json.loads(CREDENTIALS_JSON)
            if 'private_key' in cred_dict and '\\n' in cred_dict['private_key']:
                cred_dict['private_key'] = cred_dict['private_key'].replace('\\n', '\n')
            
            credentials = service_account.Credentials.from_service_account_info(
                cred_dict, scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            
            bq_client = bigquery.Client(project=self.project_id, credentials=credentials)
            query_job = bq_client.query(self.query)
            results = query_job.result()
            
            rows = []
            for row in results:
                row_dict = dict(row)
                rows.append(str(sorted(row_dict.items())))
            
            data_str = "".join(sorted(rows))
            current_hash = hashlib.sha256(data_str.encode()).hexdigest()
            
            logging.info(f"📊 현재 해시: {current_hash[:16]}...")
            
            # 이전 해시 가져오기
            previous_hash = self._get_variable_safely(self.hash_variable)
            
            # XCom에 현재 해시 푸시 (나중에 업데이트용)
            context['ti'].xcom_push(key='current_hash', value=current_hash)
            
            # [수정됨] 초기 실행이거나 해시가 다르면 True 반환
            if previous_hash is None:
                logging.info("🚀 최초 실행 감지 (이전 해시 없음) -> 실행")
                return True
            elif current_hash != previous_hash:
                logging.info(f"✅ 변경 감지됨 ({previous_hash[:8]} -> {current_hash[:8]}) -> 실행")
                return True
            else:
                logging.info("⏸️ 변경 없음 -> 대기")
                return False
                
        except Exception as e:
            logging.error(f"🔥 Sensor 에러: {e}", exc_info=True)
            return False


# ===== Task 함수들 =====
def extract_bq_metadata(**context):
    # (기존 로직 유지)
    # 단, 대량 데이터일 경우 XCom 대신 GCS 사용 고려 필요
    start_ts = time.time()
    try:
        cred_dict = json.loads(CREDENTIALS_JSON)
        if 'private_key' in cred_dict and '\\n' in cred_dict['private_key']:
             cred_dict['private_key'] = cred_dict['private_key'].replace('\\n', '\n')
        credentials = service_account.Credentials.from_service_account_info(cred_dict)
        
        bq_client = bigquery.Client(project=TARGET_PROJECT, credentials=credentials)
        
        # DataFrame 변환
        df = bq_client.query(COLUMN_QUERY).result().to_dataframe(create_bqstorage_client=False)
        
        context['ti'].xcom_push(key='metadata_df', value=df.to_dict('records'))
        context['ti'].xcom_push(key='row_count', value=len(df))
        
        logging.info(f"✅ 추출 완료: {len(df)} rows")
    except Exception as e:
        logging.exception(f"🔥 추출 실패: {e}")
        raise


def sync_to_notion(**context):
    """Notion 동기화"""
    start_ts = time.time()
    logging.info("=" * 70)
    logging.info("🧠 Notion 동기화 시작")
    logging.info("=" * 70)
    
    try:
        ti = context['ti']
        
        # XCom에서 데이터 가져오기
        logging.info("📌 Step 1: XCom에서 메타데이터 읽기")
        metadata_records = ti.xcom_pull(
            task_ids='extract_metadata',
            key='metadata_df'
        )
        
        if not metadata_records:
            raise ValueError("XCom에서 메타데이터를 찾을 수 없습니다")
        
        logging.info(f"  ✅ 데이터 로드: {len(metadata_records)} records")
        
        # DataFrame 변환
        logging.info("📌 Step 2: DataFrame 변환")
        df = pd.DataFrame(metadata_records)
        logging.info(f"  ✅ 변환 완료: {len(df)} rows × {len(df.columns)} cols")
        logging.info(f"  컬럼: {df.columns.tolist()}")
        
        # Notion 업데이트 (여기서 예외가 발생할 수 있음)
        logging.info("📌 Step 3: update_notion_databases 호출")
        result = update_notion_databases(df)  # 📌 이제 예외를 throw할 것!
        logging.info(f"  ✅ Notion 업데이트 완료: {result}")
        
        took = time.time() - start_ts
        ti.xcom_push(key='success', value=True)
        ti.xcom_push(key='duration', value=round(took, 2))
        
        logging.info("=" * 70)
        logging.info(f"✅ Notion 동기화 완료 (⏱️ {took:.1f}s)")
        logging.info("=" * 70)
        
    except Exception as e:
        logging.error("=" * 70)
        logging.exception(f"🔥 Notion 동기화 실패: {e}")
        logging.error("=" * 70)
        
        context['ti'].xcom_push(key='success', value=False)
        context['ti'].xcom_push(key='error', value=str(e))
        
        # 📌 에러를 raise하면 DAG에서 감지 가능!
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

# ===== 3. [신규] 성공 후 Variable 업데이트 함수 =====
def update_variable_after_success(**context):
    """모든 동기화가 성공적으로 끝난 후 Variable 업데이트"""
    ti = context['ti']
    current_hash = ti.xcom_pull(task_ids='detect_metadata_change', key='current_hash')
    
    if not current_hash:
        logging.warning("⚠️ 업데이트할 해시 값이 없습니다.")
        return

    key = METADATA_HASH_VAR
    try:
        # Airflow 3.0+ 호환
        try:
            from airflow.sdk import Variable as SDKVariable
            SDKVariable.set(key, current_hash)
        except ImportError:
            Variable.set(key, current_hash)
            
        logging.info(f"💾 Variable 업데이트 완료: {key} = {current_hash[:16]}...")
    except Exception as e:
        logging.error(f"🔥 Variable 저장 실패: {e}")
        raise

# ===== DAG 정의 =====
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='bq_notion_metadata_sync',
    default_args=default_args,
    description='BigQuery 메타데이터를 Notion에 동기화 (5분마다 체크)',
    schedule='*/5 * * * *',  # 5분마다 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['bigquery', 'notion', 'metadata', 'etl'],
) as dag:
    
# 1. 감지
    detect_change = BigQueryMetadataChangeSensor(
        task_id='detect_metadata_change',
        project_id=TARGET_PROJECT,
        query=COLUMN_QUERY,
        hash_variable=METADATA_HASH_VAR,
        poke_interval=60,
        timeout=300,
        mode='poke', # Worker slot 점유가 부담되면 'reschedule' 사용
    )
    
    # 2. 추출
    extract_metadata = PythonOperator(
        task_id='extract_metadata',
        python_callable=extract_bq_metadata,
    )
    
    # 3. 동기화
    sync_notion = PythonOperator(
        task_id='sync_to_notion',
        python_callable=sync_to_notion,
    )
    
    # 4. [신규] Variable 업데이트 (성공 시에만 실행)
    update_var = PythonOperator(
        task_id='update_hash_variable',
        python_callable=update_variable_after_success,
        trigger_rule=TriggerRule.ALL_SUCCESS, # 앞 단계가 모두 성공해야 실행
    )
    
    # # Task 4: 이메일 알림
    # send_email = PythonOperator(
    #     task_id='send_email_notification',
    #     python_callable=send_email_notification,
    #     trigger_rule=TriggerRule.ALL_DONE,  # 성공/실패 모두 실행
    # )
    
    # 의존성
    detect_change >> extract_metadata >> sync_notion >> update_var