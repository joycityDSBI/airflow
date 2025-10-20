"""
Airflow DAG: BigQuery Metadata → Notion ETL with Change Detection
- Custom Sensor로 5분마다 BigQuery INFORMATION_SCHEMA 변경 감지
- 변경 시에만 Notion 동기화 실행
"""

import hashlib
import logging
import time
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator
from google.cloud import bigquery

# notion_utils 모듈 임포트 (같은 디렉토리에 위치해야 함)
from notion_utils import update_notion_databases

# ===== 설정 =====
TARGET_PROJECT = "aibi-service"
TARGET_DATASET = "Service_Set"
METADATA_HASH_VAR = "bq_metadata_hash"  # Airflow Variable 키

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

# ===== Custom Sensor: BigQuery 메타데이터 변경 감지 =====
class BigQueryMetadataChangeSensor(BaseSensorOperator):
    """
    BigQuery INFORMATION_SCHEMA를 조회하여 해시값을 계산하고,
    이전 해시값과 비교하여 변경 여부를 감지하는 Sensor
    """
    
    def __init__(self, project_id: str, query: str, hash_variable: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.query = query
        self.hash_variable = hash_variable
    
    def poke(self, context):
        """
        메타데이터 조회 → 해시 계산 → 이전 해시와 비교
        변경되었으면 True 반환 (다음 Task 실행)
        """
        logging.info("🔍 BigQuery 메타데이터 변경 감지 시작")
        
        try:
            # BigQuery 클라이언트 생성
            bq_client = bigquery.Client(project=self.project_id)
            
            # 메타데이터 조회
            query_job = bq_client.query(self.query)
            results = query_job.result()
            
            # 데이터를 정렬된 문자열로 변환 (해시 계산용)
            rows = []
            for row in results:
                row_dict = dict(row)
                rows.append(str(sorted(row_dict.items())))
            
            # 전체 데이터의 해시값 계산
            data_str = "".join(sorted(rows))
            current_hash = hashlib.sha256(data_str.encode()).hexdigest()
            
            logging.info(f"📊 현재 메타데이터 해시: {current_hash[:16]}...")
            
            # 이전 해시값 가져오기 (없으면 None)
            try:
                previous_hash = Variable.get(self.hash_variable)
                logging.info(f"📋 이전 메타데이터 해시: {previous_hash[:16]}...")
            except KeyError:
                previous_hash = None
                logging.info("📋 이전 해시 없음 (초기 실행)")
            
            # 해시 비교
            if current_hash != previous_hash:
                logging.info("✅ 변경 감지됨 → ETL 실행")
                # 현재 해시를 XCom에 저장 (ETL Task에서 Variable 업데이트용)
                context['task_instance'].xcom_push(key='current_hash', value=current_hash)
                return True
            else:
                logging.info("⏸️ 변경 없음 → 대기")
                return False
                
        except Exception as e:
            logging.error(f"🔥 Sensor 에러: {e}")
            # 에러 시 False 반환 (재시도)
            return False


# ===== Task 함수들 =====
def extract_bq_metadata(**context):
    """
    BigQuery 메타데이터 조회 및 DataFrame 반환
    """
    start_ts = time.time()
    logging.info("🚀 BigQuery 메타데이터 추출 시작")
    
    try:
        bq_client = bigquery.Client(project=TARGET_PROJECT)
        
        logging.info(f"🔍 BigQuery 조회: {TARGET_PROJECT}.{TARGET_DATASET}")
        df = bq_client.query(COLUMN_QUERY).result().to_dataframe(create_bqstorage_client=False)
        
        logging.info(
            f"✅ 조회 완료: rows={len(df)}, "
            f"tables={df['table_name'].nunique()}, "
            f"columns={df['column_name'].nunique()}"
        )
        
        # DataFrame을 다음 Task로 전달 (XCom)
        # 주의: XCom은 크기 제한이 있으므로, 큰 데이터는 GCS 사용 권장
        context['task_instance'].xcom_push(key='metadata_df', value=df.to_dict('records'))
        
        took = time.time() - start_ts
        logging.info(f"⏱️ 추출 소요: {took:.1f}s")
        
        return True
        
    except Exception as e:
        logging.exception(f"🔥 메타데이터 추출 실패: {e}")
        raise


def sync_to_notion(**context):
    """
    Notion 데이터베이스 동기화
    """
    start_ts = time.time()
    logging.info("🧠 Notion 동기화 시작")
    
    try:
        # 이전 Task에서 DataFrame 가져오기
        metadata_records = context['task_instance'].xcom_pull(
            task_ids='extract_metadata',
            key='metadata_df'
        )
        
        if not metadata_records:
            raise ValueError("메타데이터가 비어있습니다")
        
        # dict → DataFrame 변환
        df = pd.DataFrame(metadata_records)
        logging.info(f"📦 DataFrame 로드: {len(df)} rows")
        
        # Notion 동기화 실행
        update_notion_databases(df)
        
        # 성공 시 해시 업데이트
        current_hash = context['task_instance'].xcom_pull(
            task_ids='detect_metadata_change',
            key='current_hash'
        )
        if current_hash:
            Variable.set(METADATA_HASH_VAR, current_hash)
            logging.info(f"💾 해시 업데이트: {current_hash[:16]}...")
        
        took = time.time() - start_ts
        logging.info(f"🎉 Notion 동기화 완료 (⏱️ {took:.1f}s)")
        
        return True
        
    except Exception as e:
        logging.exception(f"🔥 Notion 동기화 실패: {e}")
        raise


# ===== DAG 정의 =====
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='bq_notion_metadata_sync',
    default_args=default_args,
    description='BigQuery 메타데이터를 Notion에 동기화 (변경 감지 기반)',
    schedule_interval='*/5 * * * *',  # 5분마다 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['bigquery', 'notion', 'metadata', 'etl'],
) as dag:
    
    # Task 1: 메타데이터 변경 감지 Sensor
    detect_change = BigQueryMetadataChangeSensor(
        task_id='detect_metadata_change',
        project_id=TARGET_PROJECT,
        query=COLUMN_QUERY,
        hash_variable=METADATA_HASH_VAR,
        poke_interval=300,  # 5분마다 체크
        timeout=600,  # 10분 타임아웃
        mode='poke',  # 'poke' 모드 (리소스 효율적)
    )
    
    # Task 2: BigQuery 메타데이터 추출
    extract_metadata = PythonOperator(
        task_id='extract_metadata',
        python_callable=extract_bq_metadata,
        provide_context=True,
    )
    
    # Task 3: Notion 동기화
    sync_notion = PythonOperator(
        task_id='sync_to_notion',
        python_callable=sync_to_notion,
        provide_context=True,
    )
    
    # Task 의존성 정의
    detect_change >> extract_metadata >> sync_notion