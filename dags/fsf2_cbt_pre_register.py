import sys
import json
import base64
import requests
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict
import os

from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from sqlalchemy.dialects.postgresql import insert # Bulk Upsert용
from sqlalchemy import text

from airflow import DAG, Dataset
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


fsf2_cbt_pre_register_etl = Dataset('fsf2_cbt_pre_register_etl')

def get_var(key: str, default: str = None) -> str:
    """환경 변수 또는 Airflow Variable 조회"""
    return os.environ.get(key) or Variable.get(key, default_var=default)

# ==========================================
# 1. 설정 (Airflow Variable이나 환경변수로 관리 권장)
# ==========================================
EXTERNAL_API_URL = "https://freestylefootball2.com/api/pre-register/sync" # [라이브: 1월 19일 예정]
# EXTERNAL_API_URL = "https://qa-fsf2.joycity.com/api/pre-register/sync" # [QA]
DATABASE_URL = "postgresql://airflow:airflow@postgres:5432/airflow"

# [cite: 56] 명세서에 명시된 Key / IV
AES_KEY_STR = get_var("AES_KEY_STR")
AES_IV_STR = get_var("AES_IV_STR")

# 배치 사이즈 설정 (한 번에 DB에 밀어넣을 행 개수)
BATCH_SIZE = 10000


# 로깅 설정 (Airflow 로그에서 확인 가능)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ==========================================
# 2. DB 모델 정의 (SQLAlchemy)
# ==========================================
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class BetaTester(Base):
    __tablename__ = "fsf2_beta_testers"

    id = Column(Integer, primary_key=True, index=True)
    platform = Column(String, nullable=True)
    country = Column(String, nullable=False)     # 복호화하여 저장
    email = Column(String, unique=True, index=True, nullable=False) # 암호화 상태로 저장 [cite: 47]
    cbt_code = Column(String, nullable=True)
    created_at = Column(DateTime, nullable=True)
    utm_source = Column(String, nullable=True)
    utm_medium = Column(String, nullable=True)
    countryCode = Column(String, nullable=True)
    synced_at = Column(DateTime, default=datetime.utcnow)

# 테이블이 없으면 생성
Base.metadata.create_all(bind=engine)

# ==========================================
# 3. 유틸리티 함수
# ==========================================
def decrypt_country(encrypted_text: str) -> Optional[str]:
    """
    AES-256-CBC 복호화 함수 (디버깅 로그 추가 버전)
    """
    if not encrypted_text:
        return None

    try:
        # 1. Base64 디코딩
        enc_data = base64.b64decode(encrypted_text)
        
        # 2. Key / IV 준비
        # (전역 변수 AES_KEY_STR, AES_IV_STR 사용)
        key = AES_KEY_STR.encode('utf-8')
        iv = AES_IV_STR.encode('utf-8')
        
        # 3. 복호화 실행
        cipher = AES.new(key, AES.MODE_CBC, iv)
        decrypted_data = unpad(cipher.decrypt(enc_data), AES.block_size)
        
        return decrypted_data.decode('utf-8')

    except Exception as e:
        # ============================================================
        # [핵심] 복호화 실패 원인 로그 출력
        # ============================================================
        logger.error(f"##### [Decryption Failed] #####")
        logger.error(f"1. Input Text (Short): {encrypted_text[:20]}...") # 앞부분만 출력
        logger.error(f"2. Error Message: {str(e)}")
        logger.error(f"3. Error Type: {type(e).__name__}")
        
        # 키/IV 검증 (중요: 환경변수 로딩 문제 확인)
        key_len = len(AES_KEY_STR) if AES_KEY_STR else 0
        iv_len = len(AES_IV_STR) if AES_IV_STR else 0
        logger.error(f"4. Key Length Check: {key_len} (Must be 32)")
        logger.error(f"5. IV Length Check: {iv_len} (Must be 16)")
        
        # Base64 디코딩 문제인지 확인
        try:
            base64.b64decode(encrypted_text)
        except Exception as b64_err:
            logger.error(f"6. Base64 Decode Error: {str(b64_err)}")
            
        logger.error(f"################################")
        
        # 실패 시 프로그램이 죽지 않도록 원본(암호문)을 그대로 반환
        return encrypted_text
    
def execute_bulk_upsert(session, batch_data: List[Dict]):
    """
    SQLAlchemy Core의 insert().on_conflict_do_update()를 사용하여
    대량 데이터를 효율적으로 Upsert 처리
    """
    if not batch_data:
        return

# ============================================================
    # [수정된 부분] 중복 데이터 제거 로직 추가
    # 이유: 한 배치 내에 동일한 email이 존재하면 CardinalityViolation 에러 발생
    # 해결: 딕셔너리 컴프리헨션을 사용하여 email이 중복될 경우, 
    #       리스트의 뒤쪽에 있는(최신) 데이터로 덮어씌움
    # ============================================================
    unique_data_map = {row['email']: row for row in batch_data}
    clean_batch_data = list(unique_data_map.values())

    # 1. Insert 구문 생성 (중복 제거된 clean_batch_data 사용)
    stmt = insert(BetaTester).values(clean_batch_data)

    # 2. On Conflict (Upsert) 정의
    # email 컬럼이 중복(Conflict)되면 아래 필드들을 업데이트
    stmt = stmt.on_conflict_do_update(
        index_elements=['email'], # 중복 체크 기준 컬럼 (Unique Index 필수)
        set_={
            "country": stmt.excluded.country,
            "cbt_code": stmt.excluded.cbt_code,
            "platform": stmt.excluded.platform,
            "countryCode": stmt.excluded.countryCode,
            "synced_at": datetime.utcnow() # 동기화 시간 갱신
        }
    )

    # 3. 실행
    session.execute(stmt)
    session.commit()
    
# ==========================================
# 5. 메인 로직 (fetch_and_store_data)
# ==========================================
def fetch_and_store_data(start_unix: Optional[int] = None):

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    # 1. API 호출
    params = {}
    if start_unix:
        params["start_unix"] = start_unix

    logger.info(f"Fetching data from API... Params: {params}")
    
    try:
        # stream=True로 설정하여 메모리 효율성 증대 (초대용량 응답 대비)
        response = requests.get(EXTERNAL_API_URL, params=params, headers=headers, stream=True, timeout=60)
        response.raise_for_status()
        
        # 전체 JSON을 한 번에 로드 (50만건 정도는 메모리 로드 가능, 수 GB급이면 ijson 같은 라이브러리 필요)
        data_json = response.json()
        items = data_json.get("data", [])
        total_items = len(items)
        logger.info(f"Total items fetched: {total_items}")

    except Exception as e:
        logger.error(f"API Request failed: {e}")
        raise

    # 2. 데이터 가공 및 배치 처리
    session = SessionLocal()
    batch_buffer = []
    processed_count = 0

    try:

        logger.info("Truncating table fsf2_beta_testers...")
        session.execute(text("TRUNCATE TABLE fsf2_beta_testers RESTART IDENTITY CASCADE;"))

        for index, item in enumerate(items):
            encrypted_email = item.get("email")
            encrypted_country = item.get("country")
            encrypted_countryCode = item.get("countryCode")

            if not encrypted_email or not encrypted_country:
                continue

            # (1) 데이터 복호화 및 전처리
            final_country = decrypt_country(encrypted_country)
            final_countryCode = decrypt_country(encrypted_countryCode)
            
            # 날짜 변환
            created_at_dt = None
            if item.get("created_at"):
                try:
                    dt_str = item.get("created_at").replace('Z', '+00:00')
                    created_at_dt = datetime.fromisoformat(dt_str)
                except ValueError:
                    pass

            # (2) DB에 넣을 딕셔너리 생성
            row_data = {
                "platform": item.get("platform"),
                "country": final_country,    # 복호화됨
                "email": encrypted_email,    # 암호화 유지
                "cbt_code": item.get("cbt_code"),
                "created_at": created_at_dt,
                "utm_source": item.get("utm_source"),
                "utm_medium": item.get("utm_medium"),
                "countryCode": final_countryCode,
                "synced_at": datetime.utcnow()
            }
            
            batch_buffer.append(row_data)

            # (3) 배치가 꽉 차면 DB로 전송 (Bulk Upsert)
            if len(batch_buffer) >= BATCH_SIZE:
                execute_bulk_upsert(session, batch_buffer)
                processed_count += len(batch_buffer)
                batch_buffer = [] # 버퍼 초기화
                logger.info(f"Processed {processed_count}/{total_items} items...")

        # (4) 남은 데이터 처리
        if batch_buffer:
            execute_bulk_upsert(session, batch_buffer)
            processed_count += len(batch_buffer)

        logger.info(f"Sync Complete. Total Processed: {processed_count}")

    except Exception as e:
        session.rollback()
        logger.error(f"Database Error: {e}")
        raise
    finally:
        session.close()


# # Airflow PythonOperator에서 호출할 때는 이 함수를 import해서 쓰거나,
# # main 블록을 통해 직접 실행할 수 있습니다.
# if __name__ == "__main__":
#     # 테스트용: 최근 10자리 타임스탬프 예시 (필요시 sys.argv로 받도록 수정 가능)
#     # 예: python etl_script.py 1765180743
#     target_unix = int(sys.argv[1]) if len(sys.argv) > 1 else None
#     fetch_and_store_data(target_unix)


# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
}

with DAG(
    dag_id='fsf2_cbt_pre_register_etl',
    default_args=default_args,
    description='fsf2 CBT Pre-Register Data ETL',
    schedule='30 0 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['marketing', 'fsf2', 'cbt'],
) as dag:

    def run_sync(**context):
            # Airflow에서는 sys.argv 대신 context 변수나 직접 값을 넘깁니다
            ts = int(context['data_interval_start'].timestamp())
            fetch_and_store_data(ts)

    fsf2_cbt_pre_register_etl_task = PythonOperator(
        task_id='fetch_and_store_data',
        python_callable=fetch_and_store_data,
        op_kwargs={'start_unix': None},
        dag=dag
    )

    bash_task = BashOperator(
        task_id = 'bash_task',
        outlets = [fsf2_cbt_pre_register_etl],
        bash_command = 'echo "producer_1 수행 완료"'
    )

    fsf2_cbt_pre_register_etl_task >> bash_task
