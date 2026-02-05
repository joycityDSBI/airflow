from google.cloud import bigquery
from datetime import datetime, timezone, timedelta
import time
import os
import requests
import json
import pandas as pd
import logging

from google.oauth2 import service_account
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable


def get_var(key: str, default: str = None) -> str:
    """환경 변수 또는 Airflow Variable 조회"""
    return os.environ.get(key) or Variable.get(key, default_var=default)

logger = logging.getLogger(__name__)


PROJECT_ID = 'datahub-478802'
LOCATION = 'US'

BQ_DATASET_ID = 'datahub'
BQ_TABLE_ID = 'sensorTower_downloads_by_source'
CREDENTIALS_JSON = get_var('GOOGLE_CREDENTIAL_JSON')


# 가져올 게임의 unified app id (sensortower)
# 킹샷 : 67bb93ed47b43a18952ffdfc 2025/02/22
# 화이트아웃서버아벌 : 638ee532480da915a62f0b34
# 라스트워서바이벌 : 64075e77537c41636a8e1c58
# 라스트z 서바이벌 슈터 : 658ea0be1fc48c4dbb3065e6
# 타일즈 서바이버 : 67d3aaff2c328ae8e547d0ef
# 다크워 서바이벌 : 6573c39d5c3b423d5d04560f
# 레지던트 이블 : 686fdb56b1430f9d12eda7a5
# rise of kingdoms : 5ac2bdddcfc03208313848db
# POTC : 58dca1bc62d7d432f50018e9
# 건쉽배틀 total warfare: 5b997bca9ee67d1001967929
# world war : machines conquest : 5f6d6b6a18bf063c24c5d0a0
# 드래곤 엠파이어 : 625e3a06e0ba195166fbce2f

APP_ID_LIST = ['67bb93ed47b43a18952ffdfc',
                '638ee532480da915a62f0b34',
                '64075e77537c41636a8e1c58',
                '658ea0be1fc48c4dbb3065e6',
                '67d3aaff2c328ae8e547d0ef',
                '6573c39d5c3b423d5d04560f',
                '686fdb56b1430f9d12eda7a5',
                '5ac2bdddcfc03208313848db',
                '58dca1bc62d7d432f50018e9',
                '5b997bca9ee67d1001967929',
                '5f6d6b6a18bf063c24c5d0a0',
                '625e3a06e0ba195166fbce2f']

# APP_ID = '625e3a06e0ba195166fbce2f'
SENSORTOWER_TOKEN = get_var('SENSORTOWER_TOKEN')


def init_clients():
    """Task 내부에서 실행되어 필요한 클라이언트들을 생성하여 반환합니다."""
    creds = get_gcp_credentials()
    
    # 1. GCP Clients
    bq_client = bigquery.Client(project=PROJECT_ID, credentials=creds)
    
    return {
        "bq_client": bq_client
    }

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


def upsert_to_bigquery(client, df, PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID):
    """
    Dataframe을 BigQuery에 Upsert (Merge) 하는 함수
    Key: datekey, app_id
    """
    if df.empty:
        print("No data to upsert.")
        return

    # [수정 1] 타임스탬프 고정 (변수 재사용)
    timestamp = int(time.time())
    target_table_id = f"{PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}"
    staging_table_name = f"temp_staging_{timestamp}"
    staging_table_id = f"{PROJECT_ID}.{BQ_DATASET_ID}.{staging_table_name}"

    # 2. 데이터프레임을 임시 테이블에 적재
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
        schema=[
                    bigquery.SchemaField("datekey", "DATE"), # datekey는 무조건 DATE로 인식해라!
                ]
    )

    try:
        print(f"Loading data to staging table: {staging_table_id}...")
        # 테이블 적재 (이때 테이블이 생성됨)
        load_job = client.load_table_from_dataframe(df, staging_table_id, job_config=job_config)
        load_job.result() # 대기
        
        # [수정 2] 적재 후 만료 시간 업데이트
        # 이미 생성된 테이블 객체를 가져와서 만료 시간만 업데이트
        table = client.get_table(staging_table_id)
        table.expires = datetime.utcnow() + timedelta(hours=1)
        client.update_table(table, ["expires"]) # BigQuery에 반영
        
        # 3. MERGE 쿼리 생성
        columns = [col for col in df.columns]
        
        # [수정 3] 컬럼명에 Backtick(`) 추가하여 예약어 충돌 방지
        update_set = ", ".join([f"T.`{col}` = S.`{col}`" for col in columns if col not in ['datekey', 'app_id']])
        insert_cols = ", ".join([f"`{col}`" for col in columns])
        insert_values = ", ".join([f"S.`{col}`" for col in columns])

        merge_query = f"""
        MERGE `{target_table_id}` T
        USING `{staging_table_id}` S
        ON T.datekey = S.datekey AND T.app_id = S.app_id AND T.country = S.country
        
        WHEN MATCHED THEN
          UPDATE SET {update_set}
          
        WHEN NOT MATCHED THEN
          INSERT ({insert_cols})
          VALUES ({insert_values})
        """

        print("Executing MERGE query...")
        query_job = client.query(merge_query)
        query_job.result()
        
        print("Upsert complete.")

    except Exception as e:
        print(f"Upsert Failed: {e}")
        raise e

    finally:
        # [수정 4] 일관된 ID로 삭제 시도
        print(f"Dropping staging table: {staging_table_id}")
        client.delete_table(staging_table_id, not_found_ok=True)



# (1) SensorTower 다운로드 및 매출액 API 호출 함수
def sensortower_download_by_source_api(start_date, end_date, APP_ID, SENSORTOWER_TOKEN):
    
    client = init_clients()["bq_client"]

    for COUNTRY_CODE in [
            "WW", "AE", "AO", "AR", "AT", 
            "AU", "AZ", "BE", "BG", "BR", 
            "BY", "CA", "CH",
            "CL", "CN", "CO", "CR", "CZ",
            "DE", "DK", "DO", "DZ", "EC",
            "EG", "ES", "FI", "FR", "GB",
            "GH", "GR", "GT", "HK", "HR",
            "HU", "ID", "IE", "IL", "IN",
            "IT", "JP", "KE", "KR", "KW",
            "KZ", "LB", "LK", "LT", "LU",
            "MO", "MX", "MY", "NG", "NL",
            "NO", "NZ", "OM", "PA", "PE",
            "PH", "PK", "PL", "PT", "QA",
            "RO", "RU", "SA", "SE", "SG",
            "SI", "SK", "SV", "TH", "TN",
            "TR", "TW", "UA", "US", "UY",
            "UZ", "VE", "VN", "ZA"
    ]:
    
        downloads_revenue_url = f"https://api.sensortower.com/v1/unified/downloads_by_sources?app_ids={APP_ID}&countries={COUNTRY_CODE}&date_granularity=daily&start_date={start_date}&end_date={end_date}&auth_token={SENSORTOWER_TOKEN}"
        response = requests.get(downloads_revenue_url, timeout = 120)
        data = response.json()
        print(data)

        # 데이터가 비어있는지 확인
        if isinstance(data, dict) and "error" in data:
            raise ValueError(f"SensorTower API Error: {data['error']}")
        
        rows = []
        for item in data['data']:
            app_id = item['app_id']
            country = COUNTRY_CODE
            for entry in item['breakdown']:
                entry['app_id'] = app_id
                entry['country'] = country
                rows.append(entry)

        df = pd.DataFrame(rows)
            
        # (4) 전처리
        if 'date' in df.columns:
            # 1. 먼저 datetime으로 변환
            df['date'] = pd.to_datetime(df['date'])
            # 2. 시간(00:00:00)을 떼어내고 날짜(Date) 객체로 변환
            df['date'] = df['date'].dt.date
        
        numeric_cols = ["organic_abs", "organic_browse_abs", "organic_search_abs", "browser_abs", "paid_abs", "paid_search_abs"]
        
        for col in numeric_cols:
            if col in df.columns:
                # 1. 숫자로 변환 (에러 발생 시 NaN)
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # 2. NaN을 0으로 채움
                df[col] = df[col].fillna(0)
                
                # 3. [핵심] 정수형(int)으로 강제 변환 (소수점 버림)
                df[col] = df[col].astype(int)

        float_cols = ["organic_frac", "organic_browse_frac", "organic_search_frac", "browser_frac", "paid_frac", "paid_search_frac"]

        for col in float_cols:
            if col in df.columns:
                # 1. 숫자로 변환 (에러 발생 시 NaN)
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # 2. NaN을 0으로 채움
                df[col] = df[col].fillna(0.0)
                
                # 3. 실수형(float)으로 변환
                df[col] = df[col].astype(float)
        
        column_mapping = {
            'date': 'datekey'       # 왼쪽이 현재 df 컬럼명, 오른쪽이 빅쿼리 컬럼명
        }

        df.rename(columns=column_mapping, inplace=True)

        try:
            print("Upserting data to BigQuery...")
            upsert_to_bigquery(client, df, PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID)
            print("Data upserted successfully.")
        except Exception as e:
            print(f"Error during upsert: {e}")
            raise e

    
# start_date_str, end_date_str는 "YYYY-MM-DD" 형식의 문자열
# start_date는 6일 전, end_date는 오늘 날짜로 설정
def fetch_data_in_weekly_batches(total_start_str: str, total_end_str: str, APP_ID: str, SENSORTOWER_TOKEN: str):
    
    start_date = datetime.strptime(total_start_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(total_end_str, "%Y-%m-%d").date()
    current_start = start_date
    
    # 2. 루프: 현재 시작일이 전체 종료일보다 작거나 같을 때까지 반복
    while current_start <= end_date:
        
        # 3. 이번 배치의 종료일 계산 (시작일 + 6일 = 총 7일치)
        current_end = current_start + timedelta(days=6)
        
        # 4. 범위 보정: 계산된 종료일이 전체 종료일을 넘어가면, 전체 종료일로 맞춤
        if current_end > end_date:
            current_end = end_date
            
        # 5. 문자열로 다시 변환 (API 호출용)
        batch_start_str = current_start.strftime("%Y-%m-%d")
        batch_end_str = current_end.strftime("%Y-%m-%d")

        try:
            sensortower_download_by_source_api(batch_start_str, batch_end_str, APP_ID, SENSORTOWER_TOKEN)
            print(f"API 호출: {batch_start_str} ~ {batch_end_str} 수행 중...")
            
        except Exception as e:
            logger.error(f"Batch {batch_start_str} ~ {batch_end_str} failed: {e}")
            # 필요 시 여기서 raise를 하여 전체 작업을 멈출지, continue로 다음 주차로 넘어갈지 결정
            raise e
        
        # 6. 다음 배치를 위해 시작일 업데이트 (현재 종료일 + 1일)
        current_start = current_end + timedelta(days=1)

    logger.info("All batches processed successfully.")
    # print("All batches processed successfully.")



def app_id_downloads_by_source_fetch_load(APP_ID_LIST: list, SENSORTOWER_TOKEN: str):

    # 6일전 날짜를 가져오는 로직
    today = datetime.now().date()
    six_days_ago = today - timedelta(days=6)
    start_date_str = six_days_ago.strftime("%Y-%m-%d")
    end_date_str = today.strftime("%Y-%m-%d")

    for APP_ID in APP_ID_LIST:
        fetch_data_in_weekly_batches(total_start_str=start_date_str,
                                     total_end_str=end_date_str,
                                     APP_ID=APP_ID,
                                     SENSORTOWER_TOKEN=SENSORTOWER_TOKEN)
        print(f"✅ APP_ID {APP_ID} 데이터 처리 완료.")

    print("✅ 전체 데이터 처리 완료.")
    return True



###### 마이그레이션 용
def migration_data(APP_ID_LIST: list, SENSORTOWER_TOKEN: str, year_list: list = [2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]):

    # 6일전 날짜를 가져오는 로직
    month_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

    for year in year_list:
        for month in month_list:
            # 각 달의 시작일과 종료일 계산
            start_date = datetime(year, month, 1).date()
            if month == 12:
                end_date = datetime(year + 1, 1, 1).date() - timedelta(days=1)
            else:
                end_date = datetime(year, month + 1, 1).date() - timedelta(days=1)
            
            start_date_str = start_date.strftime("%Y-%m-%d")
            end_date_str = end_date.strftime("%Y-%m-%d")

            for APP_ID in APP_ID_LIST:
                fetch_data_in_weekly_batches(total_start_str=start_date_str,
                                             total_end_str=end_date_str,
                                             APP_ID=APP_ID,
                                             SENSORTOWER_TOKEN=SENSORTOWER_TOKEN)
                
                print(f"✅ APP_ID {APP_ID} 데이터 처리 완료 for {start_date_str} to {end_date_str}.")
        print(f"✅ {year} - {month} APP_ID {APP_ID} 데이터 처리 완료 ")

    print("✅ 전체 데이터 처리 완료.")
    return True


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
    dag_id='SENSORTOWER_downloads_by_source',
    default_args=default_args,
    description='센서타워 API를 통해 소스별 다운로드 데이터를 가져와 BigQuery에 적재',
    schedule= '30 21 * * *', # KST 06:30 AM 매일 실행 -> UTC 21:30 PM 전날 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['SensorTower', 'downloads_by_source', 'bigquery'],
) as dag:

    ## 실제 라이브 DAG
    # seonsortower_downloads_by_source_fetch_load_task = PythonOperator(
    #     task_id='seonsortower_downloads_by_source_fetch_load',
    #     python_callable=app_id_downloads_by_source_fetch_load,
    #     op_kwargs={
    #         'APP_ID_LIST': APP_ID_LIST,
    #         'SENSORTOWER_TOKEN': SENSORTOWER_TOKEN
    #     }
    # )
    

    MIGRATION_seonsortower_downloads_by_source_task = PythonOperator(
        task_id='MIGRATION_seonsortower_downloads_by_source',
        python_callable=migration_data,
        op_kwargs={
            'APP_ID_LIST': APP_ID_LIST,
            'SENSORTOWER_TOKEN': SENSORTOWER_TOKEN
        }
    )