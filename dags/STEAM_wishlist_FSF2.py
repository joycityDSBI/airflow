from datetime import datetime, timedelta, timezone
import requests
import pandas as pd
import io
import urllib.parse
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models import Variable
import os

import logging
from google.cloud import bigquery
import json
from google.oauth2 import service_account
import time
from typing import Any, cast



def get_var(key: str, default: str = 'default') -> str:
    """환경 변수 또는 Airflow Variable 조회"""
    return os.environ.get(key) or Variable.get(key, default_var=default)

logger = logging.getLogger(__name__)


PROJECT_ID = 'datahub-478802'
LOCATION = 'US'

BQ_DATASET_ID = 'external_data'
BQ_TABLE_ID = 'steam_wishlist_region'
CREDENTIALS_JSON = get_var('GOOGLE_CREDENTIAL_JSON')

APP_ID = '4004820'


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


def steam_wishlist_to_bq_logic(APP_ID):
    # 1. 날짜 설정 (Airflow Execution Date 활용)
    # ds: YYYY-MM-DD 형식 (예: 2026-03-19)

    # 6일전 날짜를 가져오는 로직
    today = datetime.now().date()
    #################### 7일 전으로 변경 해야 함
    six_days_ago = today - timedelta(days=7)
    start_date_str = six_days_ago.strftime("%Y-%m-%d")
    end_date_str = today.strftime("%Y-%m-%d")

    app_id = APP_ID
    
    # 2. 동적 URL 생성 (템플릿 날짜 적용)
    url = (
        f"https://partner.steampowered.com/report_csv.php?"
        f"file=SteamRegionalWishlists_{app_id}_{start_date_str}_to_{end_date_str}&"
        f"params=query=QueryWishlistActionsByCountryForCSV^appID={app_id}^"
        f"dateStart={start_date_str}^dateEnd={end_date_str}^"
        f"interpreter=WishlistCountryReportInterpreter"
    )

    # 3. 쿠키 및 헤더 설정
    # ✅ 수정: cURL에서 -b 뒤에 있던 '순수 쿠키 문자열'만 넣어야 합니다.
    pure_cookie_string = (
        f"dateStart={start_date_str}; dateEnd={end_date_str}; priorDateStart={start_date_str};priorDateEnd={end_date_str}; "
        "steamCountry=KR%7Ca386dcd11830e0bc576765a90acdd364; "
        f"steamLoginSecure={get_var('STEAM_LOGIN_SECURE')}"
    )

    cookie_dict = {}
    for c in pure_cookie_string.split(';'):
        if '=' in c:
            key, value = c.split('=', 1) # 첫 번째 '='만 기준으로 나눔
            key = key.strip()
            value = value.strip()
            
            # ⚠️ 핵심: latin-1 에러 방지를 위해 값만 URL 인코딩 처리
            # 이미 인코딩된 값은 유지하고, 생으로 들어간 유니코드만 변환합니다.
            try:
                value.encode('latin-1')
                cookie_dict[key] = value
            except UnicodeEncodeError:
                cookie_dict[key] = urllib.parse.quote(value)
        
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
        "referer": f"https://partner.steampowered.com/app/wishlist/{app_id}/"
    }

    # 4. Steam 데이터 요청
    response = requests.get(url, headers=headers, cookies=cookie_dict, timeout=60)
    response.raise_for_status()

    if "<html>" in response.text.lower():
        raise Exception("❌ Steam Session Expired! Airflow Variable에서 쿠키를 갱신하세요.")

    # 5. CSV를 DataFrame으로 변환 (메모리 내에서 처리)
    # Steam CSV는 보통 첫 몇 줄에 메타데이터가 있을 수 있으니 상황에 따라 skiprows 조절 필요
    df = pd.read_csv(io.StringIO(response.text), skiprows=2)
    print("📧데이터프레임으로 변환 완료")

    column_mapping = {
        'DateLocal': 'datekey',
        'Game': 'game',
        'CountryCode': 'country_code',
        'Region': 'region',
        'Adds': 'adds',
        'Deletes': 'deletes',
        'PurchasesAndActivations': 'purchase_and_activations',
        'Gifts': 'gifts'
    }
    df.rename(columns=column_mapping, inplace=True)
    print("📧 컬럼 명 변경 완료")

    return df



def upsert_to_bigquery():
    """
    Dataframe을 BigQuery에 Upsert (Merge) 하는 함수
    Key: datekey, app_id
    """
    client = init_clients()["bq_client"]
    df = steam_wishlist_to_bq_logic(APP_ID)

    if df.empty:
        print("No data to upsert.")
        return
    
    # (4) 전처리
    if 'datekey' in df.columns:
        # 1. 먼저 datetime으로 변환
        df['datekey'] = pd.to_datetime(df['datekey'])
        # 2. 시간(00:00:00)을 떼어내고 날짜(Date) 객체로 변환
        df['datekey'] = df['datekey'].dt.date
    
    numeric_cols = ['adds', 'deletes', 'purchase_and_activations', 'gifts']
    for col in numeric_cols:
        if col in df.columns:
            # 1. 숫자로 변환 (에러 발생 시 NaN)
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 2. NaN을 0으로 채움
            df[col] = df[col].fillna(0)
            
            # 3. [핵심] 정수형(int)으로 강제 변환 (소수점 버림)
            df[col] = df[col].astype(int)

    # [수정 1] 타임스탬프 고정 (변수 재사용)
    timestamp = int(time.time())
    target_table_id = f"{PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}"
    staging_table_name = f"temp_steam_staging_{timestamp}"
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
        print(f"📧 Loading data to staging table: {staging_table_id}...")
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
        update_set = ", ".join([f"T.`{col}` = S.`{col}`" for col in columns if col not in ['datekey', 'game', 'country_code']])
        insert_cols = ", ".join([f"`{col}`" for col in columns])
        insert_values = ", ".join([f"S.`{col}`" for col in columns])

        merge_query = f"""
        MERGE `{target_table_id}` T
        USING `{staging_table_id}` S
        ON T.datekey = S.datekey AND T.game = S.game AND T.country_code = S.country_code
        
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


def upsert_to_notion(key_columns: list):
    
    from notion_client import Client
    notion_token = get_var('NOTION_TOKEN')
    notion = Client(auth = notion_token)
    notion_db_id = '32cea67a568180ce990ae74e85de7d3d'
    
    df = steam_wishlist_to_bq_logic(APP_ID)

    if df.empty:
        print("No data to upsert.")
        return
    
    for _, row in df.iterrows():
        # 1. 다중 키를 이용한 필터 생성 (AND 조건)
        # 각 컬럼의 타입에 맞게 필터를 구성해야 합니다 (여기서는 rich_text 기준)
        and_filter = []
        for col in key_columns:
            val = str(row[col])
            
            # 컬럼명이 'Date'이거나 날짜 형식인 경우 (실제 DB 컬럼명에 맞게 수정)
            if col == "datekey": 
                and_filter.append({
                    "property": col,
                    "date": {"equals": val}  # 'rich_text' 대신 'date' 사용
                })
            else:
                and_filter.append({
                    "property": col,
                    "rich_text": {"equals": val}
                })

        # 2. 쿼리 실행
        query_res = cast(Any, notion.databases.query(
            database_id=notion_db_id,
            filter={"and": and_filter}
        ))
        
        # 3. 속성 데이터 구성 (Upsert 대상 전체 데이터)
        properties = {}
        for col in df.columns:
            val = row[col]
            str_val = str(val)

            # [핵심 수정] 여기서는 properties에 담아야 합니다!
            if col == "Name": # Title 속성
                properties[col] = {"title": [{"text": {"content": str_val}}]}
            elif col == "datekey": # Date 속성
                properties[col] = {"date": {"start": str_val}}
            elif col in ["adds", "deletes", "purchase_and_activations", "gifts"]: # Number 속성
                properties[col] = {"number": int(val) if pd.notna(val) else 0}
            else: # 나머지 일반 텍스트
                properties[col] = {"rich_text": [{"text": {"content": str_val}}]}

        # 4. 결과에 따른 처리
        if query_res["results"]:
            page_id = query_res["results"][0]["id"]
            notion.pages.update(page_id=page_id, properties=properties)
            print(f"✅ Updated: {tuple(row[key_columns])}")
        else:
            notion.pages.create(parent={"database_id": notion_db_id}, properties=properties)
            print(f"✨ Created: {tuple(row[key_columns])}")
            
        time.sleep(0.4)



def steam_follower_etl():
    
    PROJECT_ID = 'datahub-478802'
    DATASET_ID = 'external_data'
    TABLE_NAME = 'steam_game_followers'
    TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"
    
    # [중요] 기존에 정의된 CREDENTIALS_JSON 사용
    creds_dict = json.loads(CREDENTIALS_JSON) if isinstance(CREDENTIALS_JSON, str) else CREDENTIALS_JSON
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    client = bigquery.Client(project=PROJECT_ID, credentials=credentials)

    # 2. 대상 게임 리스트
    target_games = {
        "4004820": "FSF2"
    }
    
    all_data = []
    today = datetime.now(timezone.utc).date()

    # 3. 데이터 수집
    for app_id, game_name in target_games.items():
        print(f"Steam 데이터 수집 중: {game_name}...")
        url = f"https://store.steampowered.com/api/requestedcounts?ids={app_id}"

        headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
        "referer": f"https://partner.steampowered.com/app/wishlist/{app_id}/"
        }

        try:
            res = requests.get(url, headers=headers, timeout=10)
            data = res.json()
            if data and 'follower_count' in data[0]:
                all_data.append({
                    'datekey': today,
                    'app_id': str(app_id),
                    'game_name': str(game_name),
                    'follower_count': int(data[0]['follower_count']),
                    'updated_at': datetime.now(timezone.utc)
                })
        except Exception as e:
            print(f"에러 발생 ({app_id}): {e}")
        time.sleep(1)

    if not all_data:
        print("적재할 데이터가 없습니다.")
        return

    # 4. 데이터프레임 생성 및 타입 정리
    df = pd.DataFrame(all_data)
    df['datekey'] = pd.to_datetime(df['datekey']).dt.date

    # 5. BigQuery 직접 적재 (Write Append)
    # 동일 날짜 데이터 중복 방지를 위해 실행 전 오늘 데이터 삭제 (선택 사항)
    delete_query = f"DELETE FROM `{TABLE_ID}` WHERE datekey = '{today}'"
    try:
        client.query(delete_query).result()
        print(f"기존 {today} 데이터 삭제 완료.")
    except Exception:
        print("기존 데이터가 없거나 테이블이 처음 생성됩니다.")

    # 데이터 적재 설정
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("datekey", "DATE"),
            bigquery.SchemaField("follower_count", "INTEGER"),
        ],
        write_disposition="WRITE_APPEND", # 데이터 추가
    )

    try:
        job = client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
        job.result() # 적재 완료 대기
        print(f"✅ 성공: {len(df)}행이 {TABLE_ID}에 적재되었습니다.")
    except Exception as e:
        print(f"❌ 적재 실패: {e}")

    # 6. Notion DB에 데이터 추가
    try:
        from notion_client import Client
        # Airflow 환경변수 등에서 토큰 가져오기 (기존 코드의 get_var 활용)
        notion_token = get_var('NOTION_TOKEN') 
        notion = Client(auth=notion_token)
        notion_db_id = '32dea67a568180d6b987d85d7384fe68'

        print(f"Notion DB 적재 시작...")
        
        for data in all_data:
            notion.pages.create(
                parent={"database_id": notion_db_id},
                properties={
                    # 'Name' 혹은 'app_id'가 Title 속성일 경우 (데이터베이스 설정에 따라 수정 필요)
                    "app_id": {
                        "title": [
                            {"text": {"content": data['app_id']}}
                        ]
                    },
                    "game_name": {
                        "rich_text": [
                            {"text": {"content": data['game_name']}}
                        ]
                    },
                    "follower_count": {
                        "number": data['follower_count']
                    },
                    "datekey": {
                        "date": {"start": data['datekey'].isoformat()}
                    },
                    "updated_at": {
                        "date": {"start": data['updated_at'].isoformat()}
                    }
                }
            )
        print(f"✅ 성공: {len(all_data)}개의 데이터가 Notion에 추가되었습니다.")

    except Exception as e:
        print(f"❌ Notion 적재 실패: {e}")





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
    dag_id='STEAM_wishlist_FSF2',
    default_args=default_args,
    description='FSF2 스팀 위시리스트 가져오기. STEAM_SECURE 정보 지속 교체 필요',
    schedule= '30 21 * * *', # KST 06:30 AM 매일 실행 -> UTC 21:30 PM 전날 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['steam', 'wishlist', 'bigquery'],
) as dag:


    upload_to_bigquery_task = PythonOperator(
        task_id='upload_to_bigquery_task',
        python_callable=upsert_to_bigquery
    )

    upload_to_notion_task = PythonOperator(
        task_id='upload_to_notion_task',
        python_callable=upsert_to_notion,
        op_kwargs={'key_columns': ['datekey', 'game', 'country_code']}
    )

    steam_follower_etl_task = PythonOperator(
        task_id='steam_follower_etl_task',
        python_callable=steam_follower_etl
    )


    steam_follower_etl_task >>upload_to_bigquery_task >> upload_to_notion_task 