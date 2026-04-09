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
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import re



def get_var(key: str, default: str = 'default') -> str:
    """환경 변수 또는 Airflow Variable 조회"""
    return os.environ.get(key) or Variable.get(key, default_var=default)

logger = logging.getLogger(__name__)


PROJECT_ID = 'datahub-478802'
LOCATION = 'US'

BQ_DATASET_ID = 'external_data'
BQ_TABLE_ID = 'steam_wishlist_region'
BQ_TABLE_TRAFFIC_BREAKDOWN = 'steam_traffic_breakdown'
BQ_TABLE_TRAFFIC_COUNTRY = 'steam_traffic_country'
BQ_TABLE_UTM_VISITS = 'steam_utm_country'
BQ_TABLE_UTM_DAILY = 'steam_utm_visits_conversions'
CREDENTIALS_JSON = get_var('GOOGLE_CREDENTIAL_JSON')

APP_ID = '4004820'
GAME_NAME = 'FSF2'

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
        f"steamLoginSecure={get_var('STEAM_LOGIN_SECURE_PARTNER')}"
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


def fetch_steam_traffic_for_date(date_str: str):
    """
    Steam 트래픽 통계를 특정 날짜에 대해 가져옵니다.
    date_str: YYYY-MM-DD 형식
    Returns: (df_breakdown, df_country) 튜플
    """
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    url_date = dt.strftime("%m%%2F%d%%2F%Y")

    url = (
        f"https://partner.steamgames.com/apps/navtrafficstats/{APP_ID}"
        f"?attribution_filter=all&preset_date_range=custom"
        f"&start_date={url_date}&end_date={url_date}&format=csv"
    )

    pure_cookie_string = f"steamLoginSecure={get_var('STEAM_LOGIN_SECURE')}"

    cookie_dict = {}
    for c in pure_cookie_string.split(';'):
        if '=' in c:
            key, value = c.split('=', 1)
            key = key.strip()
            value = value.strip()
            try:
                value.encode('latin-1')
                cookie_dict[key] = value
            except UnicodeEncodeError:
                cookie_dict[key] = urllib.parse.quote(value)

    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
        "referer": f"https://partner.steamgames.com/apps/navtrafficstats/{APP_ID}"
    }

    response = requests.get(url, headers=headers, cookies=cookie_dict, timeout=60)
    response.raise_for_status()

    # BOM(﻿) 처리: utf-8-sig로 디코딩하면 BOM 자동 제거
    df = pd.read_csv(io.StringIO(response.content.decode('utf-8-sig')))
    print(f"[{date_str}] 트래픽 데이터 수집 완료: {len(df)}행")

    column_mapping = {
        'Page / Category': 'category',
        'Page / Feature': 'feature',
        'Impressions': 'impressions',
        'Visits': 'visits',
        'Owner Impressions': 'owner_impressions',
        'Owner Visits': 'owner_visits',
    }
    df.rename(columns=column_mapping, inplace=True)

    df['datekey'] = pd.to_datetime(date_str).date()
    df['game_name'] = GAME_NAME

    for col in ['impressions', 'visits', 'owner_impressions', 'owner_visits']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    df_country = df[df['category'] == 'Country'].copy()
    df_breakdown = df[df['category'] != 'Country'].copy()

    df_country = df_country.rename(columns={'feature': 'country_code'})
    df_country = df_country[['datekey', 'game_name', 'category', 'country_code',
                              'impressions', 'visits', 'owner_impressions', 'owner_visits']]

    df_breakdown = df_breakdown[['datekey', 'game_name', 'category', 'feature',
                                 'impressions', 'visits', 'owner_impressions', 'owner_visits']]

    return df_breakdown, df_country





def upsert_df_to_bigquery(client, df: pd.DataFrame, target_table_id: str, merge_keys: list):
    """DataFrame을 BigQuery에 MERGE(Upsert) 합니다."""
    if df.empty:
        print(f"No data to upsert for {target_table_id}.")
        return

    timestamp = int(time.time())
    staging_table_id = f"{PROJECT_ID}.{BQ_DATASET_ID}.temp_steam_traffic_staging_{timestamp}"

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
        schema=[bigquery.SchemaField("datekey", "DATE")]
    )

    try:
        print(f"Loading {len(df)}행 → staging: {staging_table_id}")
        load_job = client.load_table_from_dataframe(df, staging_table_id, job_config=job_config)
        load_job.result()

        table = client.get_table(staging_table_id)
        table.expires = datetime.utcnow() + timedelta(hours=1)
        client.update_table(table, ["expires"])

        columns = list(df.columns)
        # NULL-safe 비교: NULL = NULL은 FALSE이므로 IS NOT DISTINCT FROM 사용
        on_clause = " AND ".join([f"T.`{k}` IS NOT DISTINCT FROM S.`{k}`" for k in merge_keys])
        update_set = ", ".join([f"T.`{col}` = S.`{col}`" for col in columns if col not in merge_keys])
        insert_cols = ", ".join([f"`{col}`" for col in columns])
        insert_values = ", ".join([f"S.`{col}`" for col in columns])

        merge_query = f"""
        MERGE `{target_table_id}` T
        USING `{staging_table_id}` S
        ON {on_clause}
        WHEN MATCHED THEN
          UPDATE SET {update_set}
        WHEN NOT MATCHED THEN
          INSERT ({insert_cols})
          VALUES ({insert_values})
        """

        print(f"Executing MERGE → {target_table_id}...")
        client.query(merge_query).result()
        print(f"Upsert complete: {target_table_id}")

    except Exception as e:
        print(f"Upsert Failed [{target_table_id}]: {e}")
        raise e

    finally:
        client.delete_table(staging_table_id, not_found_ok=True)
        print(f"Staging table dropped: {staging_table_id}")


def steam_traffic_to_bigquery():
    """
    당일 기준 최근 7일 Steam 트래픽 데이터를 수집하여 BigQuery에 Upsert합니다.
    - Country 데이터 → steam_traffic_country
    - 나머지 데이터 → steam_traffic_breakdown
    """
    client = init_clients()["bq_client"]
    today = datetime.now().date()

    all_breakdown = []
    all_country = []

    for i in range(7):
        target_date = today - timedelta(days=i)
        date_str = target_date.strftime("%Y-%m-%d")
        print(f"[{i+1}/7] {date_str} 트래픽 데이터 수집 중...")
        df_breakdown, df_country = fetch_steam_traffic_for_date(date_str)
        all_breakdown.append(df_breakdown)
        all_country.append(df_country)
        time.sleep(1)

    df_breakdown_all = pd.concat(all_breakdown, ignore_index=True) if all_breakdown else pd.DataFrame()
    df_country_all = pd.concat(all_country, ignore_index=True) if all_country else pd.DataFrame()

    print(f"수집 완료 - breakdown: {len(df_breakdown_all)}행, country: {len(df_country_all)}행")

    upsert_df_to_bigquery(
        client, df_breakdown_all,
        f"{PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_TRAFFIC_BREAKDOWN}",
        merge_keys=['datekey', 'game_name', 'category', 'feature']
    )
    upsert_df_to_bigquery(
        client, df_country_all,
        f"{PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_TRAFFIC_COUNTRY}",
        merge_keys=['datekey', 'game_name', 'category', 'country_code']
    )


def fetch_steam_utm_for_date(date_str: str):
    """
    Steam UTM 트래픽/전환 데이터를 특정 날짜에 대해 가져옵니다.
    date_str: YYYY-MM-DD 형식
    Returns: DataFrame
    """
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    url_date = dt.strftime("%m%%2F%d%%2F%Y")

    url = (
        f"https://partner.steamgames.com/apps/utmtrafficstats/{APP_ID}"
        f"?preset_date_range=custom"
        f"&start_date={url_date}&end_date={url_date}&format=csv&content=country"
    )

    pure_cookie_string = f"steamLoginSecure={get_var('STEAM_LOGIN_SECURE')}"

    cookie_dict = {}
    for c in pure_cookie_string.split(';'):
        if '=' in c:
            key, value = c.split('=', 1)
            key = key.strip()
            value = value.strip()
            try:
                value.encode('latin-1')
                cookie_dict[key] = value
            except UnicodeEncodeError:
                cookie_dict[key] = urllib.parse.quote(value)

    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
        "referer": f"https://partner.steamgames.com/apps/utmtrafficstats/{APP_ID}"
    }

    response = requests.get(url, headers=headers, cookies=cookie_dict, timeout=60)
    response.raise_for_status()

    decoded = response.content.decode('utf-8-sig')
    # 구분자 자동 감지: 탭이 있으면 TSV, 없으면 CSV
    sep = '\t' if '\t' in decoded.split('\n')[0] else ','
    df = pd.read_csv(io.StringIO(decoded), sep=sep)
    print(f"[UTM {date_str}] 데이터 수집 완료: {len(df)}행, 컬럼: {list(df.columns)}")

    # 데이터 없으면 빈 DataFrame 반환
    if df.empty:
        return pd.DataFrame(columns=['datekey', 'game_name', 'source', 'campaign', 'medium',
                                     'content', 'term', 'country', 'visits', 'trusted_visits',
                                     'tracked_visits', 'returning_visits', 'wishlists',
                                     'purchases', 'activations'])

    column_mapping = {
        'Source': 'source',
        'Campaign': 'campaign',
        'Medium': 'medium',
        'Content': 'content',
        'Term': 'term',
        'Country': 'country',
        'Visits  (GMT)': 'visits',
        'Trusted Visits': 'trusted_visits',
        'Tracked Visits': 'tracked_visits',
        'Returning Visits': 'returning_visits',
        'Wishlists': 'wishlists',
        'Purchases': 'purchases',
        'Activations': 'activations',
    }
    df.rename(columns=column_mapping, inplace=True)

    df['datekey'] = pd.to_datetime(date_str).date()
    df['game_name'] = GAME_NAME

    int_cols = ['visits', 'trusted_visits', 'tracked_visits', 'returning_visits',
                'wishlists', 'purchases', 'activations']
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    str_cols = ['source', 'campaign', 'medium', 'content', 'term', 'country']
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].fillna('').astype(str)

    df = df[['datekey', 'game_name', 'source', 'campaign', 'medium', 'content', 'term',
             'country', 'visits', 'trusted_visits', 'tracked_visits', 'returning_visits',
             'wishlists', 'purchases', 'activations']]

    return df


def steam_utm_to_bigquery():
    """
    당일 기준 최근 7일 Steam UTM 트래픽/전환 데이터를 수집하여 BigQuery에 Upsert합니다.
    → steam_utm_visits_conversions
    """
    client = init_clients()["bq_client"]
    today = datetime.now().date()

    all_data = []

    for i in range(7):
        target_date = today - timedelta(days=i)
        date_str = target_date.strftime("%Y-%m-%d")
        print(f"[{i+1}/7] {date_str} UTM 데이터 수집 중...")
        df = fetch_steam_utm_for_date(date_str)
        all_data.append(df)
        time.sleep(1)

    df_all = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
    print(f"UTM 수집 완료: {len(df_all)}행")

    upsert_df_to_bigquery(
        client, df_all,
        f"{PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_UTM_VISITS}",
        merge_keys=['datekey', 'game_name', 'source', 'campaign', 'medium', 'content', 'term', 'country']
    )


def steam_utm_daily_to_bigquery():
    """
    당일 기준 최근 7일 Steam UTM daily 데이터를 단일 요청으로 수집하여 BigQuery에 Upsert합니다.
    → steam_utm_visits_conversions
    """
    client = init_clients()["bq_client"]
    today = datetime.now().date()
    seven_days_ago = today - timedelta(days=7)

    start_date_str = seven_days_ago.strftime("%m%%2F%d%%2F%Y")
    end_date_str = today.strftime("%m%%2F%d%%2F%Y")

    url = (
        f"https://partner.steamgames.com/apps/utmtrafficstats/{APP_ID}"
        f"?preset_date_range=custom"
        f"&start_date={start_date_str}&end_date={end_date_str}&format=csv&content=daily"
    )

    pure_cookie_string = f"steamLoginSecure={get_var('STEAM_LOGIN_SECURE')}"

    cookie_dict = {}
    for c in pure_cookie_string.split(';'):
        if '=' in c:
            key, value = c.split('=', 1)
            key = key.strip()
            value = value.strip()
            try:
                value.encode('latin-1')
                cookie_dict[key] = value
            except UnicodeEncodeError:
                cookie_dict[key] = urllib.parse.quote(value)

    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
        "referer": f"https://partner.steamgames.com/apps/utmtrafficstats/{APP_ID}"
    }

    response = requests.get(url, headers=headers, cookies=cookie_dict, timeout=60)
    response.raise_for_status()

    decoded = response.content.decode('utf-8-sig')
    sep = '\t' if '\t' in decoded.split('\n')[0] else ','
    df = pd.read_csv(io.StringIO(decoded), sep=sep)
    print(f"[UTM daily] 데이터 수집 완료: {len(df)}행, 컬럼: {list(df.columns)}")

    if df.empty:
        print("No UTM daily data to upsert.")
        return

    column_mapping = {
        'Date': 'datekey',
        'Source': 'source',
        'Campaign': 'campaign',
        'Medium': 'medium',
        'Content': 'content',
        'Term': 'term',
        'Device Type': 'device_type',
        'Visits  (GMT)': 'visits',
        'Trusted Visits': 'trusted_visits',
        'Tracked Visits': 'tracked_visits',
        'Returning Visits': 'returning_visits',
        'Wishlists': 'wishlists',
        'Purchases': 'purchases',
        'Activations': 'activations',
    }
    df.rename(columns=column_mapping, inplace=True)

    df['datekey'] = pd.to_datetime(df['datekey']).dt.date
    df['game_name'] = GAME_NAME

    int_cols = ['visits', 'trusted_visits', 'tracked_visits', 'returning_visits',
                'wishlists', 'purchases', 'activations']
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    str_cols = ['source', 'campaign', 'medium', 'content', 'term', 'device_type']
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].fillna('').astype(str)

    df = df[['datekey', 'game_name', 'source', 'campaign', 'medium', 'content', 'term',
             'device_type', 'visits', 'trusted_visits', 'tracked_visits', 'returning_visits',
             'wishlists', 'purchases', 'activations']]

    upsert_df_to_bigquery(
        client, df,
        f"{PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_UTM_DAILY}",
        merge_keys=['datekey', 'game_name', 'source', 'campaign', 'medium', 'content', 'term', 'device_type']
    )


def upsert_discord_members_to_notion():
    """
    Discord Invite API로 멤버 수를 가져와 Notion DB에 Upsert합니다.
    Key: datekey (KST 기준 오늘 날짜)
    """
    from notion_client import Client
    from typing import Any, cast

    # 1. KST 기준 오늘 날짜
    KST = timezone(timedelta(hours=9))
    today_kst = datetime.now(KST).strftime("%Y-%m-%d")

    # 2. Discord Invite API로 멤버 수 조회
    INVITE_CODE = "jXKR9qUFH4"
    discord_url = f"https://discord.com/api/v9/invites/{INVITE_CODE}?with_counts=true"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    resp = requests.get(discord_url, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    member_count = data.get("approximate_member_count")
    if member_count is None:
        raise ValueError(f"Discord API 응답에 approximate_member_count 없음: {data}")

    print(f"Discord 멤버 수: {member_count} (기준일: {today_kst})")

    # 3. Notion upsert
    notion_token = get_var('NOTION_TOKEN')
    notion = Client(auth=notion_token)
    notion_db_id = "33cea67a56818035b63ec74e33b74733"

    # 기존 레코드 조회 (datekey 기준)
    query_res = cast(Any, notion.databases.query(
        database_id=notion_db_id,
        filter={
            "property": "datekey",
            "date": {"equals": today_kst}
        }
    ))

    properties = {
        "datekey": {"date": {"start": today_kst}},
        "members": {"number": member_count},
    }

    if query_res["results"]:
        page_id = query_res["results"][0]["id"]
        notion.pages.update(page_id=page_id, properties=properties)
        print(f"Updated Notion page for {today_kst}: members={member_count}")
    else:
        notion.pages.create(
            parent={"database_id": notion_db_id},
            properties=properties
        )
        print(f"Created Notion page for {today_kst}: members={member_count}")


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

    upload_discord_members_to_notion_task = PythonOperator(
        task_id='upload_discord_members_to_notion_task',
        python_callable=upsert_discord_members_to_notion
    )

    upload_steam_traffic_task = PythonOperator(
        task_id='upload_steam_traffic_to_bigquery',
        python_callable=steam_traffic_to_bigquery
    )

    upload_steam_utm_task = PythonOperator(
        task_id='upload_steam_utm_to_bigquery',
        python_callable=steam_utm_to_bigquery
    )

    upload_steam_utm_daily_task = PythonOperator(
        task_id='upload_steam_utm_daily_to_bigquery',
        python_callable=steam_utm_daily_to_bigquery
    )

    upload_discord_members_to_notion_task >> upload_steam_traffic_task >> upload_steam_utm_task >> upload_steam_utm_daily_task >> upload_to_bigquery_task >> upload_to_notion_task
