

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
CREDENTIALS_JSON = get_var('GOOGLE_CREDENTIAL_JSON')

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


def daily_useage_df():
    
    client = init_clients()["bq_client"]

    query = f"""
            SELECT
            DATE(creation_time, 'Asia/Seoul') AS datekey,
            COUNT(*) AS query_count,
            ROUND(SUM(total_bytes_processed) / POW(1024, 4) * 6.25, 2) AS estimated_cost_usd
            FROM
            `region-US`.INFORMATION_SCHEMA.JOBS
            WHERE
            creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
            AND job_type = 'QUERY'
            AND state = 'DONE'
            AND error_result IS NULL
            GROUP BY datekey
            ORDER BY datekey DESC;
        """

    # 1. 쿼리 실행
    try :
        df = client.query(query).to_dataframe()
    
    except Exception as e:
        print(f"❌ 쿼리 실행 중 에러 발생: {e}")
        raise e
    
    return df


def user_useage_df():

    client = init_clients()["bq_client"]

    query = f"""
            SELECT
            user_email,
            COUNT(*) AS query_count,
            ROUND(SUM(total_bytes_processed) / POW(1024, 4), 4) AS total_tb,
            ROUND(SUM(total_bytes_processed) / POW(1024, 4) * 6.25, 2) AS estimated_cost_usd
            FROM
            `region-US`.INFORMATION_SCHEMA.JOBS
            WHERE
            creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
            AND job_type = 'QUERY'
            AND state = 'DONE'
            AND error_result IS NULL
            GROUP BY user_email
            ORDER BY estimated_cost_usd DESC;
        """

    # 1. 쿼리 실행
    try :
        df = client.query(query).to_dataframe()
    
    except Exception as e:
        print(f"❌ 쿼리 실행 중 에러 발생: {e}")
        raise e
    
    return df


def insert_daily_useage_notion():

    from notion_client import Client
    notion_token = get_var('NOTION_TOKEN')
    notion = Client(auth = notion_token)
    notion_db_id = '333ea67a568180929561c0a6eb60f3c3'
    
    df = daily_useage_df()
    df['datekey'] = pd.to_datetime(df['datekey']).dt.date
    df['query_count'] = pd.to_numeric(df['query_count'], errors='coerce').fillna(0).astype(int)
    df['estimated_cost_usd'] = pd.to_numeric(df['estimated_cost_usd'], errors='coerce').fillna(0).astype(float)

    if df.empty:
        print("No data to upsert.")
        return
    
    # 2. 쿼리 실행
    query_res = cast(Any, notion.databases.query(
        database_id=notion_db_id
    ))
    
    # 1. 기존 Notion DB 페이지 전체 삭제
    print("기존 페이지 삭제 중...")
    has_more = True
    start_cursor = None
    delete_count = 0

    while has_more:
        query_params = {"database_id": notion_db_id}
        if start_cursor:
            query_params["start_cursor"] = start_cursor

        query_res = cast(Any, notion.databases.query(**query_params))

        for page in query_res["results"]:
            notion.pages.update(
                page_id=page["id"],
                archived=True  # Notion에서 삭제 = archived 처리
            )
            delete_count += 1

        has_more = query_res.get("has_more", False)
        start_cursor = query_res.get("next_cursor")

    print(f"기존 {delete_count}건 삭제 완료")

    # 2. df 데이터 새로 INSERT
    print("새 데이터 삽입 중...")
    for idx, row in df.iterrows():
        datekey_str = str(row['datekey'])

        properties = {
            "datekey": {
                "date": {
                    "start": datekey_str
                }
            },
            "query_count": {
                "number": int(row['query_count'])
            },
            "estimated_cost_usd": {
                "number": float(row['estimated_cost_usd'])
            },
        }

        notion.pages.create(
            parent={"database_id": notion_db_id},
            properties=properties
        )

    print(f"완료: {delete_count}건 삭제 → {len(df)}건 새로 삽입")


def insert_user_useage_notion():

    from notion_client import Client
    notion_token = get_var('NOTION_TOKEN')
    notion = Client(auth = notion_token)
    notion_db_id = '333ea67a568180c9a412d186139300d3'
    
    df = user_useage_df()
    
    df['query_count'] = pd.to_numeric(df['query_count'], errors='coerce').fillna(0).astype(int)
    df['total_tb'] = pd.to_numeric(df['total_tb'], errors='coerce').fillna(0).astype(float)
    df['estimated_cost_usd'] = pd.to_numeric(df['estimated_cost_usd'], errors='coerce').fillna(0).astype(float)

    if df.empty:
        print("No data to upsert.")
        return
    
    # 2. 쿼리 실행
    query_res = cast(Any, notion.databases.query(
        database_id=notion_db_id
    ))
    
    # 1. 기존 Notion DB 페이지 전체 삭제
    print("기존 페이지 삭제 중...")
    has_more = True
    start_cursor = None
    delete_count = 0

    while has_more:
        query_params = {"database_id": notion_db_id}
        if start_cursor:
            query_params["start_cursor"] = start_cursor

        query_res = cast(Any, notion.databases.query(**query_params))

        for page in query_res["results"]:
            notion.pages.update(
                page_id=page["id"],
                archived=True  # Notion에서 삭제 = archived 처리
            )
            delete_count += 1

        has_more = query_res.get("has_more", False)
        start_cursor = query_res.get("next_cursor")

    print(f"기존 {delete_count}건 삭제 완료")

    # 2. df 데이터 새로 INSERT
    print("새 데이터 삽입 중...")
    for idx, row in df.iterrows():
        properties = {
            "user_email": {
                "rich_text": [
                    {
                        "text": {
                            "content": str(row['user_email'])
                        }
                    }
                ]
            },
            "query_count": {
                "number": int(row['query_count'])
            },
            "total_tb": {
                "number": float(row['total_tb'])
            },
            "estimated_cost_usd": {
                "number": float(row['estimated_cost_usd'])
            },
        }

        notion.pages.create(
            parent={"database_id": notion_db_id},
            properties=properties
        )

    print(f"완료: {delete_count}건 삭제 → {len(df)}건 새로 삽입")



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
    dag_id='datahub_useage_to_notion',
    default_args=default_args,
    description='데이터 허브 사용량에 대한 notion DB 업데이트',
    schedule= '30 22 * * *', # KST 06:30 AM 매일 실행 -> UTC 21:30 PM 전날 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['datahub', 'notion', 'useage'],
) as dag:


    insert_daily_useage_notion_task = PythonOperator(
        task_id='insert_daily_useage_notion_task',
        python_callable=insert_daily_useage_notion
    )

    insert_user_useage_notion_task = PythonOperator(
        task_id='insert_user_useage_notion_task',
        python_callable=insert_user_useage_notion
    )

    insert_daily_useage_notion_task >> insert_user_useage_notion_task