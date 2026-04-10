from datetime import datetime, timedelta, timezone
import re
import requests
import pandas as pd
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Variable
import os
import logging
from google.cloud import bigquery
import json
from google.oauth2 import service_account

logger = logging.getLogger(__name__)

PROJECT_ID = 'datahub-478802'
DATASET_ID = 'RESU'
TABLE_ID = 'dim_server_open_time'
NOTION_DB_ID = '33dea67a568180c89ec3fc570744fefd'


def get_var(key: str, default: str = 'default') -> str:
    return os.environ.get(key) or Variable.get(key, default=default)


def get_gcp_credentials():
    credentials_json = Variable.get('GOOGLE_CREDENTIAL_JSON')
    cred_dict = json.loads(credentials_json)
    if 'private_key' in cred_dict:
        cred_dict['private_key'] = cred_dict['private_key'].replace('\\n', '\n')
    return service_account.Credentials.from_service_account_info(
        cred_dict,
        scopes=[
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/bigquery",
        ]
    )


def parse_server_names(title_text: str) -> list[int]:
    """
    Notion '오픈 시간' 텍스트에서 서버 번호를 파싱합니다.
    - '#158 오픈' → [158]
    - '#134~135 오픈' or '#8~#37 오픈' → [134, 135, ..., 137]
    - 숫자가 없으면 빈 리스트 반환
    """
    # 범주형: #숫자~#숫자 or #숫자~숫자
    range_match = re.search(r'#?(\d+)\s*[~-]\s*#?(\d+)', title_text)
    if range_match:
        start, end = int(range_match.group(1)), int(range_match.group(2))
        return list(range(start, end + 1))

    # 단일: #숫자
    single_match = re.search(r'#(\d+)', title_text)
    if single_match:
        return [int(single_match.group(1))]

    return []


def fetch_notion_pages() -> list[dict]:
    """Notion DB에서 분류='신규 서버'인 페이지를 모두 가져옵니다."""
    notion_token = get_var('NOTION_TOKEN')
    headers = {
        "Authorization": f"Bearer {notion_token}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }
    url = f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query"
    payload = {
        "filter": {
            "property": "분류",
            "select": {"equals": "신규 서버"}
        },
        "page_size": 100,
    }

    pages = []
    while True:
        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        pages.extend(data.get("results", []))

        if not data.get("has_more"):
            break
        payload["start_cursor"] = data["next_cursor"]

    print(f"Notion에서 {len(pages)}개 페이지 수집 완료")
    return pages


def extract_property(page: dict, prop_name: str):
    """Notion 페이지에서 특정 프로퍼티 값을 추출합니다."""
    props = page.get("properties", {})
    prop = props.get(prop_name, {})
    prop_type = prop.get("type")

    if prop_type == "title":
        texts = prop.get("title", [])
        return "".join(t.get("plain_text", "") for t in texts)

    elif prop_type == "rich_text":
        texts = prop.get("rich_text", [])
        return "".join(t.get("plain_text", "") for t in texts)

    elif prop_type == "date":
        date_obj = prop.get("date")
        if date_obj:
            return date_obj.get("start")
        return None

    elif prop_type == "number":
        return prop.get("number")

    elif prop_type == "select":
        sel = prop.get("select")
        return sel.get("name") if sel else None

    return None


def load_server_open_time():
    """
    Notion DB에서 신규 서버 데이터를 가져와 BigQuery에 Truncate-Insert합니다.
    """
    pages = fetch_notion_pages()

    rows = []
    for page in pages:
        title_text = extract_property(page, "오픈 시간") or ""
        open_time_raw = extract_property(page, "시간")
        server_group_create_time_raw = extract_property(page, "서버 그룹 생성일")
        server_group_age = extract_property(page, "서버 그룹 나이")

        # open_time 파싱 (timestamp)
        open_time = None
        if open_time_raw:
            try:
                open_time = pd.to_datetime(open_time_raw).to_pydatetime()
            except Exception:
                pass

        # server_group_create_time 파싱 (date)
        server_group_create_time = None
        if server_group_create_time_raw:
            try:
                server_group_create_time = pd.to_datetime(server_group_create_time_raw).date()
            except Exception:
                pass

        # server_group_age (integer)
        age = int(server_group_age) if server_group_age is not None else None

        # 서버 번호 파싱 (범주형 처리)
        server_numbers = parse_server_names(title_text)

        if not server_numbers:
            print(f"서버 번호 파싱 실패, 스킵: '{title_text}'")
            continue

        for num in server_numbers:
            rows.append({
                "server_name": num,
                "open_time": open_time,
                "server_group_create_time": server_group_create_time,
                "server_group_age": age,
            })

    if not rows:
        print("적재할 데이터가 없습니다.")
        return

    df = pd.DataFrame(rows)
    df['server_name'] = df['server_name'].astype(int)
    df['server_group_age'] = pd.to_numeric(df['server_group_age'], errors='coerce').astype('Int64')

    print(f"총 {len(df)}행 BigQuery 적재 시작...")

    creds = get_gcp_credentials()
    client = bigquery.Client(project=PROJECT_ID, credentials=creds)
    target_table = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("server_name", "INTEGER"),
            bigquery.SchemaField("open_time", "TIMESTAMP"),
            bigquery.SchemaField("server_group_create_time", "DATE"),
            bigquery.SchemaField("server_group_age", "INTEGER"),
        ]
    )

    load_job = client.load_table_from_dataframe(df, target_table, job_config=job_config)
    load_job.result()
    print(f"BigQuery 적재 완료: {target_table} ({len(df)}행)")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

with DAG(
    dag_id='RESU_server_open_time',
    default_args=default_args,
    description='RESU 신규 서버 오픈 시간 메타데이터를 Notion에서 BigQuery로 적재 (Truncate-Insert)',
    schedule='0 21 * * *',  # KST 06:00 AM → UTC 21:00 PM
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['RESU', 'notion', 'bigquery', 'server'],
) as dag:

    load_server_open_time_to_bigquery_task = PythonOperator(
        task_id='load_server_open_time_to_bigquery',
        python_callable=load_server_open_time,
    )

    load_server_open_time_to_bigquery_task
