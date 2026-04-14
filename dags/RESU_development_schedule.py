from datetime import datetime, timedelta
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
import time

logger = logging.getLogger(__name__)

PROJECT_ID = 'datahub-478802'
DATASET_ID = 'RESU'
TABLE_ID = 'dim_development_schedule'
NOTION_DB_ID = '210ea67a568180e793abdc69ebfdbb71'  

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
            return date_obj.get("start"), date_obj.get("end")
        return None, None

    elif prop_type == "select":
        sel = prop.get("select")
        return sel.get("name") if sel else None

    elif prop_type == "multi_select":
        items = prop.get("multi_select", [])
        return ", ".join(i.get("name", "") for i in items)

    elif prop_type == "formula":
        formula = prop.get("formula", {})
        formula_type = formula.get("type")
        if formula_type == "number":
            return formula.get("number")
        elif formula_type == "string":
            return formula.get("string")
        return None

    elif prop_type == "number":
        return prop.get("number")

    return None


def fetch_notion_pages(start_date_filter: str, end_date_filter: str) -> list[dict]:
    """
    Notion DB에서 조건에 맞는 페이지를 모두 가져옵니다.
    - 파트 = 기획, 데이터
    - 분석 != 제외 (또는 비어있음)
    - 패치 날짜 시작일이 start_date_filter ~ end_date_filter 범위
    """
    notion_token = get_var('NOTION_TOKEN')
    headers = {
        "Authorization": f"Bearer {notion_token}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }
    url = f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query"
    payload = {
        "filter": {
            "and": [
                {
                    "or": [
                        {"property": "파트", "select": {"equals": "기획"}},
                        {"property": "파트", "select": {"equals": "데이터"}},
                    ]
                },
                {
                    "property": "패치 날짜",
                    "date": {"on_or_after": start_date_filter}
                },
                {
                    "property": "패치 날짜",
                    "date": {"on_or_before": end_date_filter}
                },
            ]
        },
        "page_size": 100,
    }

    all_pages = []
    while True:
        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        if not resp.ok:
            print(f"Notion API 오류: {resp.status_code} {resp.text}")
        resp.raise_for_status()
        data = resp.json()
        all_pages.extend(data.get("results", []))

        if not data.get("has_more"):
            break
        payload["start_cursor"] = data["next_cursor"]

    # 분석 = '제외' 인 행 Python 레벨에서 제외
    filtered = []
    for page in all_pages:
        analysis_val = extract_property(page, "분석")
        if analysis_val == "제외":
            continue
        filtered.append(page)

    print(f"Notion 전체 {len(all_pages)}개 중 필터 후 {len(filtered)}개 수집")
    return filtered


def build_rows(pages: list[dict]) -> list[dict]:
    """Notion 페이지 목록을 BQ 적재용 행으로 변환합니다."""
    rows = []
    for page in pages:
        milestone = extract_property(page, "마일스톤") or ""
        development_subject = extract_property(page, "개발 사항") or ""
        part = extract_property(page, "파트") or ""
        development_detail = extract_property(page, "기획서 요약") or ""

        # 패치 날짜: (start, end) 튜플 반환
        patch_date = page.get("properties", {}).get("패치 날짜", {})
        patch_type = patch_date.get("type")
        start_date = None
        end_date = None

        if patch_type == "date":
            date_obj = patch_date.get("date")
            if date_obj:
                raw_start = date_obj.get("start")
                raw_end = date_obj.get("end")
                if raw_start:
                    try:
                        start_date = pd.to_datetime(raw_start).date()
                    except Exception:
                        pass
                if raw_end:
                    try:
                        end_date = pd.to_datetime(raw_end).date()
                    except Exception:
                        pass

        rows.append({
            "milestone": milestone,
            "development_subject": development_subject,
            "part": part,
            "start_date": start_date,
            "end_date": end_date,
            "development_detail": development_detail,
        })

    return rows


def upsert_to_bigquery(client, df: pd.DataFrame):
    """DataFrame을 BigQuery에 MERGE(Upsert) 합니다."""
    if df.empty:
        print("적재할 데이터가 없습니다.")
        return

    target_table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    staging_table_id = f"{PROJECT_ID}.{DATASET_ID}.temp_dev_schedule_staging"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("milestone", "STRING"),
            bigquery.SchemaField("development_subject", "STRING"),
            bigquery.SchemaField("part", "STRING"),
            bigquery.SchemaField("start_date", "DATE"),
            bigquery.SchemaField("end_date", "DATE"),
            bigquery.SchemaField("development_detail", "STRING"),
        ]
    )

    try:
        print(f"Staging 테이블에 {len(df)}행 적재 중...")
        load_job = client.load_table_from_dataframe(df, staging_table_id, job_config=job_config)
        load_job.result()

        table = client.get_table(staging_table_id)
        table.expires = datetime.utcnow() + timedelta(hours=1)
        client.update_table(table, ["expires"])

        merge_query = f"""
        MERGE `{target_table_id}` T
        USING `{staging_table_id}` S
        ON T.milestone IS NOT DISTINCT FROM S.milestone
           AND T.development_subject IS NOT DISTINCT FROM S.development_subject
           AND T.start_date IS NOT DISTINCT FROM S.start_date
        WHEN MATCHED THEN
          UPDATE SET
            T.part = S.part,
            T.end_date = S.end_date,
            T.development_detail = S.development_detail
        WHEN NOT MATCHED THEN
          INSERT (milestone, development_subject, part, start_date, end_date, development_detail)
          VALUES (S.milestone, S.development_subject, S.part, S.start_date, S.end_date, S.development_detail)
        """

        print("MERGE 실행 중...")
        client.query(merge_query).result()
        print(f"Upsert 완료: {target_table_id} ({len(df)}행)")

    except Exception as e:
        print(f"Upsert 실패: {e}")
        raise e

    finally:
        client.delete_table(staging_table_id, not_found_ok=True)
        print(f"Staging 테이블 삭제 완료")


def load_development_schedule():
    """
    Notion 개발일정 DB에서 최근 2주(어제 기준) 데이터를 수집하여 BigQuery에 Upsert합니다.
    - 어제 날짜 기준으로 패치 날짜 시작일이 2주 이내인 행만 처리
    """
    yesterday = (datetime.now() - timedelta(days=1)).date()
    two_weeks_ago = yesterday - timedelta(days=14)

    start_date_filter = two_weeks_ago.strftime("%Y-%m-%d")
    end_date_filter = yesterday.strftime("%Y-%m-%d")

    print(f"수집 범위: {start_date_filter} ~ {end_date_filter}")

    pages = fetch_notion_pages(start_date_filter, end_date_filter)
    rows = build_rows(pages)

    if not rows:
        print("적재할 행이 없습니다.")
        return

    df = pd.DataFrame(rows)
    print(f"총 {len(df)}행 수집 완료")

    creds = get_gcp_credentials()
    client = bigquery.Client(project=PROJECT_ID, credentials=creds)
    upsert_to_bigquery(client, df)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

with DAG(
    dag_id='RESU_development_schedule',
    default_args=default_args,
    description='RESU 개발일정 DB를 Notion에서 BigQuery로 매일 Upsert (최근 2주 기준)',
    schedule='0 21 * * *',  # KST 06:00 AM → UTC 21:00 PM
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['RESU', 'notion', 'bigquery', 'development'],
) as dag:

    load_task = PythonOperator(
        task_id='load_development_schedule_to_bigquery',
        python_callable=load_development_schedule,
    )

    load_task
