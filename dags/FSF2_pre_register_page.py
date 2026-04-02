import json
import logging
import os
import time
import requests
import pandas as pd

from datetime import datetime, timedelta

from google.cloud import bigquery
from google.oauth2 import service_account

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator


logger = logging.getLogger(__name__)

# ==========================================
# 설정
# ==========================================
# API_URL = "https://freestylefootball2.com/api/pre-register/v2-cbt/stats"  # [LIVE]
API_URL = "https://qa-fsf2.joycity.com/api/pre-register/v2-cbt/stats"  # [QA]

PROJECT_ID = "datahub-478802"
BQ_DATASET_ID = "external_data"
BQ_TABLE_ID = "pre_register_page_view"
TARGET_TABLE = f"{PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}"

PROJECT_KEY = "FSF2"
LOOKBACK_DAYS = 7


def get_gcp_credentials():
    """Airflow Variable에서 GCP 자격 증명을 로드합니다."""
    credentials_json = Variable.get("GOOGLE_CREDENTIAL_JSON")
    cred_dict = json.loads(credentials_json)
    if "private_key" in cred_dict:
        cred_dict["private_key"] = cred_dict["private_key"].replace("\\n", "\n")

    scopes = [
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/bigquery",
    ]
    return service_account.Credentials.from_service_account_info(cred_dict, scopes=scopes)


def fetch_and_merge_page_view_stats(**context):
    """
    FSF2 2차 CBT 페이지 방문 통계 API에서 최근 7일치 데이터를 가져와
    BigQuery pre_register_page_view 테이블에 MERGE 합니다.
    """
    today = datetime.now().date()
    from_date = (today - timedelta(days=LOOKBACK_DAYS - 1)).strftime("%Y-%m-%d")
    to_date = today.strftime("%Y-%m-%d")

    logger.info(f"Fetching page view stats: {from_date} ~ {to_date}")

    # ---- 1. API 호출 ----
    response = requests.get(
        API_URL,
        params={"from_date": from_date, "to_date": to_date},
        headers={"Accept": "application/json", "Content-Type": "application/json"},
        timeout=60,
    )
    response.raise_for_status()

    body = response.json()
    status = body.get("status")
    if status != 200:
        raise ValueError(f"API returned status {status}: {body.get('description', '')}")

    rows = body.get("data", {}).get("rows", [])
    if not rows:
        logger.info("No rows returned from API. Nothing to merge.")
        return

    logger.info(f"Fetched {len(rows)} rows from API.")

    # ---- 2. DataFrame 생성 및 전처리 ----
    df = pd.DataFrame(rows)

    # 타입 변환
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["page_view_count"] = pd.to_numeric(df["page_view_count"], errors="coerce").fillna(0).astype(int)
    df["unique_visitor_count"] = pd.to_numeric(df["unique_visitor_count"], errors="coerce").fillna(0).astype(int)
    df["utm_source"] = df["utm_source"].fillna("")
    df["utm_medium"] = df["utm_medium"].fillna("")

    # 컬럼명 변경: date → datekey
    df.rename(columns={"date": "datekey"}, inplace=True)

    # project_name 컬럼 추가
    df.insert(0, "project_name", PROJECT_KEY)

    # 컬럼 순서 정렬
    df = df[["project_name", "datekey", "country", "utm_source", "utm_medium", "page_view_count", "unique_visitor_count"]]

    logger.info(f"DataFrame shape: {df.shape}")

    # ---- 3. BigQuery 클라이언트 초기화 ----
    creds = get_gcp_credentials()
    bq_client = bigquery.Client(project=PROJECT_ID, credentials=creds)

    # ---- 4. 스테이징 테이블에 적재 ----
    timestamp = int(time.time())
    staging_table_id = f"{PROJECT_ID}.{BQ_DATASET_ID}.temp_pre_register_page_view_{timestamp}"

    schema = [
        bigquery.SchemaField("project_name", "STRING"),
        bigquery.SchemaField("datekey", "DATE"),
        bigquery.SchemaField("country", "STRING"),
        bigquery.SchemaField("utm_source", "STRING"),
        bigquery.SchemaField("utm_medium", "STRING"),
        bigquery.SchemaField("page_view_count", "INTEGER"),
        bigquery.SchemaField("unique_visitor_count", "INTEGER"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE",
    )

    try:
        logger.info(f"Loading {len(df)} rows to staging table: {staging_table_id}")
        load_job = bq_client.load_table_from_dataframe(df, staging_table_id, job_config=job_config)
        load_job.result()

        # 스테이징 테이블 1시간 후 만료 설정
        table = bq_client.get_table(staging_table_id)
        table.expires = datetime.utcnow() + timedelta(hours=1)
        bq_client.update_table(table, ["expires"])

        # ---- 5. MERGE 쿼리 실행 ----
        merge_query = f"""
        MERGE `{TARGET_TABLE}` T
        USING `{staging_table_id}` S
        ON  T.project_name = S.project_name
        AND T.datekey      = S.datekey
        AND T.country      = S.country
        AND T.utm_source   = S.utm_source
        AND T.utm_medium   = S.utm_medium

        WHEN MATCHED THEN
          UPDATE SET
            T.page_view_count      = S.page_view_count,
            T.unique_visitor_count = S.unique_visitor_count

        WHEN NOT MATCHED THEN
          INSERT (project_name, datekey, country, utm_source, utm_medium, page_view_count, unique_visitor_count)
          VALUES (S.project_name, S.datekey, S.country, S.utm_source, S.utm_medium, S.page_view_count, S.unique_visitor_count)
        """

        logger.info("Executing MERGE query...")
        query_job = bq_client.query(merge_query)
        query_job.result()
        logger.info("MERGE complete.")

    except Exception as e:
        logger.error(f"BigQuery operation failed: {e}")
        raise

    finally:
        logger.info(f"Dropping staging table: {staging_table_id}")
        bq_client.delete_table(staging_table_id, not_found_ok=True)


# ==========================================
# DAG 정의
# ==========================================
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="FSF2_pre_register_page_view_stats",
    default_args=default_args,
    description="FSF2 2차 CBT 페이지 방문 통계 API → BigQuery MERGE (최근 7일)",
    schedule="0 1 * * *",  # KST 10:00 AM 매일 실행 (UTC 01:00)
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["fsf2", "pre_register", "bigquery", "page_view"],
) as dag:

    fetch_and_merge_task = PythonOperator(
        task_id="fetch_and_merge_page_view_stats",
        python_callable=fetch_and_merge_page_view_stats,
    )
