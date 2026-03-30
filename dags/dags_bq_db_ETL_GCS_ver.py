"""
===============================================================
DAG: notion_bq_to_db_dag_test
설명: Notion 메타데이터 기반으로 BigQuery 데이터를 Databricks Delta Table에 적재
      (BQ Export Job → GCS → S3 → Databricks COPY INTO 방식, 서버 메모리 미사용)
===============================================================

[dags_bq_db_ETL.py 대비 변경점]
- BQ → 서버 메모리 → S3 방식 제거
- BQ Export Job으로 GCS에 직접 Parquet 적재 (서버 메모리 없이 BQ가 처리)
- GCS에 쌓인 Parquet 파일을 S3로 전송
- GCS cleanup 추가 (finally에서 항상 실행)
- 임시 물리 테이블 삭제 위치를 outer finally로 이동

[GCS 버킷]
- 테스트용: GCS_TEST_BUCKET 상수 ("test_bkbk")
- 운영 전환 시: Variable.get("GCS_EXPORT_BUCKET") 으로 교체

[필요한 Airflow Variables]
- NOTION_TOKEN         : Notion API 인증 토큰
- NOTION_DB_A          : Notion 테이블 메타 정보 DB ID
- NOTION_DB_B          : Notion 컬럼 메타 정보 DB ID
- NOTION_DB_C          : Notion 테이블 x 컬럼 상세 정보 DB ID
- BQ_DB_PROJECT_ID             : BigQuery 프로젝트 ID
- BQ_DB_ETL_GCP_CREDENTIAL_JSON: GCP 서비스 계정 JSON 문자열
- databricks_instance          : Databricks 인스턴스 URL
- DATABRICKS_TOKEN             : Databricks REST API 토큰
- DATABRICKS_HTTP_PATH         : Databricks SQL Warehouse HTTP Path
- S3_EXPORT_BUCKET     : S3 버킷명
- S3_EXPORT_PREFIX     : S3 경로 prefix (예: bq-exports/)
- AWS_ACCESS_KEY_ID    : AWS 액세스 키 ID
- AWS_SECRET_ACCESS_KEY: AWS 시크릿 액세스 키
- AWS_REGION           : AWS 리전
"""

import json
import time
import logging
import uuid
import requests
import pandas as pd
import boto3
from datetime import datetime, timedelta
from typing import List, Dict, Optional

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

from google.cloud import bigquery, bigquery_storage, storage
from google.oauth2 import service_account
from databricks import sql as databricks_sql

logger = logging.getLogger(__name__)

# ===============================================================
# 상수 정의
# ===============================================================
NOTION_API_VERSION = "2022-06-28"
NOTION_BASE_URL = "https://api.notion.com/v1"

TARGET_TEST_TABLES = [
    "aibi-service.Service_Set.AA_test_user_character_mapping",
    "aibi-service.Service_Set.AA_test_user_activity_daily",
    "aibi-service.Service_Set.AA_test_active_user_daily_item_flow",
    "aibi-service.Service_Set.AA_test_revenue_ingame_currency_and_iap_total"
]

DATE_COL_MAP = {
    "aibi-service.Service_Set.revenue_ingame_currency_and_iap_total": "datetime_kst",
    "aibi-service.Service_Set.user_activity_daily": "LogDateKST",
    "aibi-service.Service_Set.active_user_daily_item_flow": "date_kst",
    "aibi-service.Service_Set.user_character_mapping": "AuthAccountRegDateKST",
    "aibi-service.Service_Set.temp_user_activity_daily": "LogDateKST",
    "aibi-service.Service_Set.revenue_ingame_currency_and_iap_total_potc_after24": "datetime_kst"
}

TABLE_COL_MAP = {
    "Hub 테이블명": "hub_table",
    "지니전용_테이블명": "genie_table",
    "지니전용_테이블설명": "genie_table_desc",
    "저장할_bricks_schema": "target_schema",
    "PK": "pk",
    "FK 테이블": "fk_tables",
    "FK_qualified_tables": "fk_qualified_tables",
    "FK 테이블 PK": "fk_table_pk",
}

COLUMN_COL_MAP = {
    "Hub 테이블명": "hub_table",
    "Hub 컬럼명": "hub_column",
    "지니전용_컬럼명": "genie_column",
    "지니전용_컬럼설명": "genie_column_desc",
}

# 테스트용 GCS 버킷 (prod 전환 시: Variable.get("GCS_EXPORT_BUCKET") 으로 교체)
# GCS_TEST_BUCKET = "test_bkbk"
GCS_TEST_BUCKET = Variable.get("GCS_EXPORT_BUCKET").strip()
GCS_EXPORT_PREFIX = "bq-exports"

# ===============================================================
# 공용 Config 함수
# ===============================================================

def get_notion_config() -> dict:
    return {
        "token": Variable.get("NOTION_TOKEN"),
        "db_a": Variable.get("NOTION_DB_A"),
        "db_b": Variable.get("NOTION_DB_B"),
        "db_c": Variable.get("NOTION_DB_C"),
        "api_version": NOTION_API_VERSION,
    }

def get_bq_config() -> dict:
    return {
        "project_id": Variable.get("BQ_DB_PROJECT_ID"),
        "credentials_json": Variable.get("BQ_DB_ETL_GCP_CREDENTIAL_JSON"),
    }

def get_databricks_config() -> dict:
    return {
        "instance": Variable.get("databricks_instance"),
        "token": Variable.get("DATABRICKS_TOKEN"),
        "catalog": "datahub",
        "http_path": Variable.get("DATABRICKS_HTTP_PATH"),
    }

def get_s3_config() -> dict:
    return {
        "bucket": Variable.get("S3_EXPORT_BUCKET"),
        "prefix": Variable.get("S3_EXPORT_PREFIX"),
        "aws_access_key_id": Variable.get("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": Variable.get("AWS_SECRET_ACCESS_KEY"),
        "region": Variable.get("AWS_REGION"),
    }

def get_gcs_config() -> dict:
    return {
        # prod 전환 시 아래 줄을 Variable.get("GCS_EXPORT_BUCKET") 으로 교체
        "bucket": GCS_TEST_BUCKET,
        "prefix": GCS_EXPORT_PREFIX,
    }

# ===============================================================
# 공용 클라이언트 생성 함수
# ===============================================================

def get_notion_headers(config: dict) -> dict:
    return {
        "Authorization": f"Bearer {config['token']}",
        "Notion-Version": config["api_version"],
        "Content-Type": "application/json"
    }

def get_bq_client(config: dict) -> bigquery.Client:
    credentials_info = json.loads(config["credentials_json"])
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return bigquery.Client(project=config["project_id"], credentials=credentials)

def get_databricks_connection(config: dict):
    return databricks_sql.connect(
        server_hostname=config["instance"],
        http_path=config["http_path"],
        access_token=config["token"]
    )

def get_s3_client(s3_config: dict):
    return boto3.client(
        "s3",
        aws_access_key_id=s3_config["aws_access_key_id"],
        aws_secret_access_key=s3_config["aws_secret_access_key"],
        region_name=s3_config["region"]
    )

def get_gcs_client(bq_config: dict) -> storage.Client:
    """GCS 클라이언트 생성 (BQ와 동일한 서비스 계정 사용)"""
    credentials_info = json.loads(bq_config["credentials_json"])
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return storage.Client(project=bq_config["project_id"], credentials=credentials)

# ===============================================================
# 공용 유틸 함수
# ===============================================================

def extract_title_auto(page: dict) -> str:
    for key, prop in page["properties"].items():
        if prop["type"] == "title":
            return "".join([t["plain_text"] for t in prop["title"]])
    return ""

def extract_rollup_text_array(rollup_field: dict) -> str:
    try:
        items = rollup_field.get("rollup", {}).get("array", [])
        return " ".join([
            rt.get("plain_text", "")
            for item in items if item.get("type") == "rich_text"
            for rt in item.get("rich_text", [])
        ]).strip()
    except Exception:
        return ""

def notion_query_all(database_id: str, headers: dict, filter_payload=None, page_size: int = 100, sleep_sec: float = 0.3) -> list:
    url = f"{NOTION_BASE_URL}/databases/{database_id}/query"
    payload = {"page_size": min(100, max(1, page_size))}
    if filter_payload:
        payload["filter"] = filter_payload

    all_results = []
    start_cursor = None

    while True:
        if start_cursor:
            payload["start_cursor"] = start_cursor

        resp = requests.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()
        all_results.extend(data.get("results", []))

        if not data.get("has_more"):
            break

        start_cursor = data.get("next_cursor")
        if sleep_sec:
            time.sleep(sleep_sec)

    return all_results

def get_title_map(database_id: str, headers: dict) -> dict:
    results = notion_query_all(database_id, headers=headers)
    return {page["id"]: extract_title_auto(page) for page in results}

def map_fk_to_qualified_names(fk_tables: list, genie_to_schema: dict) -> list:
    return [f"{genie_to_schema.get(tbl, 'bqtable')}.{tbl}" for tbl in fk_tables]

def get_fk_table_pk_map(fk_tables: list, genie_table_to_pk_map: dict) -> dict:
    if not isinstance(fk_tables, list):
        return {}
    return {fk_table: genie_table_to_pk_map.get(fk_table, []) for fk_table in fk_tables}

def execute_databricks_sql(cursor, sql: str, description: str = "") -> None:
    try:
        logger.info(f"[Databricks SQL 실행] {description}\n{sql}")
        cursor.execute(sql)
        logger.info(f"[Databricks SQL 완료] {description}")
    except Exception as e:
        logger.error(f"[Databricks SQL 실패] {description} | 에러: {e}\nSQL: {sql}")
        raise

def get_databricks_schema(cursor, target_table: str) -> dict:
    try:
        cursor.execute(f"DESCRIBE TABLE {target_table}")
        rows = cursor.fetchall()
        return {row[0]: row[1].lower() for row in rows if row[0] and not row[0].startswith("#")}
    except Exception:
        return {}

# ===============================================================
# GCS 관련 함수 (신규)
# ===============================================================

def export_bq_to_gcs(bq_client: bigquery.Client, mat_table_id: str, gcs_bucket: str, gcs_export_prefix: str) -> str:
    """
    BigQuery 물리 테이블을 GCS Parquet으로 Export.

    - URI에 * wildcard 사용 → BQ가 자동 파일 분할 (1GB 제한 처리)
    - 반환: GCS URI 패턴 (예: gs://bucket/prefix/tablename/*.parquet)
    """
    gcs_uri = f"gs://{gcs_bucket}/{gcs_export_prefix}*.parquet"

    job_config = bigquery.ExtractJobConfig(
        destination_format=bigquery.DestinationFormat.PARQUET,
        compression=bigquery.Compression.SNAPPY,
    )

    logger.info(f"[BQ→GCS] Export 시작: {mat_table_id} → {gcs_uri}")

    job = bq_client.extract_table(
        source=mat_table_id,
        destination_uris=gcs_uri,
        job_config=job_config,
    )
    job.result()

    if job.errors:
        raise RuntimeError(f"[BQ→GCS] Export 실패: {job.errors}")

    logger.info(f"[BQ→GCS] Export 완료 → {gcs_uri}")
    return gcs_uri


def copy_gcs_to_s3(gcs_client: storage.Client, s3_client, gcs_bucket: str, gcs_prefix: str, s3_bucket: str, s3_prefix: str) -> int:
    """
    GCS prefix 하위 Parquet 파일을 S3로 복사.

    Returns:
        업로드된 파일 수
    """
    bucket = gcs_client.bucket(gcs_bucket)
    blobs = list(bucket.list_blobs(prefix=gcs_prefix))

    if not blobs:
        raise AirflowException(f"[GCS→S3] GCS에서 파일을 찾을 수 없습니다. (prefix: {gcs_prefix})")

    for blob in blobs:
        file_name = blob.name.split("/")[-1]
        s3_key = f"{s3_prefix}{file_name}"
        data = blob.download_as_bytes()
        s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=data)
        logger.info(f"[GCS→S3] 업로드 완료: gs://{gcs_bucket}/{blob.name} → s3://{s3_bucket}/{s3_key} ({blob.size / (1024**2):.2f} MB)")

    logger.info(f"[GCS→S3] 총 {len(blobs)}개 파일 전송 완료 → s3://{s3_bucket}/{s3_prefix}")
    return len(blobs)


def cleanup_gcs_prefix(gcs_client: storage.Client, bucket_name: str, prefix: str) -> None:
    """GCS prefix 하위 모든 오브젝트 삭제"""
    try:
        bucket = gcs_client.bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix=prefix))

        if blobs:
            bucket.delete_blobs(blobs)
            logger.info(f"[GCS Cleanup] 완료: {len(blobs)}개 파일 삭제 → gs://{bucket_name}/{prefix}")
        else:
            logger.warning(f"[GCS Cleanup] 대상 없음: gs://{bucket_name}/{prefix}")
    except Exception as e:
        logger.warning(f"[GCS Cleanup] 실패: {prefix} | 에러: {e}")

# ===============================================================
# S3 cleanup 유틸 (기존 유지)
# ===============================================================

def cleanup_s3_prefix(s3_client, bucket: str, prefix: str) -> None:
    """S3 prefix 하위 모든 오브젝트 삭제"""
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

        objects_to_delete = []
        for page in pages:
            for obj in page.get("Contents", []):
                objects_to_delete.append({"Key": obj["Key"]})

        if objects_to_delete:
            s3_client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": objects_to_delete}
            )
            logger.info(f"[S3 Cleanup] 완료: {len(objects_to_delete)}개 파일 삭제 → s3://{bucket}/{prefix}")
        else:
            logger.warning(f"[S3 Cleanup] 대상 없음: s3://{bucket}/{prefix}")
    except Exception as e:
        logger.warning(f"[S3 Cleanup] 실패: {prefix} | 에러: {e}")

# ===============================================================
# Task 1. Notion DB 수집 → XCom으로 전달
# ===============================================================

def task_collect_notion(**context):
    logger.info("[Task1] Notion DB 수집 시작")
    config = get_notion_config()
    headers = get_notion_headers(config)

    table_title_map = get_title_map(config["db_a"], headers)
    column_title_map = get_title_map(config["db_b"], headers)
    logger.info(f"[Task1] 타이틀 맵 수집 완료 - 테이블: {len(table_title_map)}건, 컬럼: {len(column_title_map)}건")

    data_a_results = notion_query_all(config["db_a"], headers=headers)
    table_info_list = []
    for page in data_a_results:
        props = page["properties"]
        hub_table = extract_title_auto(page)
        genie_tbl_name = (props.get("지니전용_테이블명", {}).get("rich_text", [{}])[0].get("plain_text", ""))
        genie_tbl_desc = (props.get("지니전용_테이블설명", {}).get("rich_text", [{}])[0].get("plain_text", ""))
        bricks_schema = props.get("저장할_bricks_schema", {}).get("select")
        bricks_schema_name = bricks_schema.get("name") if bricks_schema else "bqtable"
        pk_columns = [t.get("name", "") for t in props.get("PK(지니전용_컬럼명)", {}).get("multi_select", [])]
        fk_tables = [t.get("name", "") for t in props.get("FK 테이블", {}).get("multi_select", [])]

        table_info_list.append({
            "Hub 테이블명": hub_table,
            "지니전용_테이블명": genie_tbl_name,
            "지니전용_테이블설명": genie_tbl_desc,
            "저장할_bricks_schema": bricks_schema_name,
            "PK": pk_columns,
            "FK 테이블": fk_tables
        })

    data_c_results = notion_query_all(config["db_c"], headers=headers)
    column_info_list = []
    for page in data_c_results:
        props = page["properties"]
        table_rel = props.get("Hub 테이블명", {}).get("relation", [])
        column_rel = props.get("Hub 컬럼명", {}).get("relation", [])
        table_id = table_rel[0]["id"] if table_rel else ""
        column_id = column_rel[0]["id"] if column_rel else ""
        hub_table = table_title_map.get(table_id, "")
        hub_column = column_title_map.get(column_id, "")
        genie_col_name = extract_rollup_text_array(props.get("지니전용_컬럼명", {}))
        genie_col_desc = extract_rollup_text_array(props.get("지니전용_컬럼설명", {}))

        if hub_table and hub_column:
            column_info_list.append({
                "Hub 테이블명": hub_table,
                "Hub 컬럼명": hub_column,
                "지니전용_컬럼명": genie_col_name,
                "지니전용_컬럼설명": genie_col_desc
            })

    logger.info(f"[Task1] 수집 완료 - 테이블 메타: {len(table_info_list)}건, 컬럼 메타: {len(column_info_list)}건")

    context["ti"].xcom_push(key="table_info_list", value=table_info_list)
    context["ti"].xcom_push(key="column_info_list", value=column_info_list)

# ===============================================================
# Task 2. 메타데이터 전처리 → XCom으로 전달
# ===============================================================

def task_preprocess_metadata(**context):
    logger.info("[Task2] 메타데이터 전처리 시작")
    ti = context["ti"]

    table_info_list = ti.xcom_pull(task_ids="task_collect_notion", key="table_info_list")
    column_info_list = ti.xcom_pull(task_ids="task_collect_notion", key="column_info_list")

    if not table_info_list or not column_info_list:
        raise AirflowException("[Task2] XCom에서 메타데이터를 가져오지 못했습니다.")

    df_table = pd.DataFrame(table_info_list)
    df_column = pd.DataFrame(column_info_list)

    used_tables = set(df_column["Hub 테이블명"].unique())

    all_fk_tables = set()
    for fk_list in df_table["FK 테이블"]:
        if isinstance(fk_list, list):
            all_fk_tables.update(fk_list)
        elif isinstance(fk_list, str) and fk_list:
            all_fk_tables.add(fk_list)

    df_fk_table = df_table[df_table["지니전용_테이블명"].isin(all_fk_tables)].reset_index(drop=True)
    df_table = df_table[df_table["Hub 테이블명"].isin(used_tables)].reset_index(drop=True)

    genie_to_schema = dict(zip(df_table["지니전용_테이블명"], df_table["저장할_bricks_schema"]))
    genie_table_to_pk_map = dict(zip(df_fk_table["지니전용_테이블명"], df_fk_table["PK"]))

    df_table["FK_qualified_tables"] = df_table["FK 테이블"].apply(
        lambda x: map_fk_to_qualified_names(x, genie_to_schema)
    )
    df_table["FK 테이블 PK"] = df_table["FK 테이블"].apply(
        lambda x: get_fk_table_pk_map(x, genie_table_to_pk_map)
    )

    df_table_out = df_table.rename(columns=TABLE_COL_MAP)
    df_column_out = df_column.rename(columns=COLUMN_COL_MAP)
    table_list = df_table_out["hub_table"].dropna().unique().tolist()

    logger.info(f"[Task2] 전처리 완료 | 처리 대상 테이블 {len(table_list)}개: {table_list}")

    ti.xcom_push(key="df_table", value=df_table_out.to_json(orient="records"))
    ti.xcom_push(key="df_column", value=df_column_out.to_json(orient="records"))

    return [{"table_name": t} for t in table_list]

# ===============================================================
# Task 3. BigQuery → GCS → S3 → Databricks ETL (Dynamic Task Mapping)
# ===============================================================

def etl_single_table(table_name: str, **context) -> None:
    """
    단일 테이블 ETL (BQ Export → GCS → S3 → Databricks COPY INTO).
    Dynamic Task Mapping에 의해 table_list 개수만큼 병렬로 자동 호출됨.

    Flow:
    1. 메타데이터 로드 및 유효성 검사
    2. BQ 뷰 → 물리 temp table materialization
    3. BQ 물리 테이블에서 날짜별 row 수 집계 (BQ query)
    4. BQ Export → GCS Parquet (서버 메모리 미사용)
    5. GCS → S3 Parquet 전송
    6. Databricks DELETE (날짜 범위)
    7. Databricks COPY INTO (S3 Parquet으로부터, 컬럼 리네임 포함)
    8. GCS cleanup + S3 cleanup + 임시 물리 테이블 삭제 (finally에서 항상 실행)
    """
    ti = context["ti"]
    df_table = pd.DataFrame(json.loads(ti.xcom_pull(task_ids="task_preprocess_metadata", key="df_table")))
    df_column = pd.DataFrame(json.loads(ti.xcom_pull(task_ids="task_preprocess_metadata", key="df_column")))

    if table_name in TARGET_TEST_TABLES:
        logger.info(f"[ETL] 스킵 대상 테이블: {table_name}")
        return

    hub_date_col = DATE_COL_MAP.get(table_name)
    if hub_date_col is None:
        raise AirflowException(f"[ETL] 기준 날짜 컬럼 미정의: {table_name}")

    row = df_table[df_table["hub_table"] == table_name]
    if row.empty:
        raise AirflowException(f"[ETL] 테이블 메타 없음: {table_name}")
    row = row.iloc[0]

    schema = (row.get("target_schema") or "bqtable").strip()
    genie_table_name = row.get("genie_table") or table_name.split(".")[-1]
    db_config = get_databricks_config()
    bq_config = get_bq_config()
    s3_config = get_s3_config()
    gcs_config = get_gcs_config()
    catalog = db_config["catalog"]
    target_table = f"{catalog}.{schema}.{genie_table_name}"

    df_map = df_column[df_column["hub_table"] == table_name][["hub_column", "genie_column", "genie_column_desc"]]
    df_map = df_map[df_map["genie_column"].astype(str).str.len() > 0]
    col_rename_map = dict(zip(df_map["hub_column"], df_map["genie_column"]))
    genie_date_col = col_rename_map.get(hub_date_col, hub_date_col)

    # 클라이언트 생성
    bq_client = get_bq_client(bq_config)
    gcs_client = get_gcs_client(bq_config)
    conn = get_databricks_connection(db_config)
    cursor = conn.cursor()
    s3_client = get_s3_client(s3_config)

    # 경로 설정 (run_id 포함 → 실행마다 고유)
    table_short_name = table_name.split(".")[-1]
    dag_run_id = context["run_id"].replace(":", "_").replace("+", "_")
    gcs_export_prefix = f"{gcs_config['prefix']}/{table_short_name}/{dag_run_id}/"
    s3_export_prefix = f"{s3_config['prefix']}{table_short_name}/{dag_run_id}/"
    s3_export_path = f"s3://{s3_config['bucket']}/{s3_export_prefix}"

    mat_table_id = None
    delete_executed = False
    try:
        logger.info(f"[ETL] {target_table} 시작 | 소스: {table_name}")

        # Step 1: Delta 스키마 조회 (테이블 없으면 빈 dict)
        delta_schema = get_databricks_schema(cursor, target_table)
        if delta_schema:
            logger.info(f"[ETL] {target_table} | Delta 스키마 {len(delta_schema)}개 컬럼 로드 완료")

        # Step 2: BQ 뷰 → 물리 temp table materialization
        parts = table_name.split(".")
        mat_table_id = f"{parts[0]}.{parts[1]}.etl_tmp_{parts[2]}_{uuid.uuid4().hex[:8]}"
        logger.info(f"[ETL] {table_name} 물리화 시작 → {mat_table_id}")
        mat_job = bq_client.query(
            f"SELECT * FROM `{table_name}`",
            job_config=bigquery.QueryJobConfig(
                destination=bigquery.TableReference.from_string(mat_table_id),
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            ),
        )
        mat_job.result()
        mat_row_count = bq_client.get_table(mat_table_id).num_rows
        logger.info(f"[ETL] 물리화 완료: {mat_table_id} ({mat_row_count:,}행)")

        # Step 3: BQ 물리 테이블에서 날짜별 row 수 집계
        date_count_query = bq_client.query(
            f"SELECT DATE(`{hub_date_col}`) AS d, COUNT(*) AS cnt "
            f"FROM `{mat_table_id}` "
            f"WHERE `{hub_date_col}` IS NOT NULL "
            f"GROUP BY 1"
        )
        date_count_map = {str(r.d): int(r.cnt) for r in date_count_query.result()}

        if not date_count_map:
            raise AirflowException(f"[ETL] 기준 날짜 데이터 없음: {table_name}")

        bq_count_log = "\n".join([f"  {d}: {date_count_map[d]:,}건" for d in sorted(date_count_map)])
        logger.info(f"[ETL] {target_table} | BQ 날짜별 row수 (총 {sum(date_count_map.values()):,}건):\n{bq_count_log}")

        # Step 4: BQ Export → GCS (서버 메모리 미사용)
        export_bq_to_gcs(bq_client, mat_table_id, gcs_config["bucket"], gcs_export_prefix)

        # Step 5: GCS → S3 전송
        copy_gcs_to_s3(
            gcs_client, s3_client,
            gcs_config["bucket"], gcs_export_prefix,
            s3_config["bucket"], s3_export_prefix,
        )

        date_set = set(date_count_map.keys())
        date_in_sql = ", ".join([f"DATE('{d}')" for d in sorted(date_set)])

        # Step 6: Databricks DELETE (날짜 범위)
        cursor.execute(
            f"SELECT DATE({genie_date_col}) as date_key, COUNT(*) as row_count "
            f"FROM {target_table} WHERE to_date({genie_date_col}) IN ({date_in_sql}) "
            f"GROUP BY 1 ORDER BY 1"
        )
        pre_delete_map = {str(r[0]): r[1] for r in (cursor.fetchall() or []) if r[0] is not None}

        execute_databricks_sql(
            cursor,
            f"DELETE FROM {target_table} WHERE to_date({genie_date_col}) IN ({date_in_sql})",
            f"DELETE {target_table}"
        )
        delete_executed = True

        delete_count_log = "\n".join([f"  {d}: {pre_delete_map.get(d, 0):,}건" for d in sorted(date_set)])
        logger.info(f"[ETL] {target_table} | 삭제된 날짜별 row수 (총 {sum(pre_delete_map.values()):,}건):\n{delete_count_log}")

        # Step 7: Databricks COPY INTO (S3 Parquet으로부터)
        # BQ DATETIME → Parquet TIMESTAMP_NTZ 로 export 되는데,
        # Delta 테이블 컬럼이 TIMESTAMP(UTC-adjusted)이면 타입 불일치로 COPY INTO 실패.
        # delta_schema에서 timestamp 타입인 컬럼은 명시적으로 CAST 처리.
        select_cols = []
        for hub_col in df_map["hub_column"]:
            genie_col = col_rename_map.get(hub_col, hub_col)
            target_type = delta_schema.get(genie_col, "")
            if target_type == "timestamp":
                expr = f"CAST(`{hub_col}` AS TIMESTAMP)"
            else:
                expr = f"`{hub_col}`"

            if hub_col != genie_col:
                select_cols.append(f"{expr} AS `{genie_col}`")
            else:
                select_cols.append(expr)

        select_clause = ",\n            ".join(select_cols)

        copy_into_sql = f"""
                        COPY INTO {target_table}
                        FROM (
                            SELECT
                                {select_clause}
                            FROM '{s3_export_path}'
                        )
                        FILEFORMAT = PARQUET
                        COPY_OPTIONS ('mergeSchema' = 'true')
                        """

        execute_databricks_sql(cursor, copy_into_sql, f"COPY INTO {target_table}")

        cursor.execute(
            f"SELECT DATE({genie_date_col}) as date_key, COUNT(*) as row_count "
            f"FROM {target_table} WHERE to_date({genie_date_col}) IN ({date_in_sql}) "
            f"GROUP BY 1 ORDER BY 1"
        )
        post_insert_map = {str(r[0]): r[1] for r in (cursor.fetchall() or []) if r[0] is not None}

        insert_count_log = "\n".join([f"  {d}: {post_insert_map.get(d, 0):,}건" for d in sorted(date_set)])
        logger.info(f"[ETL] {target_table} | 적재 완료 날짜별 row수 (총 {sum(post_insert_map.values()):,}건):\n{insert_count_log}")

        logger.info(f"[ETL] {target_table} 적재 완료")

    except Exception as e:
        logger.error(f"[ETL] {target_table} 적재 실패: {e}")
        if delete_executed:
            logger.warning(f"[ETL] {target_table} DELETE 이후 실패 감지 → Time Travel 롤백 시도 (6시간 전)")
            try:
                execute_databricks_sql(
                    cursor,
                    f"CREATE OR REPLACE TABLE {target_table} SELECT * FROM {target_table} TIMESTAMP AS OF current_timestamp() - INTERVAL 6 HOURS",
                    f"ROLLBACK {target_table}"
                )
                logger.info(f"[ETL] {target_table} 롤백 완료")
            except Exception as rb_err:
                logger.error(f"[ETL] {target_table} 롤백 실패: {rb_err}")
        raise
    finally:
        cursor.close()
        conn.close()
        # Step 8: S3 / GCS cleanup + 임시 물리 테이블 삭제 (항상 실행)
        cleanup_s3_prefix(s3_client, s3_config["bucket"], s3_export_prefix)
        cleanup_gcs_prefix(gcs_client, gcs_config["bucket"], gcs_export_prefix)
        if mat_table_id:
            bq_client.delete_table(mat_table_id, not_found_ok=True)
            logger.info(f"[ETL] 임시 물리 테이블 삭제 완료: {mat_table_id}")

# ===============================================================
# Task 4. Databricks Delta Table 튜닝
# ===============================================================

def check_constraint_exists(cursor, catalog: str, schema: str, table_name: str, constraint_name: str, constraint_type: str) -> bool:
    query = f"""
        SELECT 1
        FROM {catalog}.information_schema.table_constraints
        WHERE constraint_catalog = '{catalog}'
          AND constraint_schema = '{schema}'
          AND table_name = '{table_name}'
          AND constraint_name = '{constraint_name}'
          AND constraint_type = '{constraint_type}'
        LIMIT 1
    """
    try:
        cursor.execute(query)
        return cursor.fetchone() is not None
    except Exception as e:
        logger.warning(f"[Task4] 제약조건 존재 여부 확인 실패 ({constraint_name}): {e}")
        return False


def task_tune_databricks(**context):
    logger.info("[Task4] Databricks Delta Table 튜닝 시작")
    ti = context["ti"]

    df_table = pd.DataFrame(json.loads(ti.xcom_pull(task_ids="task_preprocess_metadata", key="df_table")))
    db_config = get_databricks_config()
    catalog = db_config["catalog"]
    genie_to_schema = dict(zip(df_table["genie_table"], df_table["target_schema"]))

    conn = get_databricks_connection(db_config)
    cursor = conn.cursor()

    try:
        # Pass 1: 모든 테이블 PK 먼저 설정 (FK 참조 대상 PK가 반드시 존재해야 함)
        logger.info("[Task4] Pass 1 - PK 설정 시작")
        for _, row in df_table.iterrows():
            schema = row.get("target_schema", "bqtable")
            table_name = row.get("genie_table")
            pk_columns = row.get("pk", [])

            if not table_name:
                continue

            if isinstance(pk_columns, list) and pk_columns:
                for col in pk_columns:
                    execute_databricks_sql(
                        cursor,
                        f"ALTER TABLE {catalog}.{schema}.{table_name} ALTER COLUMN {col} SET NOT NULL",
                        f"NOT NULL {table_name}.{col}"
                    )

                pk_cols = ", ".join(pk_columns)
                constraint_name_pk = f"pk_{table_name}"
                if not check_constraint_exists(cursor, catalog, schema, table_name, constraint_name_pk, "PRIMARY KEY"):
                    execute_databricks_sql(
                        cursor,
                        f"ALTER TABLE {catalog}.{schema}.{table_name} ADD CONSTRAINT {constraint_name_pk} PRIMARY KEY ({pk_cols})",
                        f"PK {table_name}"
                    )
                else:
                    logger.info(f"[Task4] PK 이미 존재 - 스킵: {constraint_name_pk}")

        # Pass 2: FK + CLUSTER + OPTIMIZE
        logger.info("[Task4] Pass 2 - FK/CLUSTER/OPTIMIZE 시작")
        for _, row in df_table.iterrows():
            schema = row.get("target_schema", "bqtable")
            table_name = row.get("genie_table")
            fk_mapping = row.get("fk_table_pk", {})

            if not table_name:
                continue

            if isinstance(fk_mapping, dict):
                for fk_table, fk_cols in fk_mapping.items():
                    if not isinstance(fk_cols, list) or not fk_cols:
                        continue
                    target_schema = genie_to_schema.get(fk_table, "bqtable")
                    fk_cols_str = ", ".join(fk_cols)
                    constraint_name_fk = f"fk_{table_name}_to_{fk_table}"
                    if not check_constraint_exists(cursor, catalog, schema, table_name, constraint_name_fk, "FOREIGN KEY"):
                        execute_databricks_sql(
                            cursor,
                            (f"ALTER TABLE {catalog}.{schema}.{table_name} "
                             f"ADD CONSTRAINT {constraint_name_fk} FOREIGN KEY ({fk_cols_str}) "
                             f"REFERENCES {catalog}.{target_schema}.{fk_table}({fk_cols_str})"),
                            f"FK {table_name} → {fk_table}"
                        )
                    else:
                        logger.info(f"[Task4] FK 이미 존재 - 스킵: {constraint_name_fk}")

            execute_databricks_sql(cursor, f"ALTER TABLE {catalog}.{schema}.{table_name} CLUSTER BY auto", f"CLUSTER {table_name}")
            execute_databricks_sql(cursor, f"OPTIMIZE {catalog}.{schema}.{table_name}", f"OPTIMIZE {table_name}")

    except Exception as e:
        logger.error(f"[Task4] 튜닝 중 오류 발생: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

    logger.info("[Task4] Databricks Delta Table 튜닝 완료")

# ===============================================================
# DAG 정의
# ===============================================================

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="notion_bq_to_db_dag_GCS_ver",
    default_args=default_args,
    description="Notion 메타데이터 기반 BigQuery → GCS → S3 → Databricks ETL",
    schedule="50 22 * * *",  # 수동 실행 전용
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_tasks=10,
    tags=["etl", "notion", "bigquery", "databricks", "test"],
) as dag:

    collect_notion = PythonOperator(
        task_id="task_collect_notion",
        python_callable=task_collect_notion,
    )

    preprocess_metadata = PythonOperator(
        task_id="task_preprocess_metadata",
        python_callable=task_preprocess_metadata,
    )

    etl_tasks = PythonOperator.partial(
        task_id="task_etl_single_table",
        python_callable=etl_single_table,
    ).expand(op_kwargs=preprocess_metadata.output)

    tune_databricks = PythonOperator(
        task_id="task_tune_databricks",
        python_callable=task_tune_databricks,
    )

    collect_notion >> preprocess_metadata >> etl_tasks >> tune_databricks
