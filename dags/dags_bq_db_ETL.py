"""
===============================================================
DAG: notion_to_databricks_dag
설명: Notion 메타데이터 기반으로 BigQuery 데이터를 Databricks Delta Table에 적재
===============================================================

[필요한 Airflow Variables]
- NOTION_TOKEN         : Notion API 인증 토큰 (O)
- NOTION_DB_A          : Notion 테이블 메타 정보 DB ID (O)
- NOTION_DB_B          : Notion 컬럼 메타 정보 DB ID (O)
- NOTION_DB_C          : Notion 테이블 x 컬럼 상세 정보 DB ID (O)
- BQ_DB_PROJECT_ID             : BigQuery 프로젝트 ID (parentProject)
- BQ_DB_ETL_GCP_CREDENTIAL_JSON: GCP 서비스 계정 JSON 문자열 (base64 인코딩 없이 원본 JSON)
- databricks_instance          : Databricks 인스턴스 URL (예: adb-xxxx.azuredatabricks.net)
- DATABRICKS_TOKEN             : Databricks REST API 토큰
- DATABRICKS_HTTP_PATH         : Databricks SQL Warehouse HTTP Path
- (DATABRICKS_CATALOG는 스크립트에 'datahub'로 고정)
- S3_EXPORT_BUCKET     : S3 버킷명 (BQ 데이터 Parquet 저장 대상)
- S3_EXPORT_PREFIX     : S3 경로 prefix (예: bq-exports/)
- AWS_ACCESS_KEY_ID    : AWS 액세스 키 ID (boto3 + Databricks COPY INTO용)
- AWS_SECRET_ACCESS_KEY: AWS 시크릿 액세스 키
- AWS_REGION           : AWS 리전 (예: us-east-1)

[스킵 대상 테이블 - TARGET_TEST_TABLES 상수로 관리]
- 블랙리스트 테이블은 코드 내 TARGET_TEST_TABLES 리스트에서 직접 관리
"""

import json
import time
import io
import logging
import uuid
import requests
import pandas as pd
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
from typing import List, Dict, Optional

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

from google.cloud import bigquery, bigquery_storage
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

def get_bq_storage_client(config: dict) -> bigquery_storage.BigQueryReadClient:
    credentials_info = json.loads(config["credentials_json"])
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return bigquery_storage.BigQueryReadClient(credentials=credentials)

def get_databricks_connection(config: dict):
    return databricks_sql.connect(
        server_hostname=config["instance"],
        http_path=config["http_path"],
        access_token=config["token"]
    )

def get_s3_client(s3_config: dict):
    """boto3 S3 클라이언트 생성"""
    return boto3.client(
        "s3",
        aws_access_key_id=s3_config["aws_access_key_id"],
        aws_secret_access_key=s3_config["aws_secret_access_key"],
        region_name=s3_config["region"]
    )

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
    """Databricks SQL 실행 + 디버깅 로그"""
    try:
        logger.info(f"[Databricks SQL 실행] {description}\n{sql}")
        cursor.execute(sql)
        logger.info(f"[Databricks SQL 완료] {description}")
    except Exception as e:
        logger.error(f"[Databricks SQL 실패] {description} | 에러: {e}\nSQL: {sql}")
        raise

def get_databricks_schema(cursor, target_table: str) -> dict:
    """
    Delta 테이블 스키마 조회.

    Returns:
        {genie_col: delta_type} (예: {'user_id': 'bigint', 'amount': 'double'})
        테이블이 없으면 빈 dict 반환 (첫 적재 케이스).
    """
    try:
        cursor.execute(f"DESCRIBE TABLE {target_table}")
        rows = cursor.fetchall()
        return {row[0]: row[1].lower() for row in rows if row[0] and not row[0].startswith("#")}
    except Exception:
        return {}


def _cast_to_delta_schema(df: pd.DataFrame, delta_schema: dict, col_rename_map: dict) -> pd.DataFrame:
    """
    Databricks Delta 스키마에 맞게 DataFrame 컬럼 타입 변환.
    BQ nullable INT64 → pandas float64 → Parquet DOUBLE 문제를 방지.
    """
    reverse_map = {v: k for k, v in col_rename_map.items()}
    for genie_col, delta_type in delta_schema.items():
        hub_col = reverse_map.get(genie_col, genie_col)
        if hub_col not in df.columns:
            continue
        try:
            if delta_type in ("bigint", "long"):
                df[hub_col] = df[hub_col].astype(pd.Int64Dtype())
            elif delta_type in ("int", "integer"):
                df[hub_col] = df[hub_col].astype(pd.Int32Dtype())
            elif delta_type in ("smallint", "short"):
                df[hub_col] = df[hub_col].astype(pd.Int16Dtype())
            elif delta_type == "tinyint":
                df[hub_col] = df[hub_col].astype(pd.Int8Dtype())
            elif delta_type == "boolean":
                df[hub_col] = df[hub_col].astype(pd.BooleanDtype())
            elif delta_type in ("double", "float"):
                df[hub_col] = df[hub_col].astype(pd.Float64Dtype())
            elif delta_type in ("string", "varchar", "char"):
                df[hub_col] = df[hub_col].astype(pd.StringDtype())
        except Exception as e:
            logger.warning(f"[타입 변환 실패] {hub_col} ({delta_type}): {e}")
    return df


def upload_bq_to_s3_parquet(bq_client, bqstorage_client, table_name: str, s3_client, bucket: str, s3_prefix: str, hub_date_col: str, delta_schema: dict = None, col_rename_map: dict = None) -> tuple:
    """
    BQ 뷰테이블을 물리 테이블로 먼저 materialization한 뒤 S3에 Parquet으로 업로드.
    물리화로 Storage API 읽기 성능을 확보하며, 완료 후 임시 테이블은 자동 삭제됨.

    Args:
        bq_client: BigQuery Client
        bqstorage_client: BigQuery Storage API Client
        table_name: BQ 테이블명 (project.dataset.table)
        s3_client: boto3 S3 Client
        bucket: S3 버킷명
        s3_prefix: S3 경로 prefix (예: "bq-exports/table/run_id/")
        hub_date_col: 날짜 기준 컬럼명
        delta_schema: Databricks Delta 테이블 스키마 ({genie_col: delta_type}). 타입 캐스팅에 사용.
        col_rename_map: BQ→Databricks 컬럼 rename 매핑 ({hub_col: genie_col}). delta_schema와 함께 사용.

    Returns:
        (s3_path, date_count_map): S3 경로, 날짜별 row 수 dict
    """
    logger.info(f"[BQ→S3] {table_name} Parquet 업로드 시작 → s3://{bucket}/{s3_prefix}")

    # 뷰테이블 → 물리 테이블 materialization (소스와 동일 project.dataset에 임시 저장)
    parts = table_name.split(".")
    mat_table_id = f"{parts[0]}.{parts[1]}.etl_tmp_{parts[2]}_{uuid.uuid4().hex[:8]}"
    logger.info(f"[BQ→S3] {table_name} 물리화 시작 → {mat_table_id}")
    mat_job = bq_client.query(
        f"SELECT * FROM `{table_name}`",
        job_config=bigquery.QueryJobConfig(
            destination=bigquery.TableReference.from_string(mat_table_id),
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ),
    )
    mat_job.result()
    mat_row_count = bq_client.get_table(mat_table_id).num_rows
    logger.info(f"[BQ→S3] 물리화 완료: {mat_table_id} ({mat_row_count:,}행)")

    try:
        # 물리 테이블에서 Storage API로 읽기
        query_job = bq_client.query(f"SELECT * FROM `{mat_table_id}`")

        # 청크를 200MB(비압축 Arrow 기준) 단위로 합쳐 S3에 Parquet 업로드
        TARGET_BYTES = 200 * 1024 * 1024  # 200MB
        part_idx = 0
        uploaded_files = []
        accumulated_tables = []
        accumulated_bytes = 0
        date_count_map = {}

        def _flush(tables, part_idx):
            merged = pa.concat_tables(tables, promote_options="default")
            buffer = io.BytesIO()
            pq.write_table(merged, buffer)
            file_size = buffer.tell()
            buffer.seek(0)
            s3_key = f"{s3_prefix}part-{part_idx:05d}.parquet"
            s3_client.put_object(Bucket=bucket, Key=s3_key, Body=buffer.getvalue())
            logger.info(f"[BQ→S3] 업로드 완료: part-{part_idx:05d}.parquet ({len(merged):,} 행, {file_size/1024/1024:.1f}MB)")
            return s3_key

        for chunk_df in query_job.result().to_dataframe_iterable(bqstorage_client=bqstorage_client):
            if chunk_df.empty:
                continue

            # 날짜별 카운트 누적
            for date_val, cnt in chunk_df[hub_date_col].dropna().apply(
                lambda x: str(x.date()) if hasattr(x, "date") else str(x)[:10]
            ).value_counts().items():
                date_count_map[date_val] = date_count_map.get(date_val, 0) + int(cnt)

            # Delta 스키마 기반 타입 캐스팅 (nullable INT64 → float64 문제 방지)
            if delta_schema and col_rename_map is not None:
                chunk_df = _cast_to_delta_schema(chunk_df, delta_schema, col_rename_map)

            chunk_table = pa.Table.from_pandas(chunk_df)
            accumulated_tables.append(chunk_table)
            accumulated_bytes += chunk_table.nbytes

            if accumulated_bytes >= TARGET_BYTES:
                uploaded_files.append(_flush(accumulated_tables, part_idx))
                part_idx += 1
                accumulated_tables = []
                accumulated_bytes = 0

        # 남은 데이터 flush
        if accumulated_tables:
            uploaded_files.append(_flush(accumulated_tables, part_idx))
            part_idx += 1

        if not uploaded_files:
            raise AirflowException(f"[BQ→S3] 업로드된 파일 없음: {table_name}")

        logger.info(f"[BQ→S3] {table_name} 업로드 완료 ({part_idx}개 파트 파일)")
        return f"s3://{bucket}/{s3_prefix}", date_count_map

    finally:
        bq_client.delete_table(mat_table_id, not_found_ok=True)
        logger.info(f"[BQ→S3] 임시 물리 테이블 삭제 완료: {mat_table_id}")

def cleanup_s3_prefix(s3_client, bucket: str, prefix: str) -> None:
    """
    S3 prefix 하위 모든 오브젝트 삭제

    Args:
        s3_client: boto3 S3 Client
        bucket: S3 버킷명
        prefix: 삭제할 경로 prefix
    """
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
        # 정리 실패는 로그만 하고 예외를 발생시키지 않음 (ETL 정확성에는 영향 없음)
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

    # Dynamic Task Mapping용: [{"table_name": "A"}, {"table_name": "B"}, ...] 형태로 return
    return [{"table_name": t} for t in table_list]

# ===============================================================
# Task 3. BigQuery → Databricks ETL (Dynamic Task Mapping)
# ===============================================================

def etl_single_table(table_name: str, **context) -> None:
    """
    단일 테이블 ETL (BQ → S3 Parquet → Databricks COPY INTO).
    Dynamic Task Mapping에 의해 table_list 개수만큼 병렬로 자동 호출됨.

    Flow:
    1. 메타데이터 로드 및 유효성 검사
    2. 날짜 범위 수집
    3. BQ → S3 Parquet 업로드
    4. Databricks DELETE (날짜 범위)
    5. Databricks COPY INTO (S3 Parquet으로부터, 컬럼 리네임 포함)
    6. S3 cleanup (finally에서 항상 실행)
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
    catalog = db_config["catalog"]
    target_table = f"{catalog}.{schema}.{genie_table_name}"

    df_map = df_column[df_column["hub_table"] == table_name][["hub_column", "genie_column", "genie_column_desc"]]
    df_map = df_map[df_map["genie_column"].astype(str).str.len() > 0]
    col_rename_map = dict(zip(df_map["hub_column"], df_map["genie_column"]))
    genie_date_col = col_rename_map.get(hub_date_col, hub_date_col)

    # 클라이언트 생성
    bq_client = get_bq_client(bq_config)
    bqstorage_client = get_bq_storage_client(bq_config)
    conn = get_databricks_connection(db_config)
    cursor = conn.cursor()
    s3_client = get_s3_client(s3_config)

    # S3 경로 설정 (run_id 포함 → 실행마다 고유)
    table_short_name = table_name.split(".")[-1]
    dag_run_id = context["run_id"].replace(":", "_").replace("+", "_")
    s3_export_prefix = f"{s3_config['prefix']}{table_short_name}/{dag_run_id}/"
    s3_export_path = f"s3://{s3_config['bucket']}/{s3_export_prefix}"

    delete_executed = False
    try:
        logger.info(f"[ETL] {target_table} 시작 | 소스: {table_name}")

        # Step 1: Delta 스키마 조회 (타입 캐스팅용, 테이블 없으면 빈 dict)
        delta_schema = get_databricks_schema(cursor, target_table)
        if delta_schema:
            logger.info(f"[ETL] {target_table} | Delta 스키마 {len(delta_schema)}개 컬럼 로드 완료")

        # Step 2: BQ → S3 Parquet 업로드 (날짜별 row 수 동시 집계)
        _, date_count_map = upload_bq_to_s3_parquet(
            bq_client,
            bqstorage_client,
            table_name,
            s3_client,
            s3_config["bucket"],
            s3_export_prefix,
            hub_date_col=hub_date_col,
            delta_schema=delta_schema,
            col_rename_map=col_rename_map,
        )

        date_set = set(date_count_map.keys())

        if not date_set:
            raise AirflowException(f"[ETL] 기준 날짜 데이터 없음: {table_name}")

        bq_count_log = "\n".join([f"  {d}: {date_count_map[d]:,}건" for d in sorted(date_count_map)])
        logger.info(f"[ETL] {target_table} | BQ 날짜별 row수 (총 {sum(date_count_map.values()):,}건):\n{bq_count_log}")

        # Step 3: Databricks DELETE (날짜 범위)
        date_in_sql = ", ".join([f"DATE('{d}')" for d in sorted(date_set)])

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

        # Step 4: Databricks COPY INTO (S3 Parquet으로부터)
        # SELECT 절: hub_column AS genie_column 형태로 컬럼 리네임
        select_cols = []
        for hub_col in df_map["hub_column"]:
            genie_col = col_rename_map.get(hub_col, hub_col)
            if hub_col != genie_col:
                select_cols.append(f"`{hub_col}` AS `{genie_col}`")
            else:
                select_cols.append(f"`{hub_col}`")

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
        # Step 5: S3 cleanup (항상 실행)
        cleanup_s3_prefix(s3_client, s3_config["bucket"], s3_export_prefix)

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
    dag_id="notion_bq_to_db_dag",
    default_args=default_args,
    description="Notion 메타데이터 기반 BigQuery → Databricks ETL",
    schedule="50 22 * * *",  # KST 07:50 (UTC 22:50)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_tasks=10,    # 동시 실행 Task 최대 수 (테이블 병렬 처리 상한선)
    tags=["etl", "notion", "bigquery", "databricks"],
) as dag:

    collect_notion = PythonOperator(
        task_id="task_collect_notion",
        python_callable=task_collect_notion,
    )

    preprocess_metadata = PythonOperator(
        task_id="task_preprocess_metadata",
        python_callable=task_preprocess_metadata,
    )

    # Dynamic Task Mapping: table_list 개수만큼 병렬 ETL Task 자동 생성
    # table_list = ["table_A", "table_B", ...] → 각 테이블별 독립 Task로 병렬 실행
    # Dynamic Task Mapping: preprocess_metadata의 return값([{"table_name":...},...])으로 병렬 Task 자동 생성
    etl_tasks = PythonOperator.partial(
        task_id="task_etl_single_table",
        python_callable=etl_single_table,
    ).expand(op_kwargs=preprocess_metadata.output)

    tune_databricks = PythonOperator(
        task_id="task_tune_databricks",
        python_callable=task_tune_databricks,
    )

    collect_notion >> preprocess_metadata >> etl_tasks >> tune_databricks