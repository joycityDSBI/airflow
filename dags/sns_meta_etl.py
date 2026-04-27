"""
Meta (Instagram + Facebook) SNS 데이터 일별 수집 ETL DAG

스케줄: 매일 KST 17:00 (UTC 08:00)

수집 방식:
  - 좋아요/댓글/조회수: Meta API가 현재 누적값만 제공 (일별 breakdown 미지원)
  - raw 테이블에 매일 누적값 스냅샷 저장 후 mart에서 전일 대비 차이 계산

사전 설정 필요한 Airflow Variables:
  - META_Config : JSON 문자열. game_code 키마다 자격증명 보유.
      {
        "60009": {"app_id": "...", "app_secret": "...", "access_token": "..."},
        "60010": {"app_id": "...", "app_secret": "...", "access_token": "..."}
      }
  - GOOGLE_CREDENTIAL_JSON
"""

import json
import logging
import time
from datetime import datetime, timedelta, timezone

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from google.oauth2 import service_account

from sns_meta_util import (
    check_token,
    extend_token,
    get_fb_pages,
    get_ig_account_id,
    get_ig_media_list,
    get_ig_impressions,
    get_fb_posts,
    get_fb_post_impressions,
)

logger = logging.getLogger(__name__)

BQ_PROJECT = "datahub-478802"
BQ_LOCATION = "US"
BQ_DATASET = "external_data"
SNAPSHOT_TABLE = "meta_post_snapshot"
MART_TABLE = "meta_post_daily"


# ─── BigQuery helpers ────────────────────────────────────────────────────────

def _bq_client() -> bigquery.Client:
    cred_info = json.loads(Variable.get("GOOGLE_CREDENTIAL_JSON"))
    credentials = service_account.Credentials.from_service_account_info(
        cred_info,
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )
    return bigquery.Client(
        credentials=credentials,
        project=BQ_PROJECT,
    )


def _ensure_tables(client: bigquery.Client, project: str) -> None:
    snapshot_cols = """
        platform            STRING,
        joyple_game_code    STRING,
        account_id          STRING,
        account_name        STRING,
        post_id             STRING,
        post_name           STRING,
        datekey             DATE,
        likes_cumulative    INT64,
        comments_cumulative INT64,
        views_cumulative    INT64,
        created_at          TIMESTAMP
    """
    mart_cols = """
        platform         STRING,
        joyple_game_code STRING,
        account_id       STRING,
        account_name     STRING,
        post_id          STRING,
        post_name        STRING,
        datekey          DATE,
        likes_daily      INT64,
        comments_daily   INT64,
        views_daily      INT64
    """
    specs = [
        (SNAPSHOT_TABLE, snapshot_cols, "datekey"),
        (MART_TABLE, mart_cols, "datekey"),
    ]
    for table, cols, partition_col in specs:
        ddl = f"""
            CREATE TABLE IF NOT EXISTS `{project}.{BQ_DATASET}.{table}` (
                {cols}
            )
            PARTITION BY {partition_col}
            OPTIONS (require_partition_filter = FALSE)
        """
        client.query(ddl).result()
        logger.info("Table ready: %s.%s.%s", project, BQ_DATASET, table)


def _delete_partition(client: bigquery.Client, project: str, table: str, col: str, date_val: str, platform: str = None) -> None:
    where = f"{col} = '{date_val}'"
    if platform:
        where += f" AND platform = '{platform}'"
    client.query(f"DELETE FROM `{project}.{BQ_DATASET}.{table}` WHERE {where}").result()


def _insert_rows(client: bigquery.Client, project: str, table: str, rows: list) -> None:
    if not rows:
        return
    table_ref = client.get_table(f"{project}.{BQ_DATASET}.{table}")
    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        raise RuntimeError(f"BQ 스트리밍 삽입 오류 [{table}]: {errors}")


def _now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


# ─── Task 1: 토큰 유효성 체크 및 갱신 ────────────────────────────────────────

def check_and_refresh_token(**context):
    meta_config = json.loads(Variable.get("META_Config"))
    updated = False

    for game_code, cred in meta_config.items():
        app_id = cred["app_id"]
        app_secret = cred["app_secret"]
        access_token = cred["access_token"]

        token_info = check_token(access_token, app_id, app_secret)
        logger.info("[%s] 토큰 정보: %s", game_code, token_info)

        if not token_info.get("is_valid", False):
            raise RuntimeError(f"[{game_code}] access_token 이 유효하지 않습니다. 수동 갱신 필요.")

        expires_at = token_info.get("expires_at", 0)
        if not expires_at:
            logger.info("[%s] 만료 기한 없는 토큰 (페이지 토큰 등).", game_code)
            continue

        days_left = (expires_at - time.time()) / 86400
        logger.info("[%s] 토큰 만료까지 %.1f일 남음", game_code, days_left)

        if days_left <= 7:
            logger.info("[%s] D-%d 이내 → 자동 갱신 시작", game_code, int(days_left))
            try:
                new_token = extend_token(access_token, app_id, app_secret)
                meta_config[game_code]["access_token"] = new_token
                updated = True
                logger.info("[%s] 토큰 갱신 완료.", game_code)
            except Exception as exc:
                logger.error("[%s] 토큰 갱신 실패: %s", game_code, exc)
                raise

    if updated:
        Variable.set("META_Config", json.dumps(meta_config))
        logger.info("META_Config 업데이트됨.")


# ─── Task 2: 계정/페이지 목록 조회 ──────────────────────────────────────────

def fetch_account_list(**context):
    ti = context["ti"]
    meta_config = json.loads(Variable.get("META_Config"))
    accounts = []

    for game_code, cred in meta_config.items():
        access_token = cred["access_token"]
        pages = get_fb_pages(access_token)

        for page in pages:
            page_id = page["id"]
            page_token = page.get("access_token", access_token)
            ig_user_id = get_ig_account_id(page_id, page_token)

            accounts.append({
                "fb_page_id": page_id,
                "fb_page_name": page["name"],
                "fb_page_token": page_token,
                "ig_user_id": ig_user_id,
                "game_code": game_code,
            })
            logger.info(
                "[%s] 페이지 확인: %s (id=%s, IG=%s)",
                game_code, page["name"], page_id, ig_user_id,
            )

    ti.xcom_push(key="accounts", value=json.dumps(accounts))
    logger.info("총 FB 페이지 수: %d", len(accounts))


# ─── Task 3: Instagram 데이터 수집 (누적값 raw 저장) ─────────────────────────

def collect_instagram_raw(**context):
    ti = context["ti"]
    collected_date = context["data_interval_end"].strftime("%Y-%m-%d")  # PST 기준 수집일

    accounts = json.loads(ti.xcom_pull(task_ids="fetch_account_list", key="accounts"))

    client = _bq_client()
    project = BQ_PROJECT
    _ensure_tables(client, project)

    rows = []

    for account in accounts:
        ig_user_id = account.get("ig_user_id")
        if not ig_user_id:
            logger.info("페이지 %s 에 연결된 IG 계정 없음. 스킵.", account["fb_page_name"])
            continue

        try:
            media_list = get_ig_media_list(ig_user_id, account["fb_page_token"])
        except Exception as exc:
            logger.error("IG 미디어 목록 조회 실패 ig_user_id=%s: %s", ig_user_id, exc)
            continue

        for media in media_list:
            try:
                media_type = media.get("media_type", "")
                video_views = media.get("video_views")

                # VIDEO/REELS: video_views 우선, 없으면 impressions 폴백
                if media_type == "VIDEO" and video_views:
                    views = int(video_views)
                else:
                    views = get_ig_impressions(media["id"], account["fb_page_token"])

                rows.append({
                    "platform": "instagram",
                    "joyple_game_code": account["game_code"],
                    "account_id": ig_user_id,
                    "account_name": account["fb_page_name"],
                    "post_id": media["id"],
                    "post_name": (media.get("caption") or "")[:500],
                    "datekey": collected_date,
                    "likes_cumulative": int(media.get("like_count") or 0),
                    "comments_cumulative": int(media.get("comments_count") or 0),
                    "views_cumulative": views,
                    "created_at": _now_utc_str(),
                })
            except Exception as exc:
                logger.error("IG 미디어 처리 실패 media_id=%s: %s", media.get("id"), exc)
            time.sleep(0.05)

    _delete_partition(client, project, SNAPSHOT_TABLE, "datekey", collected_date, platform="instagram")
    _insert_rows(client, project, SNAPSHOT_TABLE, rows)

    logger.info("Instagram snapshot 수집 완료: %d건", len(rows))


# ─── Task 4: Facebook 데이터 수집 (누적값 raw 저장) ──────────────────────────

def collect_facebook_raw(**context):
    ti = context["ti"]
    collected_date = context["data_interval_end"].strftime("%Y-%m-%d")  # PST 기준 수집일

    accounts = json.loads(ti.xcom_pull(task_ids="fetch_account_list", key="accounts"))

    client = _bq_client()
    project = BQ_PROJECT

    rows = []

    for account in accounts:
        page_id = account["fb_page_id"]
        page_token = account["fb_page_token"]

        try:
            posts = get_fb_posts(page_id, page_token)
        except Exception as exc:
            logger.error("FB 게시물 조회 실패 page_id=%s: %s", page_id, exc)
            continue

        for post in posts:
            try:
                likes_data = (post.get("likes") or {}).get("summary", {})
                comments_data = (post.get("comments") or {}).get("summary", {})
                likes = int(likes_data.get("total_count") or 0)
                comments = int(comments_data.get("total_count") or 0)
                views = get_fb_post_impressions(post["id"], page_token)

                rows.append({
                    "platform": "facebook",
                    "joyple_game_code": account["game_code"],
                    "account_id": page_id,
                    "account_name": account["fb_page_name"],
                    "post_id": post["id"],
                    "post_name": (post.get("message") or "")[:500],
                    "datekey": collected_date,
                    "likes_cumulative": likes,
                    "comments_cumulative": comments,
                    "views_cumulative": views,
                    "created_at": _now_utc_str(),
                })
            except Exception as exc:
                logger.error("FB 게시물 처리 실패 post_id=%s: %s", post.get("id"), exc)
            time.sleep(0.1)  # Facebook API rate limit 여유

    _delete_partition(client, project, SNAPSHOT_TABLE, "datekey", collected_date, platform="facebook")
    _insert_rows(client, project, SNAPSHOT_TABLE, rows)

    logger.info("Facebook snapshot 수집 완료: %d건", len(rows))


# ─── Task 5: 전처리 (누적값 → 일별 증감량 계산 → mart 저장) ─────────────────

def process_mart(**context):
    collected_date = context["data_interval_end"].strftime("%Y-%m-%d")  # PST 기준 수집일

    client = _bq_client()
    project = BQ_PROJECT
    ds = BQ_DATASET

    for platform, label in [("instagram", "Instagram"), ("facebook", "Facebook")]:
        merge_sql = f"""
            MERGE `{project}.{ds}.{MART_TABLE}` T
            USING (
                WITH today AS (
                    SELECT *
                    FROM `{project}.{ds}.{SNAPSHOT_TABLE}`
                    WHERE datekey = DATE('{collected_date}')
                      AND platform = '{platform}'
                ),
                yesterday AS (
                    SELECT post_id,
                           likes_cumulative,
                           comments_cumulative,
                           views_cumulative
                    FROM `{project}.{ds}.{SNAPSHOT_TABLE}`
                    WHERE datekey = DATE_SUB(DATE('{collected_date}'), INTERVAL 1 DAY)
                      AND platform = '{platform}'
                )
                SELECT
                    t.platform,
                    t.joyple_game_code,
                    t.account_id,
                    t.account_name,
                    t.post_id,
                    t.post_name,
                    DATE_SUB(DATE('{collected_date}'), INTERVAL 1 DAY) AS datekey,
                    GREATEST(0, t.likes_cumulative    - COALESCE(y.likes_cumulative,    0)) AS likes_daily,
                    GREATEST(0, t.comments_cumulative - COALESCE(y.comments_cumulative, 0)) AS comments_daily,
                    GREATEST(0, t.views_cumulative    - COALESCE(y.views_cumulative,    0)) AS views_daily
                FROM today t
                LEFT JOIN yesterday y USING (post_id)
            ) S
            ON T.post_id = S.post_id AND T.datekey = S.datekey AND T.platform = S.platform
            WHEN MATCHED THEN UPDATE SET
                joyple_game_code = S.joyple_game_code,
                account_id       = S.account_id,
                account_name     = S.account_name,
                post_name        = S.post_name,
                likes_daily      = S.likes_daily,
                comments_daily   = S.comments_daily,
                views_daily      = S.views_daily
            WHEN NOT MATCHED THEN INSERT ROW
        """
        logger.info("%s mart MERGE 시작 (date=%s)", label, collected_date)
        client.query(merge_sql).result()
        logger.info("%s mart MERGE 완료", label)


# ─── DAG 정의 ────────────────────────────────────────────────────────────────

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="sns_meta_etl",
    default_args=default_args,
    description="Meta (Instagram + Facebook) SNS 데이터 일별 수집 ETL",
    schedule="0 8 * * *",  # KST 17:00 = UTC 08:00
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["sns", "meta", "etl"],
) as dag:

    t1 = PythonOperator(
        task_id="check_and_refresh_token",
        python_callable=check_and_refresh_token,
    )

    t2 = PythonOperator(
        task_id="fetch_account_list",
        python_callable=fetch_account_list,
    )

    t3 = PythonOperator(
        task_id="collect_instagram_raw",
        python_callable=collect_instagram_raw,
    )

    t4 = PythonOperator(
        task_id="collect_facebook_raw",
        python_callable=collect_facebook_raw,
    )

    t5 = PythonOperator(
        task_id="process_mart",
        python_callable=process_mart,
    )

    t1 >> t2 >> [t3, t4] >> t5
