"""
TikTok SNS 데이터 일별 수집 ETL DAG

스케줄: 매일 KST 17:00 (UTC 08:00)

수집 방식:
  - 좋아요/댓글/조회수/공유수: TikTok API가 현재 누적값만 제공 (일별 breakdown 미지원)
  - raw 테이블에 매일 누적값 스냅샷 저장 후 mart에서 전일 대비 차이 계산
  - Access Token 만료: 24시간 → 매 실행 시 자동 갱신
  - Refresh Token 만료: 365일 → 만료 7일 전 경보 후 수동 재인증 필요

사전 설정 필요한 Airflow Variables:
  - TIKTOK_Config : JSON 문자열. game_code 키마다 자격증명 보유.
      {
        "60009": {
          "client_key": "sbawgpkbf8npeircyd",
          "client_secret": "...",
          "redirect_uri": "https://github.com/joycityDSBI/tiktok_etl_app",
          "access_token": "...",
          "refresh_token": "...",
          "open_id": "...",
          "account_name": "freestylefootball_2",
          "expires_at": 1234567890,
          "refresh_token_expires_at": 1234567890
        }
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

from sns_tiktok_util import (
    refresh_access_token,
    get_user_info,
    get_video_list,
)

logger = logging.getLogger(__name__)

BQ_PROJECT = "datahub-478802"
BQ_LOCATION = "US"
BQ_DATASET = "external_data"
VIDEO_SNAPSHOT_TABLE = "tiktok_video_snapshot"
VIDEO_DAILY_TABLE = "tiktok_video_daily"


# ─── BigQuery helpers ─────────────────────────────────────────────────────────

def _bq_client() -> bigquery.Client:
    cred_info = json.loads(Variable.get("GOOGLE_CREDENTIAL_JSON"))
    credentials = service_account.Credentials.from_service_account_info(
        cred_info,
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )
    return bigquery.Client(credentials=credentials, project=BQ_PROJECT)


def _ensure_tables(client: bigquery.Client, project: str) -> None:
    video_snapshot_cols = """
        datekey             DATE      NOT NULL,
        account_id          STRING    NOT NULL,
        video_id            STRING    NOT NULL,
        video_title         STRING,
        views_cum           INT64,
        likes_cum           INT64,
        comments_cum        INT64,
        shares_cum          INT64,
        joyple_game_code    INT64,
        handle              STRING,
        created_at          TIMESTAMP
    """
    video_daily_cols = """
        datekey             DATE      NOT NULL,
        account_id          STRING    NOT NULL,
        video_id            STRING    NOT NULL,
        title               STRING,
        views               INT64,
        likes               INT64,
        comments            INT64,
        shares              INT64,
        joyple_game_code    INT64,
        source_api          STRING,
        handle              STRING
    """
    specs = [
        (VIDEO_SNAPSHOT_TABLE, video_snapshot_cols, "datekey"),
        (VIDEO_DAILY_TABLE, video_daily_cols, "datekey"),
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


def _delete_by_game(client: bigquery.Client, project: str, table: str, date_val: str, game_code: int) -> None:
    client.query(
        f"DELETE FROM `{project}.{BQ_DATASET}.{table}` "
        f"WHERE datekey = '{date_val}' AND joyple_game_code = {game_code}"
    ).result()


def _insert_rows(client: bigquery.Client, project: str, table: str, rows: list) -> None:
    if not rows:
        return
    table_ref = f"{project}.{BQ_DATASET}.{table}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=client.get_table(table_ref).schema,
    )
    job = client.load_table_from_json(rows, table_ref, job_config=job_config)
    job.result()
    if job.errors:
        raise RuntimeError(f"BQ 로드 오류 [{table}]: {job.errors}")


def _now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


# ─── Task 1: 토큰 갱신 ────────────────────────────────────────────────────────

def check_and_refresh_token(**context):
    """Access Token은 24시간 만료이므로 매 실행 시 갱신. Refresh Token 만료 7일 전 경보."""
    tiktok_config = json.loads(Variable.get("TIKTOK_Config"))
    updated = False
    now = time.time()

    for game_code, cred in tiktok_config.items():
        # Refresh token 만료 체크
        rt_expires_at = cred.get("refresh_token_expires_at", 0)
        rt_days_left = (rt_expires_at - now) / 86400
        logger.info("[%s] Refresh token 만료까지 %.1f일", game_code, rt_days_left)
        if rt_days_left <= 7:
            raise RuntimeError(
                f"[{game_code}] Refresh token 만료 임박 ({rt_days_left:.1f}일). "
                "수동 재인증 후 TIKTOK_Config Variable 업데이트 필요."
            )

        # Access token 갱신 (매 실행마다 - 24시간 만료)
        logger.info("[%s] Access token 갱신 시작", game_code)
        try:
            token_data = refresh_access_token(
                cred["refresh_token"],
                cred["client_key"],
                cred["client_secret"],
            )
            tiktok_config[game_code]["access_token"] = token_data["access_token"]
            tiktok_config[game_code]["expires_at"] = int(now + token_data.get("expires_in", 86400))
            if "refresh_token" in token_data:
                tiktok_config[game_code]["refresh_token"] = token_data["refresh_token"]
            if "refresh_expires_in" in token_data:
                tiktok_config[game_code]["refresh_token_expires_at"] = int(now + token_data["refresh_expires_in"])
            updated = True
            logger.info("[%s] Access token 갱신 완료", game_code)
        except Exception as exc:
            logger.error("[%s] Access token 갱신 실패: %s", game_code, exc)
            raise

    if updated:
        Variable.set("TIKTOK_Config", json.dumps(tiktok_config))
        logger.info("TIKTOK_Config 업데이트됨.")


# ─── Task 2: TikTok 데이터 수집 (누적값 raw 저장) ────────────────────────────

def collect_tiktok_raw(**context):
    collected_date = context["data_interval_end"].strftime("%Y-%m-%d")
    tiktok_config = json.loads(Variable.get("TIKTOK_Config"))

    client = _bq_client()
    project = BQ_PROJECT
    _ensure_tables(client, project)

    total_video_rows = 0

    for game_code, cred in tiktok_config.items():
        access_token = cred["access_token"]
        account_id = cred["open_id"]
        game_code_int = int(game_code)

        try:
            user_info = get_user_info(access_token)
            handle = user_info.get("display_name", cred.get("account_name", ""))
        except Exception as exc:
            logger.warning("[%s] 채널명 조회 실패, config 값 사용: %s", game_code, exc)
            handle = cred.get("account_name", "")

        # 영상 통계 수집
        try:
            videos = get_video_list(access_token)
        except Exception as exc:
            logger.error("[%s] 영상 목록 수집 실패: %s", game_code, exc)
            raise

        video_rows = []
        for video in videos:
            video_rows.append({
                "datekey": collected_date,
                "account_id": account_id,
                "video_id": video["id"],
                "video_title": (video.get("title") or "")[:500],
                "views_cum": int(video.get("view_count", 0) or 0),
                "likes_cum": int(video.get("like_count", 0) or 0),
                "comments_cum": int(video.get("comment_count", 0) or 0),
                "shares_cum": int(video.get("share_count", 0) or 0),
                "joyple_game_code": game_code_int,
                "handle": handle,
                "created_at": _now_utc_str(),
            })

        if not video_rows:
            logger.warning("[%s] 영상 0건 수집", game_code)
        else:
            _delete_by_game(client, project, VIDEO_SNAPSHOT_TABLE, collected_date, game_code_int)
            _insert_rows(client, project, VIDEO_SNAPSHOT_TABLE, video_rows)
            total_video_rows += len(video_rows)
            logger.info("[%s] 영상 snapshot 저장: %d건", game_code, len(video_rows))

    if total_video_rows == 0:
        raise RuntimeError("TikTok 영상 snapshot 수집 결과 0건. API 오류 로그를 확인하세요.")

    logger.info("TikTok 영상 snapshot 수집 완료: 총 %d건", total_video_rows)


# ─── Task 3: 전처리 (누적값 → 일별 증감량 계산 → mart 저장) ─────────────────

def process_mart(**context):
    collected_date = context["data_interval_end"].strftime("%Y-%m-%d")

    client = _bq_client()
    project = BQ_PROJECT
    ds = BQ_DATASET

    merge_sql = f"""
        MERGE `{project}.{ds}.{VIDEO_DAILY_TABLE}` T
        USING (
            WITH today AS (
                SELECT *
                FROM `{project}.{ds}.{VIDEO_SNAPSHOT_TABLE}`
                WHERE datekey = DATE('{collected_date}')
            ),
            yesterday AS (
                SELECT video_id,
                       joyple_game_code,
                       likes_cum,
                       comments_cum,
                       views_cum,
                       shares_cum
                FROM `{project}.{ds}.{VIDEO_SNAPSHOT_TABLE}`
                WHERE datekey = DATE_SUB(DATE('{collected_date}'), INTERVAL 1 DAY)
            )
            SELECT
                DATE_SUB(DATE('{collected_date}'), INTERVAL 1 DAY) AS datekey,
                t.account_id,
                t.video_id,
                t.video_title                                                       AS title,
                GREATEST(0, t.views_cum    - COALESCE(y.views_cum,    0))          AS views,
                GREATEST(0, t.likes_cum    - COALESCE(y.likes_cum,    0))          AS likes,
                GREATEST(0, t.comments_cum - COALESCE(y.comments_cum, 0))          AS comments,
                GREATEST(0, t.shares_cum   - COALESCE(y.shares_cum,   0))          AS shares,
                t.joyple_game_code,
                'tiktok_api'                                                        AS source_api,
                t.handle
            FROM today t
            LEFT JOIN yesterday y USING (video_id, joyple_game_code)
        ) S
        ON T.video_id = S.video_id AND T.joyple_game_code = S.joyple_game_code AND T.datekey = S.datekey
        WHEN MATCHED THEN UPDATE SET
            account_id = S.account_id,
            title      = S.title,
            views      = S.views,
            likes      = S.likes,
            comments   = S.comments,
            shares     = S.shares,
            source_api = S.source_api,
            handle     = S.handle
        WHEN NOT MATCHED THEN INSERT ROW
    """
    logger.info("TikTok mart MERGE 시작 (date=%s)", collected_date)
    client.query(merge_sql).result()
    logger.info("TikTok mart MERGE 완료")


# ─── DAG 정의 ─────────────────────────────────────────────────────────────────

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="sns_tiktok_etl",
    default_args=default_args,
    description="TikTok SNS 데이터 일별 수집 ETL",
    schedule="0 8 * * *",  # KST 17:00 = UTC 08:00
    start_date=pendulum.datetime(2025, 1, 1),
    catchup=False,
    tags=["sns", "tiktok", "etl"],
) as dag:

    t1 = PythonOperator(
        task_id="check_and_refresh_token",
        python_callable=check_and_refresh_token,
    )

    t2 = PythonOperator(
        task_id="collect_tiktok_raw",
        python_callable=collect_tiktok_raw,
    )

    t3 = PythonOperator(
        task_id="process_mart",
        python_callable=process_mart,
    )

    t1 >> t2 >> t3
