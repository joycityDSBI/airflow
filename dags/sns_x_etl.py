"""
X(Twitter) SNS 데이터 일별 수집 ETL DAG

스케줄: 매일 KST 17:00 (UTC 08:00)

수집 방식:
  - 좋아요/리트윗/댓글/인용/북마크/조회수: X API 가 누적값만 제공
  - raw 테이블에 매일 누적값 스냅샷 저장 후 mart 에서 전일 대비 차이 계산
  - 별도 백필 task 없음 — 매일 사용자 타임라인 끝까지 페이지네이션하면
    모든 활성 트윗(최대 ~3,200)의 baseline 이 자연스럽게 갱신됨

사전 설정 필요한 Airflow Variables:
  - X_Config : JSON 문자열. 계정 username 키마다 자격증명 보유.
      {
        "FSF2_Official": {
          "consumer_key": "...",
          "consumer_secret": "...",
          "access_token": "...",
          "access_token_secret": "...",
          "joyple_game_code": 60011
        }
      }
  - GOOGLE_CREDENTIAL_JSON : datahub-478802 프로젝트 접근용
"""

import json
import logging
from datetime import datetime, timedelta, timezone

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from google.oauth2 import service_account

from sns_x_util import (
    build_client,
    extract_metrics,
    get_user_id,
    iter_user_tweets,
)

logger = logging.getLogger(__name__)

BQ_PROJECT = "datahub-478802"
BQ_LOCATION = "US"
BQ_DATASET = "external_data"
SNAPSHOT_TABLE = "x_post_snapshot"
MART_TABLE = "x_post_daily"
PLATFORM = "x"


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
        platform           STRING,
        joyple_game_code   INT64,
        account_username   STRING,
        tweet_id           STRING,
        tweet_created_at   TIMESTAMP,
        text               STRING,
        datekey            DATE,
        like_count         INT64,
        retweet_count      INT64,
        reply_count        INT64,
        quote_count        INT64,
        bookmark_count     INT64,
        impression_count   INT64,
        profile_clicks     INT64,
        url_link_clicks    INT64,
        collected_at       TIMESTAMP
    """
    mart_cols = """
        platform           STRING,
        joyple_game_code   INT64,
        account_username   STRING,
        tweet_id           STRING,
        tweet_created_at   TIMESTAMP,
        text               STRING,
        datekey            DATE,
        like_daily         INT64,
        retweet_daily      INT64,
        reply_daily        INT64,
        quote_daily        INT64,
        bookmark_daily     INT64,
        impression_daily   INT64,
        profile_clicks_daily   INT64,
        url_link_clicks_daily  INT64
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


def _delete_partition(client: bigquery.Client, project: str, table: str, col: str, date_val: str) -> None:
    client.query(
        f"DELETE FROM `{project}.{BQ_DATASET}.{table}` "
        f"WHERE {col} = '{date_val}' AND platform = '{PLATFORM}'"
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


# ─── Task 1: 계정 목록 조회 ─────────────────────────────────────────────────

def fetch_account_list(**context):
    ti = context["ti"]
    x_config = json.loads(Variable.get("X_Config"))

    accounts = []
    for username, cred in x_config.items():
        accounts.append({
            "username": username,
            "joyple_game_code": int(cred["joyple_game_code"]),
            "creds": {
                "consumer_key": cred["consumer_key"],
                "consumer_secret": cred["consumer_secret"],
                "access_token": cred["access_token"],
                "access_token_secret": cred["access_token_secret"],
            },
        })
        logger.info("X 계정 등록: %s (game_code=%s)", username, cred["joyple_game_code"])

    ti.xcom_push(key="accounts", value=json.dumps(accounts))
    logger.info("총 X 계정 수: %d", len(accounts))


# ─── Task 2: X 트윗 수집 (누적값 raw 저장) ─────────────────────────────────

def collect_x_raw(**context):
    ti = context["ti"]
    collected_date = context["data_interval_end"].strftime("%Y-%m-%d")

    accounts = json.loads(ti.xcom_pull(task_ids="fetch_account_list", key="accounts"))

    client = _bq_client()
    project = BQ_PROJECT
    _ensure_tables(client, project)

    rows = []

    for account in accounts:
        username = account["username"]
        try:
            x_client = build_client(account["creds"])
            user_id = get_user_id(x_client, username)
            logger.info("[%s] user_id=%s", username, user_id)
        except Exception as exc:
            logger.error("[%s] 계정 초기화 실패: %s", username, exc)
            continue

        tweet_count = 0
        for tweet in iter_user_tweets(x_client, user_id):
            try:
                metrics = extract_metrics(tweet)
                rows.append({
                    "platform": PLATFORM,
                    "joyple_game_code": account["joyple_game_code"],
                    "account_username": username,
                    "tweet_id": tweet["id"],
                    "tweet_created_at": tweet["created_at"].strftime("%Y-%m-%d %H:%M:%S")
                                        if tweet.get("created_at") else None,
                    "text": (tweet.get("text") or "")[:500],
                    "datekey": collected_date,
                    "like_count": metrics["like_count"],
                    "retweet_count": metrics["retweet_count"],
                    "reply_count": metrics["reply_count"],
                    "quote_count": metrics["quote_count"],
                    "bookmark_count": metrics["bookmark_count"],
                    "impression_count": metrics["impression_count"],
                    "profile_clicks": metrics["profile_clicks"],
                    "url_link_clicks": metrics["url_link_clicks"],
                    "collected_at": _now_utc_str(),
                })
                tweet_count += 1
            except Exception as exc:
                logger.error("[%s] 트윗 처리 실패 tweet_id=%s: %s", username, tweet.get("id"), exc)

        logger.info("[%s] 수집 완료: %d건", username, tweet_count)

    if not rows:
        raise RuntimeError("X snapshot 수집 결과 0건. API 오류 로그를 확인하세요.")

    _delete_partition(client, project, SNAPSHOT_TABLE, "datekey", collected_date)
    _insert_rows(client, project, SNAPSHOT_TABLE, rows)

    logger.info("X snapshot 수집 완료: %d건 (date=%s)", len(rows), collected_date)


# ─── Task 3: 전처리 (누적값 → 일별 증감량 계산 → mart 저장) ─────────────────

def process_mart(**context):
    collected_date = context["data_interval_end"].strftime("%Y-%m-%d")

    client = _bq_client()
    project = BQ_PROJECT
    ds = BQ_DATASET

    # 일별 mart 의 datekey 는 "어제 ~ 오늘 사이의 증감"이므로 (오늘 - 1) 일자로 기록.
    # Meta DAG 와 동일한 컨벤션.
    merge_sql = f"""
        MERGE `{project}.{ds}.{MART_TABLE}` T
        USING (
            WITH today AS (
                SELECT *
                FROM `{project}.{ds}.{SNAPSHOT_TABLE}`
                WHERE datekey = DATE('{collected_date}')
                  AND platform = '{PLATFORM}'
            ),
            yesterday AS (
                SELECT tweet_id,
                       like_count,
                       retweet_count,
                       reply_count,
                       quote_count,
                       bookmark_count,
                       impression_count,
                       profile_clicks,
                       url_link_clicks
                FROM `{project}.{ds}.{SNAPSHOT_TABLE}`
                WHERE datekey = DATE_SUB(DATE('{collected_date}'), INTERVAL 1 DAY)
                  AND platform = '{PLATFORM}'
            )
            SELECT
                t.platform,
                t.joyple_game_code,
                t.account_username,
                t.tweet_id,
                t.tweet_created_at,
                t.text,
                DATE_SUB(DATE('{collected_date}'), INTERVAL 1 DAY) AS datekey,
                GREATEST(0, t.like_count        - COALESCE(y.like_count,        0)) AS like_daily,
                GREATEST(0, t.retweet_count     - COALESCE(y.retweet_count,     0)) AS retweet_daily,
                GREATEST(0, t.reply_count       - COALESCE(y.reply_count,       0)) AS reply_daily,
                GREATEST(0, t.quote_count       - COALESCE(y.quote_count,       0)) AS quote_daily,
                GREATEST(0, t.bookmark_count    - COALESCE(y.bookmark_count,    0)) AS bookmark_daily,
                GREATEST(0, t.impression_count  - COALESCE(y.impression_count,  0)) AS impression_daily,
                CASE WHEN t.profile_clicks IS NULL THEN NULL
                     ELSE GREATEST(0, t.profile_clicks - COALESCE(y.profile_clicks, 0)) END AS profile_clicks_daily,
                CASE WHEN t.url_link_clicks IS NULL THEN NULL
                     ELSE GREATEST(0, t.url_link_clicks - COALESCE(y.url_link_clicks, 0)) END AS url_link_clicks_daily
            FROM today t
            LEFT JOIN yesterday y USING (tweet_id)
        ) S
        ON T.tweet_id = S.tweet_id AND T.datekey = S.datekey AND T.platform = S.platform
        WHEN MATCHED THEN UPDATE SET
            joyple_game_code      = S.joyple_game_code,
            account_username      = S.account_username,
            tweet_created_at      = S.tweet_created_at,
            text                  = S.text,
            like_daily            = S.like_daily,
            retweet_daily         = S.retweet_daily,
            reply_daily           = S.reply_daily,
            quote_daily           = S.quote_daily,
            bookmark_daily        = S.bookmark_daily,
            impression_daily      = S.impression_daily,
            profile_clicks_daily  = S.profile_clicks_daily,
            url_link_clicks_daily = S.url_link_clicks_daily
        WHEN NOT MATCHED THEN INSERT ROW
    """
    logger.info("X mart MERGE 시작 (date=%s)", collected_date)
    client.query(merge_sql).result()
    logger.info("X mart MERGE 완료")


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
    dag_id="sns_x_etl",
    default_args=default_args,
    description="X(Twitter) SNS 데이터 일별 수집 ETL",
    schedule="0 8 * * *",  # KST 17:00 = UTC 08:00
    start_date=pendulum.datetime(2026, 5, 26),
    catchup=False,
    tags=["sns", "x", "twitter", "etl"],
) as dag:

    t1 = PythonOperator(
        task_id="fetch_account_list",
        python_callable=fetch_account_list,
    )

    t2 = PythonOperator(
        task_id="collect_x_raw",
        python_callable=collect_x_raw,
    )

    t3 = PythonOperator(
        task_id="process_mart",
        python_callable=process_mart,
    )

    t1 >> t2 >> t3
