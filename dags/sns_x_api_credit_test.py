"""
X(Twitter) API 크레딧 소모량 측정용 테스트 DAG

목적:
  - X API 1회 호출 시 크레딧이 얼마나 차감되는지 확인
  - get_user 1회 + get_users_tweets 1페이지(=1회) 만 호출하고 종료
  - BigQuery 적재 없음 (순수 API 호출만)

사용법:
  1. X 개발자 대시보드에서 현재 크레딧 잔량 확인
  2. Airflow UI 에서 이 DAG 를 manual trigger
  3. 다시 X 개발자 대시보드에서 잔량 확인 → 차감량 비교

대상 계정: X_Config Variable 의 첫 번째 계정
스케줄: 없음 (manual trigger only)
"""

import json
import logging
import time
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from sns_x_util import (
    PAGE_SIZE,
    TWEET_FIELDS,
    build_client,
)

logger = logging.getLogger(__name__)


def run_single_api_calls(**context):
    x_config = json.loads(Variable.get("X_Config"))
    if not x_config:
        raise RuntimeError("X_Config 가 비어 있습니다.")

    username, cred = next(iter(x_config.items()))
    logger.info("=" * 60)
    logger.info("테스트 대상 계정: %s (game_code=%s)", username, cred.get("joyple_game_code"))
    logger.info("=" * 60)

    client = build_client({
        "consumer_key": cred["consumer_key"],
        "consumer_secret": cred["consumer_secret"],
        "access_token": cred["access_token"],
        "access_token_secret": cred["access_token_secret"],
    })

    # ─── API 호출 1: get_user (username → user_id) ────────────────────
    logger.info(">>> [1/2] get_user 호출 시작 (username=%s)", username)
    t0 = time.monotonic()
    user_resp = client.get_user(username=username, user_auth=True)
    elapsed_user = time.monotonic() - t0

    if not user_resp or not user_resp.data:
        errors = getattr(user_resp, "errors", None)
        raise RuntimeError(f"get_user 응답 없음. errors={errors}")

    user_id = str(user_resp.data.id)
    logger.info(">>> [1/2] get_user 완료: user_id=%s, elapsed=%.3fs", user_id, elapsed_user)
    logger.info("    [응답] data    = %s", user_resp.data.data if hasattr(user_resp.data, "data") else user_resp.data)
    logger.info("    [응답] includes= %s", getattr(user_resp, "includes", None))
    logger.info("    [응답] errors  = %s", getattr(user_resp, "errors", None))
    logger.info("    [응답] meta    = %s", getattr(user_resp, "meta", None))

    # ─── API 호출 2: get_users_tweets 1페이지 (max 100건) ─────────────
    logger.info(">>> [2/2] get_users_tweets 호출 시작 (user_id=%s, max_results=%d)",
                user_id, PAGE_SIZE)
    t1 = time.monotonic()
    tweets_resp = client.get_users_tweets(
        id=user_id,
        max_results=PAGE_SIZE,
        tweet_fields=TWEET_FIELDS,
        pagination_token=None,
        user_auth=True,
    )
    elapsed_tweets = time.monotonic() - t1

    tweets = tweets_resp.data or []
    meta = tweets_resp.meta or {}
    logger.info(">>> [2/2] get_users_tweets 완료: tweets=%d, elapsed=%.3fs",
                len(tweets), elapsed_tweets)
    logger.info("    [응답] meta    = %s", meta)
    logger.info("    [응답] includes= %s", getattr(tweets_resp, "includes", None))
    logger.info("    [응답] errors  = %s", getattr(tweets_resp, "errors", None))

    # 트윗별 전체 필드 덤프 (id, text, created_at, public/non_public/organic_metrics)
    for idx, tweet in enumerate(tweets, start=1):
        logger.info(
            "    [트윗 %d/%d] id=%s, created_at=%s",
            idx, len(tweets), tweet.id, tweet.created_at,
        )
        logger.info("        text              = %s", (tweet.text or "")[:200])
        logger.info("        public_metrics    = %s", tweet.public_metrics)
        logger.info("        non_public_metrics= %s", getattr(tweet, "non_public_metrics", None))
        logger.info("        organic_metrics   = %s", getattr(tweet, "organic_metrics", None))

    # ─── 요약 ──────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("테스트 완료 — 총 API 호출 횟수: 2회")
    logger.info("  1) get_user            : %.3fs", elapsed_user)
    logger.info("  2) get_users_tweets x1 : %.3fs (tweets=%d)", elapsed_tweets, len(tweets))
    logger.info("X 개발자 대시보드에서 크레딧 차감량을 확인하세요.")
    logger.info("=" * 60)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,  # 테스트용이라 재시도로 인한 추가 호출 방지
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="sns_x_api_credit_test",
    default_args=default_args,
    description="X API 크레딧 소모량 측정용 1회 호출 테스트",
    schedule=None,  # manual trigger only
    start_date=pendulum.datetime(2026, 6, 10),
    catchup=False,
    tags=["sns", "x", "twitter", "test", "manual"],
) as dag:

    PythonOperator(
        task_id="run_single_api_calls",
        python_callable=run_single_api_calls,
    )
