"""
X(Twitter) API 호출 유틸리티 (sns_meta_util 와 같은 역할).

tweepy v2 Client(OAuth 1.0a User Context) 기반.
non_public_metrics 까지 가져오기 위해 모든 호출에 user_auth=True 명시.
"""

import logging
import time
from typing import Iterator, Optional

import tweepy

logger = logging.getLogger(__name__)

# get_users_tweets 페이지당 최대치 (X API v2 제약)
PAGE_SIZE = 100
# 사용자 타임라인 endpoint 자체 한도가 약 3,200건이므로 32 페이지에서 안전 컷
MAX_PAGES = 32
# 페이지 사이 지연 (rate limit + pay-per-use 부담 완화)
PAGE_SLEEP_SEC = 0.1

# 가져올 tweet 필드 (non_public_metrics, organic_metrics 는 OAuth1 본인 계정 인증 필요)
TWEET_FIELDS = [
    "id",
    "text",
    "created_at",
    "public_metrics",
    "non_public_metrics",
    "organic_metrics",
]


def build_client(creds: dict) -> tweepy.Client:
    """X_Config 의 한 계정 분량 dict 로부터 tweepy.Client 생성."""
    return tweepy.Client(
        consumer_key=creds["consumer_key"],
        consumer_secret=creds["consumer_secret"],
        access_token=creds["access_token"],
        access_token_secret=creds["access_token_secret"],
        wait_on_rate_limit=True,
    )


def get_user_id(client: tweepy.Client, username: str) -> str:
    """username → user_id 변환. 실패 시 RuntimeError."""
    try:
        resp = client.get_user(username=username, user_auth=True)
    except tweepy.TweepyException as exc:
        # 4xx 응답 본문 로깅 (CLAUDE.md 규칙)
        logger.error("get_user 실패 username=%s: %s", username, exc)
        raise

    if not resp or not resp.data:
        errors = getattr(resp, "errors", None)
        raise RuntimeError(f"username={username} 조회 결과 없음. errors={errors}")

    return str(resp.data.id)


def iter_user_tweets(client: tweepy.Client, user_id: str) -> Iterator[dict]:
    """
    사용자 타임라인을 페이지네이션하며 트윗 dict 를 yield.

    각 dict 는 {id, text, created_at, public_metrics, non_public_metrics, organic_metrics}
    구조. 일부 필드는 X 정책상 NULL/누락 가능 (예: 30일 지난 트윗의 non_public_metrics).
    """
    pagination_token: Optional[str] = None
    page_count = 0

    while page_count < MAX_PAGES:
        page_count += 1
        try:
            resp = client.get_users_tweets(
                id=user_id,
                max_results=PAGE_SIZE,
                tweet_fields=TWEET_FIELDS,
                pagination_token=pagination_token,
                user_auth=True,
            )
        except tweepy.TweepyException as exc:
            logger.error(
                "get_users_tweets 실패 user_id=%s page=%d: %s",
                user_id, page_count, exc,
            )
            raise

        tweets = resp.data or []
        logger.info(
            "user_id=%s page=%d tweets=%d", user_id, page_count, len(tweets),
        )

        for tweet in tweets:
            # tweepy 응답을 dict 로 정규화 (data 객체 그대로면 직렬화 까다로움)
            yield {
                "id": str(tweet.id),
                "text": tweet.text or "",
                "created_at": tweet.created_at,  # tz-aware UTC datetime
                "public_metrics": tweet.public_metrics or {},
                "non_public_metrics": getattr(tweet, "non_public_metrics", None) or {},
                "organic_metrics": getattr(tweet, "organic_metrics", None) or {},
            }

        meta = resp.meta or {}
        pagination_token = meta.get("next_token")
        if not pagination_token:
            break

        time.sleep(PAGE_SLEEP_SEC)

    if page_count >= MAX_PAGES and pagination_token:
        logger.warning(
            "user_id=%s MAX_PAGES(%d) 도달 — 더 오래된 트윗 일부 미수집 가능",
            user_id, MAX_PAGES,
        )


def extract_metrics(tweet: dict) -> dict:
    """
    tweepy 의 public_metrics + non_public_metrics + organic_metrics 를
    BigQuery 컬럼에 맞게 평탄화.

    impression_count: organic_metrics(있으면 정확) → public_metrics(폴백) 순서.
    profile_clicks / url_link_clicks: organic_metrics → non_public_metrics 순서.
    조회 불가 / 30일 경과 트윗은 None.
    """
    public = tweet.get("public_metrics") or {}
    non_public = tweet.get("non_public_metrics") or {}
    organic = tweet.get("organic_metrics") or {}

    def pick(*sources_and_keys, default=None):
        for src, key in sources_and_keys:
            if src and src.get(key) is not None:
                return src[key]
        return default

    return {
        "like_count": int(public.get("like_count", 0) or 0),
        "retweet_count": int(public.get("retweet_count", 0) or 0),
        "reply_count": int(public.get("reply_count", 0) or 0),
        "quote_count": int(public.get("quote_count", 0) or 0),
        "bookmark_count": int(public.get("bookmark_count", 0) or 0),
        "impression_count": pick(
            (organic, "impression_count"),
            (non_public, "impression_count"),
            (public, "impression_count"),
            default=0,
        ),
        "profile_clicks": pick(
            (organic, "user_profile_clicks"),
            (non_public, "user_profile_clicks"),
            default=None,
        ),
        "url_link_clicks": pick(
            (organic, "url_link_clicks"),
            (non_public, "url_link_clicks"),
            default=None,
        ),
    }
