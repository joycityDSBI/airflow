import time
import logging
import requests
from typing import Optional

logger = logging.getLogger(__name__)

GRAPH_API_BASE = "https://graph.facebook.com/v25.0"


def _api_get(url: str, params: dict, max_retries: int = 3) -> dict:
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code == 429:
                wait_sec = (2 ** attempt) * 5
                logger.warning("Rate limit hit. Sleeping %ds (attempt %d)", wait_sec, attempt + 1)
                time.sleep(wait_sec)
                continue
            if resp.status_code == 400:
                logger.error("400 응답 본문: %s", resp.text)
                resp.raise_for_status()  # 400은 재시도 없이 즉시 raise
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as exc:
            if attempt == max_retries - 1:
                raise
            if exc.response is not None and exc.response.status_code == 400:
                raise  # 400은 재시도 불필요
            wait_sec = (2 ** attempt) * 2
            logger.warning("HTTP error (attempt %d/%d): %s. Retrying in %ds", attempt + 1, max_retries, exc, wait_sec)
            time.sleep(wait_sec)
    raise RuntimeError(f"Max retries exceeded: {url}")


def _paginate(url: str, params: dict) -> list:
    results = []
    next_url = url
    next_params = params
    while next_url:
        data = _api_get(next_url, next_params)
        results.extend(data.get("data", []))
        paging = data.get("paging", {})
        next_url = paging.get("next")
        next_params = {}
        time.sleep(0.1)
    return results


def check_token(access_token: str, app_id: str, app_secret: str) -> dict:
    data = _api_get(
        f"{GRAPH_API_BASE}/debug_token",
        {"input_token": access_token, "access_token": f"{app_id}|{app_secret}"},
    )
    return data.get("data", {})


def extend_token(access_token: str, app_id: str, app_secret: str) -> str:
    resp = requests.get(
        f"{GRAPH_API_BASE}/oauth/access_token",
        params={
            "grant_type": "fb_exchange_token",
            "client_id": app_id,
            "client_secret": app_secret,
            "fb_exchange_token": access_token,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def get_fb_pages(user_token: str) -> list:
    return _paginate(
        f"{GRAPH_API_BASE}/me/accounts",
        {"access_token": user_token, "fields": "id,name,access_token", "limit": 100},
    )


def get_ig_account_id(page_id: str, page_token: str) -> Optional[str]:
    data = _api_get(
        f"{GRAPH_API_BASE}/{page_id}",
        {"access_token": page_token, "fields": "instagram_business_account"},
    )
    ig = data.get("instagram_business_account")
    return ig["id"] if ig else None


def get_ig_media_list(ig_user_id: str, page_token: str) -> list:
    return _paginate(
        f"{GRAPH_API_BASE}/{ig_user_id}/media",
        {
            "access_token": page_token,
            "fields": "id,caption,timestamp,media_type,like_count,comments_count,video_views",
            "limit": 100,
        },
    )


def get_ig_impressions(media_id: str, page_token: str) -> int:
    """누적 노출수. Reels 등 impressions 미지원 타입은 plays로 폴백."""
    for metric in ["impressions", "plays"]:
        try:
            data = _api_get(
                f"{GRAPH_API_BASE}/{media_id}/insights",
                {"access_token": page_token, "metric": metric},
            )
            for item in data.get("data", []):
                if item["name"] == metric:
                    values = item.get("values", [])
                    return int(values[-1].get("value", 0)) if values else 0
        except Exception as exc:
            logger.warning("%s 조회 실패 media_id=%s: %s", metric, media_id, exc)
            if "400" not in str(exc):
                break
    return 0


def get_fb_posts(page_id: str, page_token: str) -> list:
    return _paginate(
        f"{GRAPH_API_BASE}/{page_id}/published_posts",
        {
            "access_token": page_token,
            "fields": "id,message,created_time",
            "limit": 100,
        },
    )


def _get_fb_post_insight(post_id: str, page_token: str, metric: str) -> dict:
    try:
        data = _api_get(
            f"{GRAPH_API_BASE}/{post_id}/insights",
            {"access_token": page_token, "metric": metric, "period": "lifetime"},
        )
        for item in data.get("data", []):
            if item["name"] == metric:
                values = item.get("values", [])
                return values[0].get("value", 0) if values else 0
        return 0
    except Exception as exc:
        logger.warning("%s 조회 실패 post_id=%s: %s", metric, post_id, exc)
        return 0


def get_fb_post_likes(post_id: str, page_token: str) -> int:
    result = _get_fb_post_insight(post_id, page_token, "post_reactions_like_total")
    return int(result) if isinstance(result, (int, float)) else 0


def get_fb_post_comments(post_id: str, page_token: str) -> int:
    result = _get_fb_post_insight(post_id, page_token, "post_activity_by_action_type")
    if isinstance(result, dict):
        return int(result.get("comment", 0))
    return 0


def get_fb_post_impressions(post_id: str, page_token: str) -> int:
    result = _get_fb_post_insight(post_id, page_token, "post_video_views")
    return int(result) if isinstance(result, (int, float)) else 0
