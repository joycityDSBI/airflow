import time
import logging
import requests

logger = logging.getLogger(__name__)

TIKTOK_API_BASE = "https://open.tiktokapis.com/v2"


def get_access_token(code: str, client_key: str, client_secret: str, redirect_uri: str) -> dict:
    """OAuth code → access_token + refresh_token (최초 1회)"""
    resp = requests.post(
        f"{TIKTOK_API_BASE}/oauth/token/",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "client_key": client_key,
            "client_secret": client_secret,
            "code": code,
            "grant_type": "authorization_code",
            "redirect_uri": redirect_uri,
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("error"):
        raise RuntimeError(f"Token 발급 실패: {data}")
    return data


def refresh_access_token(refresh_token: str, client_key: str, client_secret: str) -> dict:
    """refresh_token으로 access_token 갱신. 갱신된 토큰 정보 dict 반환."""
    resp = requests.post(
        f"{TIKTOK_API_BASE}/oauth/token/",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "client_key": client_key,
            "client_secret": client_secret,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("error"):
        raise RuntimeError(f"Token 갱신 실패: {data}")
    return data


def _api_get(url: str, access_token: str, params: dict = None, max_retries: int = 3) -> dict:
    headers = {"Authorization": f"Bearer {access_token}"}
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=headers, params=params or {}, timeout=30)
            if resp.status_code == 429:
                wait_sec = (2 ** attempt) * 5
                logger.warning("Rate limit hit. Sleeping %ds (attempt %d)", wait_sec, attempt + 1)
                time.sleep(wait_sec)
                continue
            if resp.status_code == 400:
                logger.error("400 응답 본문: %s", resp.text)
                resp.raise_for_status()
            resp.raise_for_status()
            data = resp.json()
            error_code = data.get("error", {}).get("code", "ok")
            if error_code not in ("ok", ""):
                raise RuntimeError(f"TikTok API 오류: {data}")
            return data
        except requests.HTTPError as exc:
            if attempt == max_retries - 1:
                raise
            if exc.response is not None and exc.response.status_code == 400:
                raise
            wait_sec = (2 ** attempt) * 2
            logger.warning("HTTP error (attempt %d/%d): %s. Retrying in %ds", attempt + 1, max_retries, exc, wait_sec)
            time.sleep(wait_sec)
    raise RuntimeError(f"Max retries exceeded: {url}")


def _api_post(url: str, access_token: str, params: dict = None, body: dict = None, max_retries: int = 3) -> dict:
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    for attempt in range(max_retries):
        try:
            resp = requests.post(url, headers=headers, params=params or {}, json=body or {}, timeout=30)
            if resp.status_code == 429:
                wait_sec = (2 ** attempt) * 5
                logger.warning("Rate limit hit. Sleeping %ds (attempt %d)", wait_sec, attempt + 1)
                time.sleep(wait_sec)
                continue
            if resp.status_code == 400:
                logger.error("400 응답 본문: %s", resp.text)
                resp.raise_for_status()
            resp.raise_for_status()
            data = resp.json()
            error_code = data.get("error", {}).get("code", "ok")
            if error_code not in ("ok", ""):
                raise RuntimeError(f"TikTok API 오류: {data}")
            return data
        except requests.HTTPError as exc:
            if attempt == max_retries - 1:
                raise
            if exc.response is not None and exc.response.status_code == 400:
                raise
            wait_sec = (2 ** attempt) * 2
            logger.warning("HTTP error (attempt %d/%d): %s. Retrying in %ds", attempt + 1, max_retries, exc, wait_sec)
            time.sleep(wait_sec)
    raise RuntimeError(f"Max retries exceeded: {url}")


def get_user_info(access_token: str) -> dict:
    """계정 기본 정보 + 통계 조회"""
    data = _api_get(
        f"{TIKTOK_API_BASE}/user/info/",
        access_token,
        params={"fields": "open_id,union_id,display_name,follower_count,following_count,likes_count,video_count"},
    )
    return data.get("data", {}).get("user", {})


def get_video_list(access_token: str) -> list:
    """전체 영상 목록 + 통계 조회 (cursor 페이지네이션)"""
    videos = []
    cursor = 0
    has_more = True

    while has_more:
        data = _api_post(
            f"{TIKTOK_API_BASE}/video/list/",
            access_token,
            params={"fields": "id,title,create_time,like_count,comment_count,view_count,share_count"},
            body={"max_count": 20, "cursor": cursor},
        )
        result = data.get("data", {})
        videos.extend(result.get("videos", []))
        has_more = result.get("has_more", False)
        cursor = result.get("cursor", 0)
        if has_more:
            time.sleep(0.5)

    return videos
