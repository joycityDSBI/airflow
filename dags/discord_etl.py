"""
Discord 서버 운영 지표 수집 → BigQuery 적재 DAG

수집 테이블 (external_data 데이터셋):
- discord_daily_server_stats    : 일별 서버 멤버 수 스냅샷
- discord_member_snapshot       : 전체 멤버 목록 스냅샷 (신규가입/탈퇴 추적)
- discord_invite_snapshot       : 초대링크별 누적 사용수 스냅샷
- discord_audit_log_events      : 초대 생성 이벤트 로그 (INVITE_CREATE)
- discord_user_chat_activity    : 채널별 유저별 일별 채팅 횟수 (DAU)

스케줄: KST 06:00 매일 (cron: "0 21 * * *" UTC)
수집 대상: 어제 KST 날짜

Airflow Variables 필요:
- DISCORD_BOT_TOKEN     : Discord Bot Token
- GOOGLE_CREDENTIAL_JSON : GCP 서비스 계정 키 (기존)
"""

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models import Variable
from google.cloud import bigquery
import requests
import json
import time
import logging
from datetime import datetime, timezone, timedelta
import pytz
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ETL_Utils import get_gcp_credentials, calc_target_date, PROJECT_ID

logger = logging.getLogger(__name__)

# ─── 상수 ────────────────────────────────────────────────────────────────────

BASE_URL = "https://discord.com/api/v10"
DATASET_ID = "external_data"
DISCORD_EPOCH = 1420070400000  # 2015-01-01 UTC ms

# 수집할 서버 목록 (Guild ID 추가 시 여기에 입력)
GUILD_IDS = [
    "1433264315124678666",  # TODO: 서버명 입력
]


# ─── Discord API 유틸 ─────────────────────────────────────────────────────────

def _get_headers() -> dict:
    token = Variable.get("DISCORD_BOT_TOKEN")
    return {
        "Authorization": f"Bot {token}",
        "Content-Type": "application/json",
    }


def _discord_get(path: str, params: dict = None) -> any:
    """Rate-limit 대응 포함 Discord REST GET 요청"""
    headers = _get_headers()
    url = f"{BASE_URL}{path}"
    while True:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code == 429:
            retry_after = float(resp.json().get("retry_after", 1.0))
            logger.warning(f"Rate limited. Retrying after {retry_after}s")
            time.sleep(retry_after)
            continue
        resp.raise_for_status()
        time.sleep(0.5)
        return resp.json()


def _datetime_to_snowflake(dt: datetime) -> int:
    """datetime → Discord Snowflake ID (날짜 범위 필터링용)"""
    return (int(dt.timestamp() * 1000) - DISCORD_EPOCH) << 22


def _snowflake_to_datetime(snowflake_id) -> datetime:
    """Discord Snowflake ID → UTC datetime"""
    return datetime.fromtimestamp(
        ((int(snowflake_id) >> 22) + DISCORD_EPOCH) / 1000,
        tz=timezone.utc
    )


def _get_target_range_utc(target_date_str: str):
    """KST 날짜 문자열 → UTC 시작/종료 datetime 반환"""
    kst = pytz.timezone("Asia/Seoul")
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()
    start_kst = kst.localize(datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0))
    end_kst = start_kst + timedelta(days=1)
    return start_kst.astimezone(timezone.utc), end_kst.astimezone(timezone.utc)


def _get_server_name(guild_id: str) -> str:
    """서버명 조회"""
    guild = _discord_get(f"/guilds/{guild_id}", params={"with_counts": "true"})
    return guild.get("name", "")


# ─── BigQuery upsert 유틸 ─────────────────────────────────────────────────────

def _upsert_df_to_bq(client, df: pd.DataFrame, table_id: str,
                     merge_keys: list, date_cols: list = None):
    """DataFrame → staging table → MERGE → cleanup"""
    if df.empty:
        logger.info(f"[{table_id}] No data to upsert.")
        return

    timestamp = int(time.time())
    target_table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"
    staging_table_id = f"{PROJECT_ID}.{DATASET_ID}.temp_{table_id}_{timestamp}"

    schema = [bigquery.SchemaField(col, "DATE") for col in (date_cols or [])]
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
        schema=schema,
    )

    try:
        logger.info(f"[{table_id}] Loading {len(df)} rows to staging...")
        client.load_table_from_dataframe(df, staging_table_id, job_config=job_config).result()

        table = client.get_table(staging_table_id)
        table.expires = datetime.utcnow() + timedelta(hours=1)
        client.update_table(table, ["expires"])

        cols = df.columns.tolist()
        on_clause = " AND ".join([f"T.`{k}` = S.`{k}`" for k in merge_keys])
        update_set = ", ".join([f"T.`{c}` = S.`{c}`" for c in cols if c not in merge_keys])
        insert_cols = ", ".join([f"`{c}`" for c in cols])
        insert_vals = ", ".join([f"S.`{c}`" for c in cols])

        client.query(f"""
            MERGE `{target_table_id}` T
            USING `{staging_table_id}` S ON {on_clause}
            WHEN MATCHED THEN UPDATE SET {update_set}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """).result()
        logger.info(f"[{table_id}] Upsert complete.")

    except Exception as e:
        logger.error(f"[{table_id}] Upsert failed: {e}")
        raise

    finally:
        client.delete_table(staging_table_id, not_found_ok=True)


# ─── Task 함수 ────────────────────────────────────────────────────────────────

def fetch_and_load_server_stats(**context):
    """
    서버 멤버 수 스냅샷 수집 → discord_daily_server_stats 적재

    수집 API: GET /guilds/{guild_id}?with_counts=true
    - total_members  : 전체 멤버 수 (근사값)
    - online_members : Discord 앱을 켜고 있는 멤버 수 (근사값, 서버 특정 아님)
    """
    target_dates, _ = calc_target_date(context["logical_date"])
    target_date_str = target_dates[0]
    client = bigquery.Client(project=PROJECT_ID, credentials=get_gcp_credentials())
    now_ts = datetime.now(timezone.utc).isoformat()
    rows = []

    for guild_id in GUILD_IDS:
        guild = _discord_get(f"/guilds/{guild_id}", params={"with_counts": "true"})
        rows.append({
            "datekey": target_date_str,
            "server_id": guild_id,
            "server_name": guild.get("name", ""),
            "total_members": guild.get("approximate_member_count", 0),
            "online_members": guild.get("approximate_presence_count", 0),
            "inserted_at": now_ts,
        })
        logger.info(f"[server_stats] {guild.get('name')}: {guild.get('approximate_member_count')} members")

    df = pd.DataFrame(rows)
    df["inserted_at"] = pd.to_datetime(df["inserted_at"])
    _upsert_df_to_bq(client, df, "discord_daily_server_stats",
                     merge_keys=["datekey", "server_id"],
                     date_cols=["datekey"])


def fetch_and_load_member_snapshot(**context):
    """
    전체 멤버 목록 스냅샷 수집 → discord_member_snapshot 적재

    수집 API: GET /guilds/{guild_id}/members (페이지네이션)
    - 신규 가입자 : joined_at이 어제인 유저
    - 탈퇴자     : 전날 스냅샷에 있었으나 오늘 없는 유저 (BQ 쿼리로 계산)

    주의: Developer Portal에서 SERVER MEMBERS INTENT 활성화 필요
    """
    target_dates, _ = calc_target_date(context["logical_date"])
    target_date_str = target_dates[0]
    client = bigquery.Client(project=PROJECT_ID, credentials=get_gcp_credentials())
    now_ts = datetime.now(timezone.utc).isoformat()
    all_rows = []

    for guild_id in GUILD_IDS:
        server_name = _get_server_name(guild_id)
        after = "0"

        while True:
            members = _discord_get(f"/guilds/{guild_id}/members",
                                   params={"limit": 1000, "after": after})
            if not members:
                break

            for m in members:
                user = m.get("user", {})
                all_rows.append({
                    "datekey": target_date_str,
                    "server_id": guild_id,
                    "server_name": server_name,
                    "user_id": user.get("id", ""),
                    "user_name": user.get("username", ""),
                    "joined_at": m.get("joined_at", ""),
                    "inserted_at": now_ts,
                })

            after = members[-1]["user"]["id"]
            if len(members) < 1000:
                break

        logger.info(f"[member_snapshot] {server_name}: {len(all_rows)} members")

    if not all_rows:
        return

    df = pd.DataFrame(all_rows)
    df["inserted_at"] = pd.to_datetime(df["inserted_at"])
    df["joined_at"] = pd.to_datetime(df["joined_at"], utc=True)
    _upsert_df_to_bq(client, df, "discord_member_snapshot",
                     merge_keys=["datekey", "server_id", "user_id"],
                     date_cols=["datekey"])


def fetch_and_load_invite_snapshot(**context):
    """
    초대링크 스냅샷 수집 → discord_invite_snapshot 적재

    수집 API: GET /guilds/{guild_id}/invites
    - uses       : 누적 사용 횟수 (전날 diff = 일별 초대링크 가입자 수)
    - created_at : 초대 링크 생성 시각

    주의: Bot에 Manage Guild 권한 필요
    """
    target_dates, _ = calc_target_date(context["logical_date"])
    target_date_str = target_dates[0]
    client = bigquery.Client(project=PROJECT_ID, credentials=get_gcp_credentials())
    now_ts = datetime.now(timezone.utc).isoformat()
    all_rows = []

    for guild_id in GUILD_IDS:
        server_name = _get_server_name(guild_id)
        invites = _discord_get(f"/guilds/{guild_id}/invites")

        if not invites:
            logger.info(f"[invite_snapshot] {server_name}: No invites found.")
            continue

        for inv in invites:
            inviter = inv.get("inviter") or {}
            all_rows.append({
                "datekey": target_date_str,
                "server_id": guild_id,
                "server_name": server_name,
                "invite_code": inv.get("code", ""),
                "inviter_id": inviter.get("id", ""),
                "inviter_name": inviter.get("username", ""),
                "uses": int(inv.get("uses", 0)),
                "max_uses": int(inv.get("max_uses", 0)),
                "created_at": inv.get("created_at", ""),
                "expires_at": inv.get("expires_at") or None,
                "inserted_at": now_ts,
            })

        logger.info(f"[invite_snapshot] {server_name}: {len(invites)} invite(s)")

    if not all_rows:
        return

    df = pd.DataFrame(all_rows)
    df["inserted_at"] = pd.to_datetime(df["inserted_at"])
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
    df["expires_at"] = pd.to_datetime(df["expires_at"], utc=True, errors="coerce")
    _upsert_df_to_bq(client, df, "discord_invite_snapshot",
                     merge_keys=["datekey", "server_id", "invite_code"],
                     date_cols=["datekey"])


def fetch_and_load_audit_logs(**context):
    """
    초대 생성 감사 로그 수집 → discord_audit_log_events 적재

    수집 API: GET /guilds/{guild_id}/audit-logs?action_type=40
    - action_type=40 : INVITE_CREATE (초대 링크 생성)
    - Snowflake ID로 날짜 범위 필터링

    주의: 감사 로그는 Discord에서 약 45일만 보관 → 매일 수집 필수
    """
    target_dates, _ = calc_target_date(context["logical_date"])
    target_date_str = target_dates[0]
    start_utc, end_utc = _get_target_range_utc(target_date_str)
    client = bigquery.Client(project=PROJECT_ID, credentials=get_gcp_credentials())
    now_ts = datetime.now(timezone.utc).isoformat()
    all_rows = []

    for guild_id in GUILD_IDS:
        server_name = _get_server_name(guild_id)
        before_id = str(_datetime_to_snowflake(end_utc))

        while True:
            data = _discord_get(f"/guilds/{guild_id}/audit-logs", params={
                "action_type": 40,
                "limit": 100,
                "before": before_id,
            })
            entries = data.get("audit_log_entries", [])
            users_map = {u["id"]: u.get("username", "") for u in data.get("users", [])}

            if not entries:
                break

            done = False
            for entry in entries:
                entry_time = _snowflake_to_datetime(entry["id"])
                if entry_time < start_utc:
                    done = True
                    break
                if entry_time >= end_utc:
                    continue

                all_rows.append({
                    "event_id": str(entry["id"]),
                    "datekey": target_date_str,
                    "server_id": guild_id,
                    "server_name": server_name,
                    "action_type": 40,
                    "action_type_name": "INVITE_CREATE",
                    "user_id": str(entry.get("user_id", "")),
                    "user_name": users_map.get(str(entry.get("user_id", "")), ""),
                    "target_id": str(entry.get("target_id", "")),
                    "created_at": entry_time.isoformat(),
                    "inserted_at": now_ts,
                })

            if done or len(entries) < 100:
                break
            before_id = entries[-1]["id"]

        logger.info(f"[audit_logs] {server_name}: {len(all_rows)} INVITE_CREATE event(s)")

    if not all_rows:
        return

    df = pd.DataFrame(all_rows)
    df["inserted_at"] = pd.to_datetime(df["inserted_at"])
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
    _upsert_df_to_bq(client, df, "discord_audit_log_events",
                     merge_keys=["event_id"],
                     date_cols=["datekey"])


def fetch_and_load_chat_activity(**context):
    """
    채널별 유저별 일별 채팅 횟수 수집 → discord_user_chat_activity 적재

    수집 API:
    - GET /guilds/{guild_id}/channels : 텍스트 채널 목록 조회
    - GET /channels/{channel_id}/messages : 채널별 메시지 페이지네이션

    DAU 계산:
    - SELECT COUNT(DISTINCT user_id) FROM discord_user_chat_activity WHERE datekey = 'X'

    주의:
    - Bot 메시지 제외 (author.bot = true)
    - 채널 접근 권한 없으면 해당 채널 건너뜀 (403 처리)
    - 어제 날짜 메시지만 수집 (Snowflake ID로 날짜 필터링)
    """
    target_dates, _ = calc_target_date(context["logical_date"])
    target_date_str = target_dates[0]
    start_utc, end_utc = _get_target_range_utc(target_date_str)
    client = bigquery.Client(project=PROJECT_ID, credentials=get_gcp_credentials())
    now_ts = datetime.now(timezone.utc).isoformat()
    all_rows = []

    for guild_id in GUILD_IDS:
        server_name = _get_server_name(guild_id)

        channels = _discord_get(f"/guilds/{guild_id}/channels")
        # type 0 = GUILD_TEXT, type 5 = GUILD_ANNOUNCEMENT
        text_channels = [c for c in channels if c.get("type") in (0, 5)]
        logger.info(f"[chat_activity] {server_name}: {len(text_channels)} text channel(s)")

        for channel in text_channels:
            channel_id = channel["id"]
            channel_name = channel.get("name", "")
            user_msg_count = {}
            user_names = {}
            before_id = str(_datetime_to_snowflake(end_utc))

            try:
                while True:
                    messages = _discord_get(f"/channels/{channel_id}/messages", params={
                        "limit": 100,
                        "before": before_id,
                    })
                    if not messages:
                        break

                    done = False
                    for msg in messages:
                        msg_time = datetime.fromisoformat(msg["timestamp"].replace("Z", "+00:00"))
                        if msg_time < start_utc:
                            done = True
                            break
                        if msg_time >= end_utc:
                            continue

                        author = msg.get("author", {})
                        if author.get("bot", False):
                            continue

                        uid = author.get("id", "")
                        user_msg_count[uid] = user_msg_count.get(uid, 0) + 1
                        user_names[uid] = author.get("username", "")

                    if done or len(messages) < 100:
                        break
                    before_id = messages[-1]["id"]

            except requests.HTTPError as e:
                if e.response.status_code == 403:
                    logger.warning(f"  채널 '{channel_name}' 접근 권한 없음. 건너뜀.")
                    continue
                raise

            for uid, count in user_msg_count.items():
                all_rows.append({
                    "datekey": target_date_str,
                    "server_id": guild_id,
                    "server_name": server_name,
                    "channel_id": channel_id,
                    "channel_name": channel_name,
                    "user_id": uid,
                    "user_name": user_names.get(uid, ""),
                    "message_count": count,
                    "inserted_at": now_ts,
                })

            logger.info(f"  채널 '{channel_name}': {len(user_msg_count)} unique user(s)")

    if not all_rows:
        logger.info(f"[chat_activity] {target_date_str}: No data.")
        return

    df = pd.DataFrame(all_rows)
    df["inserted_at"] = pd.to_datetime(df["inserted_at"])
    _upsert_df_to_bq(client, df, "discord_user_chat_activity",
                     merge_keys=["datekey", "server_id", "channel_id", "user_id"],
                     date_cols=["datekey"])
    logger.info(f"[chat_activity] {target_date_str}: {len(all_rows)} row(s) loaded.")


# ─── DAG 정의 ─────────────────────────────────────────────────────────────────

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="discord_etl",
    default_args=default_args,
    description="Discord 서버 운영 지표 수집 → BigQuery 적재",
    schedule="0 21 * * *",  # KST 06:00 매일 (UTC 21:00 전날)
    start_date=datetime(2026, 4, 14),
    catchup=False,
    tags=["discord", "external_data", "etl"],
) as dag:

    t_server_stats = PythonOperator(
        task_id="fetch_and_load_server_stats",
        python_callable=fetch_and_load_server_stats,
    )

    t_member_snapshot = PythonOperator(
        task_id="fetch_and_load_member_snapshot",
        python_callable=fetch_and_load_member_snapshot,
    )

    t_invite_snapshot = PythonOperator(
        task_id="fetch_and_load_invite_snapshot",
        python_callable=fetch_and_load_invite_snapshot,
    )

    t_audit_logs = PythonOperator(
        task_id="fetch_and_load_audit_logs",
        python_callable=fetch_and_load_audit_logs,
    )

    t_chat_activity = PythonOperator(
        task_id="fetch_and_load_chat_activity",
        python_callable=fetch_and_load_chat_activity,
    )

    # 5개 태스크 모두 독립적으로 병렬 실행
    [t_server_stats, t_member_snapshot, t_invite_snapshot, t_audit_logs, t_chat_activity]
