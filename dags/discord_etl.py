"""
Discord 서버 운영 지표 수집 → BigQuery 적재 DAG

수집 테이블 (external_data 데이터셋):
- discord_member_snapshot       : 전체 멤버 목록 스냅샷 (신규가입/탈퇴 추적)
- discord_invite_snapshot       : 초대링크별 누적 사용수 스냅샷
- discord_audit_log_events      : 초대 생성 이벤트 로그 (INVITE_CREATE)
- discord_user_chat_activity    : 채널별 유저별 일별 채팅 횟수 (DAU)
- discord_message_reactions     : 당일 메시지 이모지 리액션 유저 로그 (DAU 보완)

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
from urllib.parse import quote

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ETL_Utils import get_gcp_credentials, PROJECT_ID

logger = logging.getLogger(__name__)

# ─── 상수 ────────────────────────────────────────────────────────────────────

BASE_URL = "https://discord.com/api/v10"
DATASET_ID = "external_data"
DISCORD_EPOCH = 1420070400000  # 2015-01-01 UTC ms

# 수집할 서버 목록 (Guild ID 추가 시 여기에 입력)
GUILDS = [
    {
        "guild_id": "1433264315124678666",  # TODO: 서버명 입력
        "joyple_game_code": 60009,              # TODO: joyple_game_code 입력
    },
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
    """PST 날짜 문자열 → UTC 시작/종료 datetime 반환"""
    pst = pytz.timezone("America/Los_Angeles")
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()
    start_pst = pst.localize(datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0))
    end_pst = start_pst + timedelta(days=1)
    return start_pst.astimezone(timezone.utc), end_pst.astimezone(timezone.utc)


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

    for col in (date_cols or []):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col]).dt.date

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


def fetch_and_load_member_snapshot(**context):
    """
    전체 멤버 목록 스냅샷 수집 → discord_member_snapshot 적재

    수집 API: GET /guilds/{guild_id}/members (페이지네이션)
    - 신규 가입자 : joined_at이 오늘인 유저
    - 탈퇴자     : 전날 스냅샷에 있었으나 오늘 없는 유저 (BQ 쿼리로 계산)

    주의: Developer Portal에서 SERVER MEMBERS INTENT 활성화 필요
    """
    pst = pytz.timezone("America/Los_Angeles")
    target_date_str = datetime.now(pst).date().strftime("%Y-%m-%d")
    client = bigquery.Client(project=PROJECT_ID, credentials=get_gcp_credentials())
    now_ts = datetime.now(timezone.utc).isoformat()
    all_rows = []

    for guild_config in GUILDS:
        guild_id         = guild_config["guild_id"]
        joyple_game_code = int(guild_config["joyple_game_code"])
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
                    "joyple_game_code": joyple_game_code,
                })

            after = members[-1]["user"]["id"]
            if len(members) < 1000:
                break

        logger.info(f"[member_snapshot] {server_name}: {len(all_rows)} members")

    if not all_rows:
        return

    df = pd.DataFrame(all_rows)
    df["inserted_at"] = pd.to_datetime(df["inserted_at"])
    df["joined_at"] = pd.to_datetime(df["joined_at"], format="ISO8601", utc=True)
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
    pst = pytz.timezone("America/Los_Angeles")
    target_date_str = datetime.now(pst).date().strftime("%Y-%m-%d")
    client = bigquery.Client(project=PROJECT_ID, credentials=get_gcp_credentials())
    now_ts = datetime.now(timezone.utc).isoformat()
    all_rows = []

    for guild_config in GUILDS:
        guild_id         = guild_config["guild_id"]
        joyple_game_code = int(guild_config["joyple_game_code"])
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
                "total_joined_via_invite": int(inv.get("uses", 0)),
                "max_join_limit": int(inv.get("max_uses", 0)),
                "created_at": inv.get("created_at", ""),
                "expires_at": inv.get("expires_at") or None,
                "inserted_at": now_ts,
                "joyple_game_code": joyple_game_code,
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
    pst = pytz.timezone("America/Los_Angeles")
    target_date_str = (datetime.now(pytz.utc).astimezone(pst).date() - timedelta(days=1)).strftime("%Y-%m-%d")
    start_utc, end_utc = _get_target_range_utc(target_date_str)
    client = bigquery.Client(project=PROJECT_ID, credentials=get_gcp_credentials())
    now_ts = datetime.now(timezone.utc).isoformat()
    all_rows = []

    for guild_config in GUILDS:
        guild_id         = guild_config["guild_id"]
        joyple_game_code = int(guild_config["joyple_game_code"])
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

            pst = pytz.timezone("America/Los_Angeles")
            done = False
            for entry in entries:
                entry_time = _snowflake_to_datetime(entry["id"])
                if entry_time < start_utc:
                    done = True
                    break
                if entry_time >= end_utc:
                    continue

                entry_datekey = entry_time.astimezone(pst).date().strftime("%Y-%m-%d")
                all_rows.append({
                    "event_id": str(entry["id"]),
                    "datekey": entry_datekey,
                    "server_id": guild_id,
                    "server_name": server_name,
                    "action_type": 40,
                    "action_type_name": "INVITE_CREATE",
                    "user_id": str(entry.get("user_id", "")),
                    "user_name": users_map.get(str(entry.get("user_id", "")), ""),
                    "target_id": str(entry.get("target_id", "")),
                    "created_at": entry_time.isoformat(),
                    "inserted_at": now_ts,
                    "joyple_game_code": joyple_game_code,
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
    ti = context["ti"]
    pst = pytz.timezone("America/Los_Angeles")
    target_date_str = (datetime.now(pytz.utc).astimezone(pst).date() - timedelta(days=1)).strftime("%Y-%m-%d")
    start_utc, end_utc = _get_target_range_utc(target_date_str)
    client = bigquery.Client(project=PROJECT_ID, credentials=get_gcp_credentials())
    now_ts = datetime.now(timezone.utc).isoformat()
    all_rows = []
    reaction_messages = []  # reactions 있는 메시지 메타데이터 → XCom으로 전달

    for guild_config in GUILDS:
        guild_id         = guild_config["guild_id"]
        joyple_game_code = int(guild_config["joyple_game_code"])
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

                        # reactions 있는 메시지 → XCom 전달용 목록에 추가
                        raw_reactions = msg.get("reactions", [])
                        if raw_reactions:
                            emojis = []
                            for r in raw_reactions:
                                ej = r.get("emoji", {})
                                ej_id = ej.get("id")
                                ej_name = ej.get("name", "")
                                emojis.append(f"{ej_name}:{ej_id}" if ej_id else ej_name)
                            reaction_messages.append({
                                "datekey": target_date_str,
                                "server_id": guild_id,
                                "server_name": server_name,
                                "channel_id": channel_id,
                                "channel_name": channel_name,
                                "message_id": msg["id"],
                                "emojis": emojis,
                                "joyple_game_code": joyple_game_code,
                            })

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
                    "joyple_game_code": joyple_game_code,
                })

            logger.info(f"  채널 '{channel_name}': {len(user_msg_count)} unique user(s)")

    # reactions 있는 메시지 목록을 XCom으로 전달
    ti.xcom_push(key="reaction_messages", value=json.dumps(reaction_messages))
    logger.info(f"[chat_activity] reactions 있는 메시지 {len(reaction_messages)}건 XCom push")

    if not all_rows:
        logger.info(f"[chat_activity] {target_date_str}: No data.")
        return

    df = pd.DataFrame(all_rows)
    df["inserted_at"] = pd.to_datetime(df["inserted_at"])
    _upsert_df_to_bq(client, df, "discord_user_chat_activity",
                     merge_keys=["datekey", "server_id", "channel_id", "user_id"],
                     date_cols=["datekey"])
    logger.info(f"[chat_activity] {target_date_str}: {len(all_rows)} row(s) loaded.")


def fetch_and_load_reactions(**context):
    """
    이모지 리액션 유저 수집 → discord_message_reactions 적재

    수집 흐름:
    1. fetch_and_load_chat_activity의 XCom에서 reactions 있는 메시지 목록 수신
    2. 메시지별 이모지별 유저 목록 API 호출
       - 유니코드 이모지: 이모지 문자 그대로 (예: 👍)
       - 커스텀 이모지 : name:id 형식 (예: custom_name:123456789)
    3. BigQuery 적재

    주의:
    - 메시지 재스캔 없음 — XCom으로 받은 목록만 처리
    - 리액션 timestamp은 Discord API 미제공 → inserted_at(수집 시각)으로 대체
    - 이모지 수 × 메시지 수만큼 API 호출 발생 (rate limit 주의)
    """
    ti = context["ti"]
    pst = pytz.timezone("America/Los_Angeles")
    target_date_str = (datetime.now(pytz.utc).astimezone(pst).date() - timedelta(days=1)).strftime("%Y-%m-%d")
    client = bigquery.Client(project=PROJECT_ID, credentials=get_gcp_credentials())
    now_ts = datetime.now(timezone.utc).isoformat()
    all_rows = []

    # XCom에서 reactions 있는 메시지 목록 수신
    raw = ti.xcom_pull(key="reaction_messages", task_ids="fetch_and_load_chat_activity")
    reaction_messages = json.loads(raw) if raw else []
    logger.info(f"[reactions] XCom에서 {len(reaction_messages)}건 수신")

    if not reaction_messages:
        logger.info(f"[reactions] {target_date_str}: No messages with reactions.")
        return

    for msg_meta in reaction_messages:
        channel_id       = msg_meta["channel_id"]
        channel_name     = msg_meta["channel_name"]
        message_id       = msg_meta["message_id"]
        server_id        = msg_meta["server_id"]
        server_name      = msg_meta["server_name"]
        joyple_game_code = int(msg_meta.get("joyple_game_code", 0))

        for emoji_str in msg_meta["emojis"]:
            encoded_emoji = quote(emoji_str, safe="")
            after_user_id = None

            while True:
                params = {"limit": 100}
                if after_user_id:
                    params["after"] = after_user_id

                try:
                    users = _discord_get(
                        f"/channels/{channel_id}/messages/{message_id}/reactions/{encoded_emoji}",
                        params=params,
                    )
                except requests.HTTPError as e:
                    if e.response.status_code in (404, 403):
                        logger.warning(f"  메시지 {message_id} 이모지 '{emoji_str}' 조회 실패({e.response.status_code}). 건너뜀.")
                        break
                    raise

                if not users:
                    break

                for user in users:
                    all_rows.append({
                        "datekey": target_date_str,
                        "server_id": server_id,
                        "server_name": server_name,
                        "channel_id": channel_id,
                        "channel_name": channel_name,
                        "message_id": message_id,
                        "emoji": emoji_str,
                        "user_id": user.get("id", ""),
                        "user_name": user.get("username", ""),
                        "inserted_at": now_ts,
                        "joyple_game_code": joyple_game_code,
                    })

                if len(users) < 100:
                    break
                after_user_id = users[-1]["id"]

    logger.info(f"[reactions] {target_date_str}: {len(all_rows)} reaction row(s) collected")

    if not all_rows:
        return

    df = pd.DataFrame(all_rows)
    df["inserted_at"] = pd.to_datetime(df["inserted_at"])
    _upsert_df_to_bq(client, df, "discord_message_reactions",
                     merge_keys=["datekey", "server_id", "channel_id", "message_id", "emoji", "user_id"],
                     date_cols=["datekey"])
    logger.info(f"[reactions] {target_date_str}: {len(all_rows)} row(s) loaded.")


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
    schedule="0 8 * * *",  # KST 17:00 매일 (UTC 08:00)
    start_date=datetime(2026, 4, 14),
    catchup=False,
    tags=["discord", "external_data", "etl"],
) as dag:

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

    t_reactions = PythonOperator(
        task_id="fetch_and_load_reactions",
        python_callable=fetch_and_load_reactions,
    )

    # 독립 태스크는 병렬 실행, reactions는 chat_activity 이후 실행 (rate limit 방지)
    [t_member_snapshot, t_invite_snapshot, t_audit_logs, t_chat_activity]
    t_chat_activity >> t_reactions
