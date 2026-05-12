import time
import os
import json
import requests

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models import Variable

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.cloud import bigquery
from datetime import datetime, timedelta, timezone
from google.oauth2 import service_account


def get_var(key: str, default: str | None = None) -> str:
    """환경 변수 또는 Airflow Variable 조회"""
    return os.environ.get(key) or Variable.get(key, default_var=default)


DAG_FOLDER = os.path.dirname(os.path.realpath(__file__))

API_KEY = get_var("YOUTUBE_ANALYTICS_API_KEY")

SCOPES = [
    "https://www.googleapis.com/auth/yt-analytics.readonly",
    "https://www.googleapis.com/auth/youtube.readonly"
]

PROJECT_ID = 'datahub-478802'
LOCATION = 'US'

DATASET_ID = 'external_data'
views_by_video_id_table_id     = 'youtube_views_by_video_id'
views_by_video_id_pre_table_id = 'youtube_views_by_video_id_pre'
views_by_age_gender_table_id   = 'youtube_views_by_age_gender'
comments_table_id              = 'youtube_comments'

CREDENTIALS_JSON = get_var('GOOGLE_CREDENTIAL_JSON')
NOTION_TOKEN     = get_var('NOTION_TOKEN')

# FSF2 Notion 페이지 ID (URL: notion.so/.../327ea67a5681806b8b37c938a17732f8)
NOTION_FSF2_PAGE_ID = '327ea67a5681806b8b37c938a17732f8'

# ============================================================
# game_code=159 채널은 하드코딩 유지
# game_code=60009 채널은 Notion FSF2 페이지에서 런타임에 동적 조회
# ============================================================
CHANNELS_159 = [
    {
        'yt_channel_id': 'UCJoKVy_QTIm-KIaxuDVq6zQ',
        'handle': 'BIOHAZARD-SurvivalUnit',
        'joyple_game_code': 159,
        'analytics_access': False,
        'token_file': os.path.join(DAG_FOLDER, 'BIOHAZARD-SurvivalUnit-Token.json'),
    },
    {
        'yt_channel_id': 'UC0ghbXNaI27YeODTh4fjubg',
        'handle': 'ResidentEvil-SurvivalUnit',
        'joyple_game_code': 159,
        'analytics_access': False,
    },
]

# game_code=60009 채널 중 Notion 미등록 채널 하드코딩
CHANNELS_60009_FIXED = [
    {
        'channel_name': 'Offical_WW',
        'handle': '@FreeStyleFootball2_EN',
    },
]


# ============================================================
# Notion 표 블록 파싱
# ============================================================

def _get_cell_text(cell: list) -> str:
    """table_row cell (rich_text 배열) → plain text"""
    return ''.join(rt.get('plain_text', '') for rt in cell)


def get_youtube_channels_from_notion() -> list[dict]:
    """
    Notion FSF2 페이지의 '서브 채널' 섹션 아래 표 블록을 파싱.
    플랫폼 == 'Youtube'인 행의 채널명, 핸들명 반환.
    컬럼 순서: 플랫폼(0), 채널명(1), ID(2), PW(3), 핸들명(4), 담당자(5)
    """
    headers = {
        'Authorization': f'Bearer {NOTION_TOKEN}',
        'Notion-Version': '2022-06-28',
    }

    # 1. 페이지 최상위 블록 전체 조회
    blocks = []
    url = f'https://api.notion.com/v1/blocks/{NOTION_FSF2_PAGE_ID}/children'
    params = {'page_size': 100}

    while True:
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()
        blocks.extend(data.get('results', []))
        if not data.get('has_more'):
            break
        params['start_cursor'] = data['next_cursor']
        time.sleep(0.3)

    # 2. "서브 채널" heading_3 블록 다음의 table 블록 찾기
    table_block_id = None
    found_header = False

    for block in blocks:
        if found_header:
            if block['type'] == 'table':
                table_block_id = block['id']
                break
            if block['type'] in ('heading_1', 'heading_2', 'heading_3'):
                break  # 다음 섹션 진입 시 탐색 중단

        if block['type'] == 'heading_3':
            text = ''.join(
                rt.get('plain_text', '')
                for rt in block['heading_3']['rich_text']
            )
            if '서브 채널' in text:
                found_header = True

    if not table_block_id:
        raise ValueError("Notion FSF2 페이지에서 '서브 채널' 표 블록을 찾지 못했습니다.")

    # 3. 표 행(table_row) 조회
    rows = []
    url = f'https://api.notion.com/v1/blocks/{table_block_id}/children'
    params = {'page_size': 100}

    while True:
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()
        rows.extend(data.get('results', []))
        if not data.get('has_more'):
            break
        params['start_cursor'] = data['next_cursor']
        time.sleep(0.3)

    # 4. 헤더 행 제외, 플랫폼 == 'Youtube' 필터링
    channels = []
    for i, row in enumerate(rows):
        if i == 0:  # 헤더 행 스킵
            continue
        cells = row.get('table_row', {}).get('cells', [])
        if len(cells) < 6:
            continue

        platform     = _get_cell_text(cells[0]).strip()
        channel_name = _get_cell_text(cells[1]).strip()
        handle       = _get_cell_text(cells[4]).strip()

        if platform != 'Youtube':
            continue
        if not handle:
            print(f"[Notion] 핸들명 없음, 건너뜀: {channel_name}")
            continue

        channels.append({'channel_name': channel_name, 'handle': handle})

    print(f"[Notion] YouTube 채널 {len(channels)}개 파싱 완료")
    return channels


# ============================================================
# 핸들 → channel_id 변환
# ============================================================

def resolve_handle_to_channel_id(handle: str) -> str | None:
    """YouTube 핸들(@포함/미포함) → UCxxxx channel_id (REST API 직접 호출)"""
    clean_handle = handle.lstrip('@')
    try:
        resp = requests.get(
            'https://www.googleapis.com/youtube/v3/channels',
            params={'part': 'id', 'forHandle': clean_handle, 'key': API_KEY},
        )
        resp.raise_for_status()
        items = resp.json().get('items', [])
        if items:
            channel_id = items[0]['id']
            print(f"[핸들 조회] '{handle}' → '{channel_id}'")
            return channel_id
        print(f"[핸들 조회] '{handle}' 결과 없음")
        return None
    except Exception as e:
        print(f"[핸들 조회] '{handle}' 실패: {e}")
        return None


# ============================================================
# 인증 및 서비스 빌드
# ============================================================

def get_services(token_file):
    creds = None

    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(token_file, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("토큰 갱신 중...")
            creds.refresh(Request())
        else:
            raise Exception(
                f"유효한 토큰이 없습니다. (경로: {token_file})\n"
                "채널마다 개별 토큰이 필요합니다. 아래 절차를 따르세요:\n"
                "  1. 수집 대상 채널의 브랜드 계정 관리자 권한을 OAuth2 인증할 Google 계정에 부여받기\n"
                "  2. 로컬에서 OAuth2 인증 후 token.json 발급\n"
                "  3. 발급된 token.json을 서버의 DAG 폴더에 업로드"
            )

        with open(token_file, "w") as token:
            token.write(creds.to_json())
            print(f"새로운 토큰이 저장되었습니다: {token_file}")

    data_service = build("youtube", "v3", credentials=creds)
    analytics_service = build("youtubeAnalytics", "v2", credentials=creds)

    return data_service, analytics_service


# ============================================================
# 채널 내 동영상 리스트 추출
# ============================================================

def get_video_id_title_map(data_service):
    """채널의 모든 동영상 ID와 제목을 딕셔너리로 반환"""
    video_map = {}
    res = data_service.channels().list(mine=True, part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None
    print("동영상 목록 및 제목 추출 중...")
    while True:
        res = data_service.playlistItems().list(
            playlistId=playlist_id,
            part='snippet',
            maxResults=50,
            pageToken=next_page_token
        ).execute()

        for item in res['items']:
            v_id = item['snippet']['resourceId']['videoId']
            v_title = item['snippet']['title']
            video_map[v_id] = v_title

        next_page_token = res.get('nextPageToken')
        if not next_page_token:
            break
    return video_map


# ============================================================
# Analytics API 방식 데이터 수집
# ============================================================

def fetch_combined_data(analytics_service, video_map, start_date, end_date):
    all_data = []

    for v_id, v_title in video_map.items():
        print(f"조회 중: {v_title} ({v_id})")

        for attempt in range(3):
            try:
                request = analytics_service.reports().query(
                    ids="channel==MINE",
                    startDate=start_date,
                    endDate=end_date,
                    metrics="views,likes,comments,shares",
                    dimensions="day",
                    filters=f"video=={v_id}"
                )
                response = request.execute()

                if 'rows' in response:
                    for row in response['rows']:
                        all_data.append([v_id, v_title] + row)

                time.sleep(0.2)
                break

            except Exception as e:
                if "500" in str(e) and attempt < 2:
                    print(f"   ㄴ 서버 에러 발생. {attempt+1}차 재시도 중...")
                    time.sleep(2)
                else:
                    print(f"에러 발생 ({v_id}): {e}")
                    break

    columns = ['video_id', 'title', 'date', 'views', 'likes', 'comments', 'shares']
    df = pd.DataFrame(all_data, columns=columns)
    df['datekey'] = pd.to_datetime(df['date']).dt.date

    return df[['datekey', 'video_id', 'title', 'views', 'likes', 'comments', 'shares']]


def fetch_combined_data_viewer_per_video(analytics_service, video_map, start_date, end_date):
    all_data = []

    for v_id, v_title in video_map.items():
        print(f"인구통계 조회 중: {v_title} ({v_id})")

        for attempt in range(3):
            try:
                request = analytics_service.reports().query(
                    ids="channel==MINE",
                    startDate=start_date,
                    endDate=end_date,
                    metrics="viewerPercentage",
                    dimensions="ageGroup,gender",
                    sort="ageGroup,gender",
                    filters=f"video=={v_id}"
                )
                response = request.execute()

                if 'rows' in response:
                    for row in response['rows']:
                        all_data.append([v_id, v_title] + row)

                time.sleep(0.3)
                break

            except Exception as e:
                if "500" in str(e) and attempt < 2:
                    print(f"   ㄴ 서버 에러 발생. {attempt+1}차 재시도 중...")
                    time.sleep(2)
                else:
                    print(f"에러 발생 ({v_id}): {e}")
                    break

    columns = ['video_id', 'video_title', 'age_group', 'gender', 'viewer_percentage']
    df = pd.DataFrame(all_data, columns=columns)
    df['datekey'] = end_date
    return df


# ============================================================
# YouTube Data API 방식 데이터 수집 (Analytics 권한 없는 채널)
# ============================================================

def get_all_comments_with_api_key(video_map):
    youtube = build("youtube", "v3", developerKey=API_KEY)
    comments_data = []

    for v_id, v_title in video_map.items():
        print(f"전체 수집 중: {v_title} ({v_id})")
        next_page_token = None

        while True:
            try:
                request = youtube.commentThreads().list(
                    part="snippet",
                    videoId=v_id,
                    maxResults=100,
                    textFormat="plainText",
                    pageToken=next_page_token
                )
                response = request.execute()

                items = response.get('items', [])
                if not items:
                    break

                for item in items:
                    comment = item['snippet']['topLevelComment']['snippet']
                    comments_data.append({
                        'video_id': v_id,
                        'video_title': v_title,
                        'comment_id': item['id'],
                        'author': comment['authorDisplayName'],
                        'text': comment['textDisplay'],
                        'published_at': comment['publishedAt'],
                        'like_count': comment['likeCount']
                    })

                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break

            except HttpError as e:
                if e.resp.status == 403 and 'commentsDisabled' in str(e.reason):
                    print(f"영상 {v_id} 댓글 비활성화 상태 — 건너뜀.")
                else:
                    print(f"영상 {v_id} 처리 중 에러 발생: {e}")
                    print(f"  응답 본문: {e.content}")
                break
            except Exception as e:
                print(f"영상 {v_id} 처리 중 에러 발생: {e}")
                break
        time.sleep(0.2)

    if not comments_data:
        return pd.DataFrame(columns=['comment_id', 'video_id', 'video_title', 'author', 'text', 'published_at', 'like_count'])
    df = pd.DataFrame(comments_data)
    return df[['comment_id', 'video_id', 'video_title', 'author', 'text', 'published_at', 'like_count']]


def get_video_map_with_api_key(channel_id):
    """채널 ID(UCxxxx)로 모든 동영상 ID와 제목을 딕셔너리로 반환 (API 키 사용)"""
    youtube = build("youtube", "v3", developerKey=API_KEY)
    res = youtube.channels().list(id=channel_id, part='contentDetails').execute()
    if not res.get('items'):
        print(f"채널 {channel_id} 조회 결과 없음. 채널 ID 또는 접근 권한 확인 필요.")
        return {}
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    video_map = {}
    next_page_token = None
    print(f"동영상 목록 추출 중 (API 키, 채널: {channel_id})...")
    while True:
        res = youtube.playlistItems().list(
            playlistId=playlist_id,
            part='snippet',
            maxResults=50,
            pageToken=next_page_token
        ).execute()

        for item in res['items']:
            v_id = item['snippet']['resourceId']['videoId']
            v_title = item['snippet']['title']
            video_map[v_id] = v_title

        next_page_token = res.get('nextPageToken')
        if not next_page_token:
            break
    return video_map


def fetch_cumulative_stats_with_api_key(video_map, datekey):
    """videos.list statistics로 영상별 누적값 수집. shares는 Data API 미제공으로 수집 불가."""
    youtube = build("youtube", "v3", developerKey=API_KEY)
    all_data = []
    video_ids = list(video_map.keys())

    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i + 50]
        res = youtube.videos().list(
            part='statistics',
            id=','.join(batch)
        ).execute()

        for item in res.get('items', []):
            v_id = item['id']
            stats = item.get('statistics', {})
            all_data.append({
                'datekey':      datekey,
                'video_id':     v_id,
                'video_title':  video_map.get(v_id, ''),
                'views_cum':    int(stats.get('viewCount',    0)),
                'likes_cum':    int(stats.get('likeCount',    0)),
                'comments_cum': int(stats.get('commentCount', 0)),
            })
        time.sleep(0.2)

    df = pd.DataFrame(all_data)
    df['datekey'] = pd.to_datetime(df['datekey']).dt.date
    return df


def compute_daily_delta_from_pre(client, channel_id, datekey):
    """pre 테이블의 누적값을 이용해 전일 대비 일별 증분(delta)을 계산하여 반환."""
    pre_table = f"`{PROJECT_ID}.{DATASET_ID}.{views_by_video_id_pre_table_id}`"
    query = f"""
    SELECT
        today.datekey,
        '{channel_id}'                                                           AS channel_id,
        today.video_id,
        today.video_title                                                        AS title,
        GREATEST(today.views_cum    - IFNULL(yesterday.views_cum,    0), 0)     AS views,
        GREATEST(today.likes_cum    - IFNULL(yesterday.likes_cum,    0), 0)     AS likes,
        GREATEST(today.comments_cum - IFNULL(yesterday.comments_cum, 0), 0)     AS comments,
        0                                                                        AS shares
    FROM {pre_table} today
    INNER JOIN {pre_table} yesterday
        ON  today.video_id   = yesterday.video_id
        AND today.channel_id = yesterday.channel_id
        AND yesterday.datekey = DATE_SUB(today.datekey, INTERVAL 1 DAY)
    WHERE today.datekey    = '{datekey}'
      AND today.channel_id = '{channel_id}'
    """
    print(f"pre 테이블 delta 계산 중 (채널: {channel_id}, 날짜: {datekey})...")
    df = client.query(query).to_dataframe()
    df['datekey'] = pd.to_datetime(df['datekey']).dt.date
    return df


# ============================================================
# BigQuery Upsert (Merge)
# ============================================================

def upsert_to_bigquery(client, df, PROJECT_ID, BQ_DATASET_ID, TABLE_ID):
    if df.empty:
        print("No data to upsert.")
        return

    timestamp = int(time.time())
    target_table_id = f"{PROJECT_ID}.{BQ_DATASET_ID}.{TABLE_ID}"
    staging_table_name = f"temp_staging_{timestamp}"
    staging_table_id = f"{PROJECT_ID}.{BQ_DATASET_ID}.{staging_table_name}"

    if TABLE_ID == 'youtube_comments':
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            write_disposition="WRITE_TRUNCATE"
        )
    else:
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            write_disposition="WRITE_TRUNCATE",
            schema=[
                bigquery.SchemaField("datekey", "DATE"),
            ]
        )

    try:
        print(f"Loading data to staging table: {staging_table_id}...")
        load_job = client.load_table_from_dataframe(df, staging_table_id, job_config=job_config)
        load_job.result()

        table = client.get_table(staging_table_id)
        table.expires = datetime.now(timezone.utc) + timedelta(hours=1)
        client.update_table(table, ["expires"])

        columns = [col for col in df.columns]

        pk_map = {
            'youtube_views_by_video_id':     ['datekey', 'channel_id', 'video_id'],
            'youtube_views_by_video_id_pre': ['datekey', 'channel_id', 'video_id'],
            'youtube_views_by_age_gender':   ['datekey', 'channel_id', 'video_id', 'age_group', 'gender'],
            'youtube_comments':              ['comment_id', 'channel_id', 'video_id'],
        }
        pks = pk_map.get(TABLE_ID, ['datekey'])
        update_set = ", ".join([f"T.`{col}` = S.`{col}`" for col in columns if col not in pks])

        insert_cols = ", ".join([f"`{col}`" for col in columns])
        insert_values = ", ".join([f"S.`{col}`" for col in columns])

        if TABLE_ID in ('youtube_views_by_video_id', 'youtube_views_by_video_id_pre'):
            merge_query = f"""
            MERGE `{target_table_id}` T
            USING `{staging_table_id}` S
            ON T.datekey = S.datekey AND T.channel_id = S.channel_id AND T.video_id = S.video_id

            WHEN MATCHED THEN
                UPDATE SET {update_set}

            WHEN NOT MATCHED THEN
                INSERT ({insert_cols})
                VALUES ({insert_values})
            """
        elif TABLE_ID == 'youtube_views_by_age_gender':
            merge_query = f"""
            MERGE `{target_table_id}` T
            USING `{staging_table_id}` S
            ON T.datekey = S.datekey AND T.channel_id = S.channel_id AND T.video_id = S.video_id AND T.age_group = S.age_group AND T.gender = S.gender

            WHEN MATCHED THEN
                UPDATE SET {update_set}

            WHEN NOT MATCHED THEN
                INSERT ({insert_cols})
                VALUES ({insert_values})
            """
        elif TABLE_ID == 'youtube_comments':
            merge_query = f"""
            MERGE `{target_table_id}` T
            USING (
                SELECT * FROM `{staging_table_id}`
                QUALIFY ROW_NUMBER() OVER (PARTITION BY comment_id, channel_id, video_id ORDER BY like_count DESC) = 1
            ) S
            ON T.comment_id = S.comment_id AND T.channel_id = S.channel_id AND T.video_id = S.video_id

            WHEN MATCHED THEN
                UPDATE SET {update_set}

            WHEN NOT MATCHED THEN
                INSERT ({insert_cols})
                VALUES ({insert_values})
            """

        print("Executing MERGE query...")
        query_job = client.query(merge_query)
        query_job.result()

        print("Upsert complete.")

    except Exception as e:
        print(f"Upsert Failed: {e}")
        raise e

    finally:
        print(f"Dropping staging table: {staging_table_id}")
        client.delete_table(staging_table_id, not_found_ok=True)


# ============================================================
# 메인 ETL 함수
# ============================================================

def youtube_etl():
    today = datetime.now(timezone.utc).date()
    yesterday = today - timedelta(days=1)
    end_date             = yesterday.isoformat()
    start_date_data_api  = yesterday.isoformat()
    start_date_analytics = (today - timedelta(days=7)).isoformat()
    print(f"데이터 조회 기간 (Data API)     : {start_date_data_api} ~ {end_date}")
    print(f"데이터 조회 기간 (Analytics API): {start_date_analytics} ~ {end_date}")

    creds_dict = json.loads(CREDENTIALS_JSON) if isinstance(CREDENTIALS_JSON, str) else CREDENTIALS_JSON
    bq_creds = service_account.Credentials.from_service_account_info(creds_dict)
    client = bigquery.Client(project=PROJECT_ID, credentials=bq_creds)

    # ── STEP 1/4: Notion FSF2 채널 목록 조회 ───────────────────────────────
    print("\n" + "=" * 60)
    print("STEP 1/4: Notion FSF2 YouTube 채널 목록 조회")
    print("=" * 60)
    notion_channels = get_youtube_channels_from_notion()

    print(f"\n[Notion 파싱 결과] {len(notion_channels)}개 채널:")
    for ch in notion_channels:
        print(f"  채널명={ch['channel_name']} | 핸들={ch['handle']}")

    all_60009_raw = notion_channels + CHANNELS_60009_FIXED
    print(f"[하드코딩 추가] CHANNELS_60009_FIXED {len(CHANNELS_60009_FIXED)}개 병합 → 총 {len(all_60009_raw)}개 핸들 조회 예정")

    channels_60009 = []
    for ch in all_60009_raw:
        channel_id = resolve_handle_to_channel_id(ch['handle'])
        if not channel_id:
            print(f"[채널 조회] channel_id 조회 실패, 건너뜀: {ch['channel_name']} / {ch['handle']}")
            continue
        channels_60009.append({
            'yt_channel_id':    channel_id,
            'handle':           ch['handle'],
            'channel_name':     ch['channel_name'],
            'joyple_game_code': 60009,
            'analytics_access': False,
        })
        time.sleep(0.2)

    CHANNELS = CHANNELS_159 + channels_60009

    print(f"\n[최종 수집 대상 채널] 총 {len(CHANNELS)}개:")
    for ch in CHANNELS:
        print(f"  game_code={ch['joyple_game_code']} | 채널명={ch.get('channel_name', ch.get('handle', '-'))} | 핸들={ch.get('handle', '-')} | channel_id={ch['yt_channel_id']}")
    print(f"[STEP 1 완료] 159: {len(CHANNELS_159)}개, 60009: {len(channels_60009)}개")

    # ── STEP 2/4: 채널별 데이터 수집 ────────────────────────────────────────
    print("\n" + "=" * 60)
    print("STEP 2/4: 채널별 YouTube 데이터 수집")
    print("=" * 60)
    all_views, all_age_gender, all_comments = [], [], []

    for channel in CHANNELS:
        channel_id    = channel['yt_channel_id']
        handle        = channel.get('handle', '')
        game_code     = int(channel['joyple_game_code'])
        has_analytics = channel.get('analytics_access', False)
        source_api    = 'youtube_analytics' if has_analytics else 'youtube_data'
        print(f"\n===== 채널 처리 시작: {channel_id} / {handle} (Analytics 권한: {has_analytics}) =====")

        if has_analytics:
            data_svc, ana_svc = get_services(channel['token_file'])
            video_map = get_video_id_title_map(data_svc)

            df_v = fetch_combined_data(ana_svc, video_map, start_date_analytics, end_date)
            df_v['channel_id']       = channel_id
            df_v['handle']           = handle
            df_v['joyple_game_code'] = game_code
            df_v['source_api']       = source_api
            all_views.append(df_v)

            df_ag = fetch_combined_data_viewer_per_video(ana_svc, video_map, start_date_analytics, end_date)
            df_ag['channel_id']       = channel_id
            df_ag['handle']           = handle
            df_ag['joyple_game_code'] = game_code
            all_age_gender.append(df_ag)

        else:
            video_map = get_video_map_with_api_key(channel_id)

            df_pre = fetch_cumulative_stats_with_api_key(video_map, end_date)
            df_pre['channel_id']       = channel_id
            df_pre['handle']           = handle
            df_pre['joyple_game_code'] = game_code
            upsert_to_bigquery(client, df_pre, PROJECT_ID, DATASET_ID, views_by_video_id_pre_table_id)
            print(f"Pre table upserted for {channel_id}.")

            df_v = compute_daily_delta_from_pre(client, channel_id, end_date)
            if not df_v.empty:
                df_v['handle']           = handle
                df_v['joyple_game_code'] = game_code
                df_v['source_api']       = source_api
                all_views.append(df_v)

        df_c = get_all_comments_with_api_key(video_map)
        df_c['channel_id']       = channel_id
        df_c['handle']           = handle
        df_c['joyple_game_code'] = game_code
        all_comments.append(df_c)

        print(f"===== 채널 처리 완료: {channel_id} =====\n")

    # ── STEP 3/4: 데이터 정제 및 중복 제거 ─────────────────────────────────
    print("\n" + "=" * 60)
    print("STEP 3/4: 데이터 정제 및 중복 제거")
    print("=" * 60)
    df_views        = pd.concat(all_views,    ignore_index=True) if all_views        else pd.DataFrame()
    df_age_gender   = pd.concat(all_age_gender, ignore_index=True) if all_age_gender else pd.DataFrame()
    df_comments_all = pd.concat(all_comments, ignore_index=True) if all_comments     else pd.DataFrame()

    if not df_views.empty:
        df_views['views']            = pd.to_numeric(df_views['views'],            errors='coerce').fillna(0).astype(int)
        df_views['likes']            = pd.to_numeric(df_views['likes'],            errors='coerce').fillna(0).astype(int)
        df_views['comments']         = pd.to_numeric(df_views['comments'],         errors='coerce').fillna(0).astype(int)
        df_views['shares']           = pd.to_numeric(df_views['shares'],           errors='coerce').fillna(0).astype(int)
        df_views['joyple_game_code'] = pd.to_numeric(df_views['joyple_game_code'], errors='coerce').fillna(0).astype(int)
        df_views['channel_id']       = df_views['channel_id'].astype(str)
        df_views['source_api']       = df_views['source_api'].astype(str)

    if not df_age_gender.empty:
        df_age_gender['viewer_percentage'] = pd.to_numeric(df_age_gender['viewer_percentage'], errors='coerce').fillna(0.0).astype(float)
        df_age_gender['age_group']         = df_age_gender['age_group'].astype(str)
        df_age_gender['gender']            = df_age_gender['gender'].astype(str)
        df_age_gender['video_id']          = df_age_gender['video_id'].astype(str)
        df_age_gender['video_title']       = df_age_gender['video_title'].astype(str)
        df_age_gender['channel_id']        = df_age_gender['channel_id'].astype(str)
        df_age_gender['joyple_game_code']  = pd.to_numeric(df_age_gender['joyple_game_code'], errors='coerce').fillna(0).astype(int)
        df_age_gender['datekey']           = pd.to_datetime(df_age_gender['datekey']).dt.date

    if not df_comments_all.empty:
        df_comments_all['published_at'] = pd.to_datetime(df_comments_all['published_at'], errors='coerce')

        if df_comments_all['published_at'].dt.tz is None:
            df_comments_all['published_at'] = df_comments_all['published_at'].dt.tz_localize('UTC')
        else:
            df_comments_all['published_at'] = df_comments_all['published_at'].dt.tz_convert('UTC')

        df_comments_all['like_count']        = pd.to_numeric(df_comments_all['like_count'],        errors='coerce').fillna(0).astype(int)
        df_comments_all['joyple_game_code']  = pd.to_numeric(df_comments_all['joyple_game_code'],  errors='coerce').fillna(0).astype(int)

        for col in ['video_id', 'video_title', 'comment_id', 'author', 'text', 'channel_id']:
            df_comments_all[col] = df_comments_all[col].astype(str)

    views_mb      = df_views.memory_usage(deep=True).sum() / 1024 / 1024
    age_gender_mb = df_age_gender.memory_usage(deep=True).sum() / 1024 / 1024
    comments_mb   = df_comments_all.memory_usage(deep=True).sum() / 1024 / 1024
    total_mb      = views_mb + age_gender_mb + comments_mb
    print(
        f"[메모리 사용량] "
        f"views={views_mb:.2f}MB | "
        f"age_gender={age_gender_mb:.2f}MB | "
        f"comments={comments_mb:.2f}MB | "
        f"합계={total_mb:.2f}MB "
        f"(rows: views={len(df_views)}, age_gender={len(df_age_gender)}, comments={len(df_comments_all)})"
    )

    if not df_views.empty:
        df_views = df_views.drop_duplicates(subset=['datekey', 'channel_id', 'video_id'])
    if not df_age_gender.empty:
        df_age_gender = df_age_gender.drop_duplicates(subset=['datekey', 'channel_id', 'video_id', 'age_group', 'gender'])
    if not df_comments_all.empty:
        df_comments_all = df_comments_all.drop_duplicates(subset=['comment_id', 'channel_id', 'video_id'])

    # ── STEP 4/4: BigQuery 적재 ──────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("STEP 4/4: BigQuery 적재")
    print("=" * 60)
    upsert_to_bigquery(client, df_views, PROJECT_ID, DATASET_ID, views_by_video_id_table_id)
    print("[STEP 4] youtube_views_by_video_id 적재 완료")

    upsert_to_bigquery(client, df_age_gender, PROJECT_ID, DATASET_ID, views_by_age_gender_table_id)
    print("[STEP 4] youtube_views_by_age_gender 적재 완료")

    upsert_to_bigquery(client, df_comments_all, PROJECT_ID, DATASET_ID, comments_table_id)
    print("[STEP 4] youtube_comments 적재 완료")

    print("\n" + "=" * 60)
    print("ETL 전체 완료")
    print("=" * 60)


# ============================================================
# DAG 정의
# ============================================================

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
}

with DAG(
    dag_id='youtube_api_ETL_v2',
    default_args=default_args,
    description='유튜브 api 호출하여 BigQuery에 적재 (채널 목록 Notion 동적 조회)',
    schedule='0 8 * * *',  # PT 00:00 기준 (PST=UTC+8, KST 17:00)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['youtube', 'FSF2', 'RESU', 'bigquery'],
) as dag:

    youtube_etl_task = PythonOperator(
        task_id='youtube_etl_task',
        python_callable=youtube_etl,
    )


youtube_etl_task
