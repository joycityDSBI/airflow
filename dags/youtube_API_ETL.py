import time
import os

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
import json


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
views_by_video_id_pre_table_id = 'youtube_views_by_video_id_pre'  # 누적값 임시 적재용
views_by_age_gender_table_id   = 'youtube_views_by_age_gender'
comments_table_id              = 'youtube_comments'

CREDENTIALS_JSON = get_var('GOOGLE_CREDENTIAL_JSON')

# ============================================================
# 채널 설정
#
# [인증 방식]
# YouTube Analytics API는 채널 소유자(또는 브랜드 계정 관리자)의
# OAuth2 인증이 필요합니다. 수집하려는 채널마다 별도 토큰이 필요합니다.
#
# [토큰 발급 절차 (채널당 1회)]
#   1. 수집 대상 채널의 브랜드 계정 관리자 권한을 우리 Google 계정에 부여받는다.
#   2. 로컬에서 OAuth2 인증 흐름을 실행하여 token.json 발급
#      (generate_token.py 또는 유사 스크립트 사용)
#   3. 발급된 token.json 파일을 서버의 DAG 폴더에 업로드
#
# [항목 설명]
#   - yt_channel_id    : 실제 YouTube 채널 ID (UC로 시작하는 값)
#                        BigQuery channel_id 컬럼에 저장됨
#   - joyple_game_code : JoyCity 게임 코드 (INTEGER). 공용 테이블에서 게임 구분자로 사용.
#   - analytics_access : True  → 브랜드 계정 관리자 권한 보유.
#                                 Analytics API로 일별 데이터 직접 수집.
#                                 token_file 필수.
#                        False → Analytics API 권한 없음 (현재 기본값).
#                                 YouTube Data API 누적값 방식으로 수집
#                                 (pre 테이블 적재 후 일별 delta 계산).
#   - token_file       : analytics_access=True 채널에만 필요.
#                        해당 채널 OAuth2 인증으로 발급받은 token.json 경로.
# ============================================================
CHANNELS = [
    {
        'yt_channel_id': 'UCJoKVy_QTIm-KIaxuDVq6zQ',  # BIOHAZARD-SurvivalUnit (JP/KR)
        'handle': 'BIOHAZARD-SurvivalUnit',
        'joyple_game_code': 159,
        'analytics_access': False,
        'token_file': os.path.join(DAG_FOLDER, 'BIOHAZARD-SurvivalUnit-Token.json'),
    },
    {
        'yt_channel_id': 'UC0ghbXNaI27YeODTh4fjubg',  # RESU (EN/Global)
        'handle': 'ResidentEvil-SurvivalUnit',
        'joyple_game_code': 159,
        'analytics_access': False,
    },
    # {
    #     'yt_channel_id': 'UCCCIXa7W452h5MEga8L-AoA',  # FreeStyleFootball2 WW - 채널 ID 확인 필요
    #     'handle': 'FreeStyleFootball2_WW',
    #     'joyple_game_code': 60009,
    #     'analytics_access': False,
    # },
    {
        'yt_channel_id': 'UCMXkl-5yTf7ILL3EIgASSOw',  # モエサッカーファイア
        'handle': 'モエサッカーファイア',
        'joyple_game_code': 60009,
        'analytics_access': False,
    },
    {
        'yt_channel_id': 'UCm_qEt6y2dNsP22-zZ6aLxg',  # 내맘대로홍보
        'handle': '내맘대로홍보',
        'joyple_game_code': 60009,
        'analytics_access': False,
    },
    {
        'yt_channel_id': 'UCfR0wUYMlzfNc0MGW6vEFiw',  # 캐릭터를아끼자
        'handle': '캐릭터를아끼자',
        'joyple_game_code': 60009,
        'analytics_access': False,
    },
    {
        'yt_channel_id': 'UCMbhwVGWK6l0p6_hcuAi2Iw',  # Promoting it my damn way
        'handle': 'Promotingitmydamnway',
        'joyple_game_code': 60009,
        'analytics_access': False,
    },
    {
        'yt_channel_id': 'UCfEeyimdLqIxn6GNpjhisNQ',  # 프리스타일풋볼2 런칭존버 푸시업 챌린지
        'handle': '프풋2챌린지',
        'joyple_game_code': 60009,
        'analytics_access': False,
    },
    {
        'yt_channel_id': 'UCEl7v-mdnHN4aznlBMjRAfw',  # 팀장알러지
        'handle': 'menheraforteamleader',
        'joyple_game_code': 60009,
        'analytics_access': False,
    },
    {
        'yt_channel_id': 'UC7qGtl_aq6c92MoOAR-w9rQ',  # BossAllergy
        'handle': 'BossAllergy',
        'joyple_game_code': 60009,
        'analytics_access': False,
    },
]


# 인증 및 서비스 빌드 함수
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


# 채널 내 동영상 리스트 추출
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


# 각 영상별 일자별 조회수, 좋아요, 댓글 수, 공유 수 조회
def fetch_combined_data(analytics_service, video_map, start_date, end_date):
    all_data = []

    for v_id, v_title in video_map.items():
        print(f"조회 중: {v_title} ({v_id})")

        for attempt in range(3):
            try:
                request = analytics_service.reports().query(
                    ids="channel==MINE", #TODO : 브랜드 계정관리 채널의 경우 channel_id 값으로 설정을해야함. 참고하길.
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


# 시청자 성별/연령대 데이터 조회. 누적 값 기준 viewerPercentage
def fetch_combined_data_viewer_per_video(analytics_service, video_map, start_date, end_date):
    all_data = []

    for v_id, v_title in video_map.items():
        print(f"인구통계 조회 중: {v_title} ({v_id})")

        for attempt in range(3):
            try:
                request = analytics_service.reports().query(
                    ids="channel==MINE", #TODO : 브랜드 계정관리 채널의 경우 channel_id 값으로 설정을해야함. 참고하길.
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


# 전체 비디오 댓글 수집 (API 키 방식)
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
                break
            except Exception as e:
                print(f"영상 {v_id} 처리 중 에러 발생: {e}")
                break
        time.sleep(0.2)

    if not comments_data:
        return pd.DataFrame(columns=['comment_id', 'video_id', 'video_title', 'author', 'text', 'published_at', 'like_count'])
    df = pd.DataFrame(comments_data)
    return df[['comment_id', 'video_id', 'video_title', 'author', 'text', 'published_at', 'like_count']]
 

# API 키로 채널 비디오 목록 조회 (Analytics 권한 없는 채널용)
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


# 영상별 누적 조회수/좋아요/댓글 수 수집 (YouTube Data API, Analytics 권한 없는 채널용)
def fetch_cumulative_stats_with_api_key(video_map, datekey):
    """videos.list statistics로 영상별 누적값 수집. shares는 Data API 미제공으로 수집 불가."""
    youtube = build("youtube", "v3", developerKey=API_KEY)
    all_data = []
    video_ids = list(video_map.keys())

    # videos.list는 한 번에 최대 50개 처리
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


# pre 테이블에서 일별 delta 계산
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


# BigQuery에 Upsert (Merge) 하는 함수
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


def youtube_etl():
    today = datetime.now(timezone.utc).date()
    yesterday = today - timedelta(days=1)
    end_date             = yesterday.isoformat()
    start_date_data_api  = yesterday.isoformat()           # Data API: 하루치만
    start_date_analytics = (today - timedelta(days=7)).isoformat()  # Analytics API: 7일 재수집 (딜레이 대응)
    print(f"데이터 조회 기간 (Data API)     : {start_date_data_api} ~ {end_date}")
    print(f"데이터 조회 기간 (Analytics API): {start_date_analytics} ~ {end_date}")

    creds_dict = json.loads(CREDENTIALS_JSON) if isinstance(CREDENTIALS_JSON, str) else CREDENTIALS_JSON
    bq_creds = service_account.Credentials.from_service_account_info(creds_dict)
    client = bigquery.Client(project=PROJECT_ID, credentials=bq_creds)

    all_views, all_age_gender, all_comments = [], [], []

    for channel in CHANNELS:
        channel_id       = channel['yt_channel_id']
        handle           = channel.get('handle', '')
        game_code        = int(channel['joyple_game_code'])
        has_analytics    = channel.get('analytics_access', False)
        source_api       = 'youtube_analytics' if has_analytics else 'youtube_data'
        print(f"\n===== 채널 처리 시작: {channel_id} / {handle} (Analytics 권한: {has_analytics}) =====")

        if has_analytics:
            # ── Analytics API 방식 (브랜드 계정 관리자 권한 보유) ──────────────
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
            # ── YouTube Data API 누적값 방식 (Analytics 권한 없음) ───────────────
            # 1) 비디오 목록 조회
            video_map = get_video_map_with_api_key(channel_id)

            # 2) 오늘 누적값 → pre 테이블 적재 (channel_id 구분자)
            df_pre = fetch_cumulative_stats_with_api_key(video_map, end_date)
            df_pre['channel_id']       = channel_id
            df_pre['handle']           = handle
            df_pre['joyple_game_code'] = game_code
            upsert_to_bigquery(client, df_pre, PROJECT_ID, DATASET_ID, views_by_video_id_pre_table_id)
            print(f"Pre table upserted for {channel_id}.")

            # 3) pre 테이블에서 전일 대비 delta 계산 → 메인 테이블용 DataFrame
            df_v = compute_daily_delta_from_pre(client, channel_id, end_date)
            if not df_v.empty:
                df_v['handle']           = handle
                df_v['joyple_game_code'] = game_code
                df_v['source_api']       = source_api
                all_views.append(df_v)

        # 댓글은 모든 채널 공통 (API 키 방식)
        df_c = get_all_comments_with_api_key(video_map)
        df_c['channel_id']       = channel_id
        df_c['handle']           = handle
        df_c['joyple_game_code'] = game_code
        all_comments.append(df_c)

        print(f"===== 채널 처리 완료: {channel_id} =====\n")

    df_views = pd.concat(all_views, ignore_index=True) if all_views else pd.DataFrame()
    df_age_gender = pd.concat(all_age_gender, ignore_index=True) if all_age_gender else pd.DataFrame()
    df_comments_all = pd.concat(all_comments, ignore_index=True) if all_comments else pd.DataFrame()

    # 조회수 데이터 타입 정리
    if not df_views.empty:
        df_views['views']            = pd.to_numeric(df_views['views'],            errors='coerce').fillna(0).astype(int)
        df_views['likes']            = pd.to_numeric(df_views['likes'],            errors='coerce').fillna(0).astype(int)
        df_views['comments']         = pd.to_numeric(df_views['comments'],         errors='coerce').fillna(0).astype(int)
        df_views['shares']           = pd.to_numeric(df_views['shares'],           errors='coerce').fillna(0).astype(int)
        df_views['joyple_game_code'] = pd.to_numeric(df_views['joyple_game_code'], errors='coerce').fillna(0).astype(int)
        df_views['channel_id']       = df_views['channel_id'].astype(str)
        df_views['source_api']       = df_views['source_api'].astype(str)

    # 인구통계 데이터 타입 정리 (Analytics 채널만 존재)
    if not df_age_gender.empty:
        df_age_gender['viewer_percentage'] = pd.to_numeric(df_age_gender['viewer_percentage'], errors='coerce').fillna(0.0).astype(float)
        df_age_gender['age_group']         = df_age_gender['age_group'].astype(str)
        df_age_gender['gender']            = df_age_gender['gender'].astype(str)
        df_age_gender['video_id']          = df_age_gender['video_id'].astype(str)
        df_age_gender['video_title']       = df_age_gender['video_title'].astype(str)
        df_age_gender['channel_id']        = df_age_gender['channel_id'].astype(str)
        df_age_gender['joyple_game_code']  = pd.to_numeric(df_age_gender['joyple_game_code'], errors='coerce').fillna(0).astype(int)
        df_age_gender['datekey']           = pd.to_datetime(df_age_gender['datekey']).dt.date

    # 댓글 데이터 타입 정리
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

    views_mb     = df_views.memory_usage(deep=True).sum() / 1024 / 1024
    age_gender_mb = df_age_gender.memory_usage(deep=True).sum() / 1024 / 1024
    comments_mb  = df_comments_all.memory_usage(deep=True).sum() / 1024 / 1024
    total_mb     = views_mb + age_gender_mb + comments_mb
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

    upsert_to_bigquery(client, df_views, PROJECT_ID, DATASET_ID, views_by_video_id_table_id)
    print("Views by video ID upserted.")

    upsert_to_bigquery(client, df_age_gender, PROJECT_ID, DATASET_ID, views_by_age_gender_table_id)
    print("Views by age/gender upserted.")

    upsert_to_bigquery(client, df_comments_all, PROJECT_ID, DATASET_ID, comments_table_id)
    print("Comments upserted.")


# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
}

with DAG(
    dag_id='youtube_api_ETL',
    default_args=default_args,
    description='유튜브 api 호출하여 BigQuery에 적재',
    schedule='0 8 * * *',  # PT 00:00 기준 (PST=UTC+8, KST 17:00).
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['youtube', 'RESU', 'bigquery'],
) as dag:

    youtube_etl_task = PythonOperator(
        task_id='youtube_etl_task',
        python_callable=youtube_etl,
    )


youtube_etl_task
