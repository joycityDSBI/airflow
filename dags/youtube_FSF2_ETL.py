import time
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
import pandas as pd
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.cloud import bigquery
from datetime import datetime, timedelta, timezone
from google.oauth2 import service_account



def get_var(key: str, default: str = None) -> str:
    """환경 변수 또는 Airflow Variable 조회"""
    return os.environ.get(key) or Variable.get(key, default_var=default)

DAG_FOLDER = os.path.dirname(os.path.realpath(__file__))
TOKEN_FILE = os.path.join(DAG_FOLDER, "youtube_token.json")
CLIENT_SECRETS_FILE = os.path.join(DAG_FOLDER, "youtube_analytics_api_credential.json")

API_KEY = get_var("YOUTUBE_ANALYTICS_API_KEY")

SCOPES = [
    "https://www.googleapis.com/auth/yt-analytics.readonly",
    "https://www.googleapis.com/auth/youtube.readonly"
]

PROJECT_ID = 'datahub-478802'
LOCATION = 'US'

DATASET_ID = 'datahub'
views_by_video_id_table_id = 'youtube_FSF2_views_by_video_id'
views_by_age_gender_table_id = 'youtube_FSF2_views_by_age_gender'
comments_table_id = 'youtube_FSF2_comments'

CREDENTIALS_JSON = get_var('GOOGLE_CREDENTIAL_JSON')



# 인증 및 서비스 빌드 함수
def get_services():
    creds = None
    
    # 1. 기존에 저장된 token.json 파일이 있는지 확인
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    
    # 2. 유효한 인증 정보가 없는 경우 (처음이거나 만료됨)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            # 토큰이 만료되었지만 갱신(Refresh)이 가능한 경우
            print("토큰 갱신 중...")
            creds.refresh(Request())
        else:
            raise Exception("유효한 토큰이 없습니다. 로컬에서 생성한 token.json을 서버에 업로드하세요.")
            # # 아예 처음 인증하는 경우 브라우저 실행 (로컬에서 인증 후 token.json 파일을 서버에 업로드하는 방식으로 변경 )
            # print("새로운 인증 세션 시작...")
            # flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
            # creds = flow.run_local_server(port=0)
                    
        # 3. ★중요★ 인증받은 정보를 파일로 저장
        with open(TOKEN_FILE, "w") as token:
            token.write(creds.to_json())
            print(f"새로운 토큰이 저장되었습니다: {TOKEN_FILE}")

    # 두 가지 서비스 빌드
    data_service = build("youtube", "v3", credentials=creds)
    analytics_service = build("youtubeAnalytics", "v2", credentials=creds)
    
    return data_service, analytics_service


# 채널 내 동영상 리스트 추출
def get_video_id_title_map(data_service):
    """채널의 모든 동영상 ID와 제목을 딕셔너리로 반환"""
    video_map = {}
    # 내 채널 정보에서 '업로드된 동영상' 재생목록 ID 가져오기
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
        if not next_page_token: break
    return video_map


# 각 영상별 일자별 조회수, 좋아요, 댓글 수, 공유 수 조회
def fetch_combined_data(analytics_service, video_map, start_date, end_date):
    all_data = []
    
    for v_id, v_title in video_map.items():
        print(f"조회 중: {v_title} ({v_id})")
        
        # 재시도를 위한 루프 (최대 3번)
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
                
                time.sleep(0.2) # 평소보다 조금 더 여유를 둡니다
                break # 성공 시 재시도 루프 탈출
                
            except Exception as e:
                if "500" in str(e) and attempt < 2:
                    print(f"   ㄴ 서버 에러 발생. {attempt+1}차 재시도 중...")
                    time.sleep(2) # 500 에러 시에는 조금 길게 쉽니다
                else:
                    print(f"에러 발생 ({v_id}): {e}")
                    break # 3번 다 실패하면 다음 영상으로 이동

    columns = ['video_id', 'title', 'date', 'views', 'likes', 'comments', 'shares']
    df = pd.DataFrame(all_data, columns=columns)
    df['datekey'] = pd.to_datetime(df['date']).dt.date # date를 datekey로 복사 및 타입 변환

    return df[['datekey', 'video_id', 'title', 'views', 'likes', 'comments', 'shares']]



# 시청자 성별/연령대 데이터 조회. 누적 값 기준 viewerPercentage
def fetch_combined_data_viewer(analytics_service, start_date, end_date):
    all_data = []
    # 재시도를 위한 루프 (최대 3번)
    for attempt in range(3):
        try:
            request = analytics_service.reports().query(
            ids="channel==MINE",
            startDate=start_date,
            endDate=end_date,
            metrics="viewerPercentage",
            dimensions="ageGroup,gender",
            sort="ageGroup,gender"
            )
            response = request.execute()
            
            if 'rows' in response:
                for row in response['rows']:
                    all_data.append(row)
            
            time.sleep(0.3) # 평소보다 조금 더 여유를 둡니다
            break # 성공 시 재시도 루프 탈출
            
        except Exception as e:
            if "500" in str(e) and attempt < 2:
                print(f"   ㄴ 서버 에러 발생. {attempt+1}차 재시도 중...")
                time.sleep(2) # 500 에러 시에는 조금 길게 쉽니다
            else:
                print(f"에러 발생: {e}")
                break # 3번 다 실패하면 다음 영상으로 이동

    columns = ['age_group', 'gender', 'viewer_percentage']
    df = pd.DataFrame(all_data, columns=columns)
    df['datekey'] = end_date
    return df



## 전체 비디오 댓글 수집 (API 키 방식)
def get_all_comments_with_api_key(video_map):
    # API 키 방식으로 서비스 빌드
    youtube = build("youtube", "v3", developerKey=API_KEY)
    comments_data = []

    for v_id, v_title in video_map.items():
        print(f"전체 수집 중: {v_title} ({v_id})")
        next_page_token = None
        
        while True:
            try:
                # API 호출 (pageToken을 통해 다음 페이지로 이동)
                request = youtube.commentThreads().list(
                    part="snippet",
                    videoId=v_id,
                    maxResults=100,  # 한 번에 가져올 수 있는 최대치
                    textFormat="plainText",
                    pageToken=next_page_token
                )
                response = request.execute()
                
                items = response.get('items', [])
                if not items:
                    break

                for item in items:
                    comment = item['snippet']['topLevelComment']['snippet']
                    
                    # 모든 데이터를 필터 없이 리스트에 추가
                    comments_data.append({
                        'video_id': v_id,
                        'video_title': v_title,
                        'comment_id': item['id'],
                        'author': comment['authorDisplayName'],
                        'text': comment['textDisplay'],
                        'published_at': comment['publishedAt'],
                        'like_count': comment['likeCount']
                    })
                
                # 다음 페이지 토큰 확인 (더 이상 없으면 루프 종료)
                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break
                    
            except Exception as e:
                # 특정 영상에서 에러(댓글 비활성화 등) 발생 시 해당 영상만 건너뜀
                print(f"영상 {v_id} 처리 중 에러 발생: {e}")
                break
        time.sleep(0.2)

    # 루프가 완전히 끝난 후 모든 데이터를 데이터프레임으로 변환
    df = pd.DataFrame(comments_data)

    return df[['comment_id', 'video_id', 'video_title', 'author', 'text', 'published_at', 'like_count']]


## BigQuery에 Upsert (Merge) 하는 함수
def upsert_to_bigquery(client, df, PROJECT_ID, BQ_DATASET_ID, TABLE_ID):
    """
    Dataframe을 BigQuery에 Upsert (Merge) 하는 함수
    Key: datekey, app_id
    """
    if df.empty:
        print("No data to upsert.")
        return

    # [수정 1] 타임스탬프 고정 (변수 재사용)
    timestamp = int(time.time())
    target_table_id = f"{PROJECT_ID}.{BQ_DATASET_ID}.{TABLE_ID}"
    staging_table_name = f"temp_staging_{timestamp}"
    staging_table_id = f"{PROJECT_ID}.{BQ_DATASET_ID}.{staging_table_name}"

    # 2. 데이터프레임을 임시 테이블에 적재
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
        schema=[
                    bigquery.SchemaField("datekey", "DATE"), # datekey는 무조건 DATE로 인식해라!
                ]
    )

    try:
        print(f"Loading data to staging table: {staging_table_id}...")
        # 테이블 적재 (이때 테이블이 생성됨)
        load_job = client.load_table_from_dataframe(df, staging_table_id, job_config=job_config)
        load_job.result() # 대기
        
        # [수정 2] 적재 후 만료 시간 업데이트
        # 이미 생성된 테이블 객체를 가져와서 만료 시간만 업데이트
        table = client.get_table(staging_table_id)
        table.expires = datetime.utcnow() + timedelta(hours=1)
        client.update_table(table, ["expires"]) # BigQuery에 반영
        
        # 3. MERGE 쿼리 생성
        columns = [col for col in df.columns]
        
        # [수정 3] 컬럼명에 Backtick(`) 추가하여 예약어 충돌 방지
        # 테이블별 PK(Primary Key) 정의
        pk_map = {
            'youtube_FSF2_views_by_video_id': ['datekey', 'video_id'],
            'youtube_FSF2_views_by_age_gender': ['datekey', 'age_group', 'gender'],
            'youtube_FSF2_comments': ['comment_id', 'video_id']
        }
        pks = pk_map.get(TABLE_ID, ['datekey']) # 기본값 설정
        # PK가 아닌 컬럼들만 업데이트 대상으로 지정
        update_set = ", ".join([f"T.`{col}` = S.`{col}`" for col in columns if col not in pks])

        insert_cols = ", ".join([f"`{col}`" for col in columns])
        insert_values = ", ".join([f"S.`{col}`" for col in columns])

        if TABLE_ID == 'youtube_FSF2_views_by_video_id':
            merge_query = f"""
            MERGE `{target_table_id}` T
            USING `{staging_table_id}` S
            ON T.datekey = S.datekey AND T.video_id = S.video_id
            
            WHEN MATCHED THEN
                UPDATE SET {update_set}
                
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols})
                VALUES ({insert_values})
            """
        elif TABLE_ID == 'youtube_FSF2_views_by_age_gender':
            merge_query = f"""
            MERGE `{target_table_id}` T
            USING `{staging_table_id}` S
            ON T.datekey = S.datekey AND T.age_group = S.age_group AND T.gender = S.gender
            
            WHEN MATCHED THEN
                UPDATE SET {update_set}
            
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols})
                VALUES ({insert_values})
            """
        elif TABLE_ID == 'youtube_FSF2_comments':
            merge_query = f"""
            MERGE `{target_table_id}` T
            USING `{staging_table_id}` S
            ON T.comment_id = S.comment_id AND T.video_id = S.video_id
            
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
        # [수정 4] 일관된 ID로 삭제 시도
        print(f"Dropping staging table: {staging_table_id}")
        client.delete_table(staging_table_id, not_found_ok=True)


def youtube_FSF2_etl():
    data_svc, ana_svc = get_services()
    video_map = get_video_id_title_map(data_svc)
    today = datetime.now(timezone.utc).date()
    start_date = (today - timedelta(days=7)).isoformat() # 7일 전부터 오늘까지
    end_date = today.isoformat()    
    print(f"데이터 조회 기간: {start_date} ~ {end_date}")

    start_date = '2025-01-01' # 마이그레이션을 위해 일단 고정값으로 변경 (누적 데이터)
    
    df_views_by_video = fetch_combined_data(ana_svc, video_map, start_date, end_date)
    df_views_by_age_gender = fetch_combined_data_viewer(ana_svc, '2025-01-01', end_date) # 여긴 start_date 고정 (누적 데이터)
    df_comments = get_all_comments_with_api_key(video_map)
    
    bq_creds = service_account.Credentials.from_service_account_info(CREDENTIALS_JSON)
    client = bigquery.Client(project=PROJECT_ID, credentials=bq_creds)
    
    upsert_to_bigquery(client, df_views_by_video, PROJECT_ID, DATASET_ID, views_by_video_id_table_id)
    print("Views by video ID upserted.")
    
    upsert_to_bigquery(client, df_views_by_age_gender, PROJECT_ID, DATASET_ID, views_by_age_gender_table_id)
    print("Views by age/gender upserted.")
    
    upsert_to_bigquery(client, df_comments, PROJECT_ID, DATASET_ID, comments_table_id)
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
    dag_id='youtube_FSF2_ETL',
    default_args=default_args,
    description='유튜브 데이터를 가져와 BigQuery에 적재',
    schedule= '0 20 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['youtube', 'FSF2', 'bigquery'],
) as dag:


    youtube_FSF2_etl_task = PythonOperator(
        task_id='youtube_FSF2_etl_task',
        python_callable=youtube_FSF2_etl,
    )


youtube_FSF2_etl_task