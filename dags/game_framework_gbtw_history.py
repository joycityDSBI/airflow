import requests
import pandas as pd
from google.cloud import bigquery
import os
import logging
import json
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.models import Variable
from google.oauth2 import service_account

def get_var(key: str, default: str = None, required: bool = False) -> str:
    """환경 변수 → Airflow Variable 순서로 조회"""
    env_value = os.environ.get(key)
    if env_value:
        print(f"✓ 환경 변수에서 {key} 로드됨")
        return env_value
    
    try:
        try:
            var_value = Variable.get(key, default=None)
        except TypeError:
            var_value = Variable.get(key, default_var=None)
        
        if var_value:
            print(f"✓ Airflow Variable에서 {key} 로드됨")
            return var_value
    except Exception as e:
        print(f"⚠️  Variable.get({key}) 오류: {str(e)}")
    
    if default is not None:
        print(f"ℹ️  기본값으로 {key} 설정됨: {default}")
        return default
    
    if required:
        raise ValueError(f"필수 설정 {key}을(를) 찾을 수 없습니다.")
    
    print(f"ℹ️  {key} 값을 찾을 수 없습니다 (선택사항)")
    return None


## 환경변수 설정
NOTION_TOKEN = get_var('NOTION_TOKEN_MS') # MS 팀 API 키
# DBID = "24eea67a568181b7b76ecf3886e624c2"
PROJECT_ID = "data-science-division-216308"
DATASET_ID = "gameInsightFramework"
TABLE_ID = "GBTW_history"
NOTION_API_VERSION = "2022-06-28"
DBID = "24eea67a568181c88be2fccd76608551"

# ---------------------------------------------------------
# 2. 유틸리티 함수 (클라이언트 생성 및 검증)
# ---------------------------------------------------------
def get_gcp_credentials():
    """Airflow Variable에서 GCP 자격 증명을 로드합니다."""
    credentials_json = Variable.get('GOOGLE_CREDENTIAL_JSON')
    cred_dict = json.loads(credentials_json)
    if 'private_key' in cred_dict:
        cred_dict['private_key'] = cred_dict['private_key'].replace('\\n', '\n')
    
    # [수정] 스코프(Scopes)를 명시적으로 여러 개 추가합니다.
    SCOPES = [
        "https://www.googleapis.com/auth/cloud-platform",       # 기본 전체 권한
        "https://www.googleapis.com/auth/devstorage.read_write", # GCS 업로드 필수 권한
        "https://www.googleapis.com/auth/bigquery",             # BigQuery 권한
        "https://www.googleapis.com/auth/drive"                 # (혹시 모를) 드라이브 권한
    ]
    
    return service_account.Credentials.from_service_account_info(
        cred_dict,
        scopes=SCOPES
    )

def truncate_table_bq(project_id: str, dataset_id: str, table_id: str):
    """BigQuery 테이블의 모든 데이터를 삭제합니다."""
    creds = get_gcp_credentials()
    
    # BigQuery 클라이언트 생성
    client = bigquery.Client(project=project_id, credentials=creds)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    
    query = f"TRUNCATE TABLE `{table_ref}`"
    
    query_job = client.query(query)
    query_job.result()  # 쿼리 완료 대기
    
    print(f"✓ BigQuery 테이블 {table_ref}의 모든 데이터가 삭제되었습니다.")



def query_notion_database(notion_token, dbid, notion_version):
  # 노션 API 엔드포인트 URL
    url = f"https://api.notion.com/v1/databases/{dbid}/query"
  # API 요청에 필요한 헤더 (인증토큰, 버전, 데이터타입)
    headers = {
        "Authorization": f"Bearer {notion_token}",
        "Notion-Version": notion_version,
        "Content-Type": "application/json"
    }

    results = []       # 조회된 데이터 저장할 리스트
    has_more = True    # 다음 페이지가 있는지 여부
    next_cursor = None # 다음 페이지 시작 위치
  # 데이터가 더 없을때까지 반복
    while has_more:
        # start cursor 가 있으면 다음페이지 요청 없으면 첫 페이지 요청
        payload = {"start_cursor": next_cursor} if next_cursor else {}
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
        results.extend(data["results"])
        has_more = data.get("has_more", False)
        next_cursor = data.get("next_cursor")

    return results

def query_notion_database(notion_token, dbid, notion_version):
  # 노션 API 엔드포인트 URL
    url = f"https://api.notion.com/v1/databases/{dbid}/query"
  # API 요청에 필요한 헤더 (인증토큰, 버전, 데이터타입)
    headers = {
        "Authorization": f"Bearer {notion_token}",
        "Notion-Version": notion_version,
        "Content-Type": "application/json"
    }

    results = []       # 조회된 데이터 저장할 리스트
    has_more = True    # 다음 페이지가 있는지 여부
    next_cursor = None # 다음 페이지 시작 위치
  # 데이터가 더 없을때까지 반복
    while has_more:
        # start cursor 가 있으면 다음페이지 요청 없으면 첫 페이지 요청
        payload = {"start_cursor": next_cursor} if next_cursor else {}
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
        results.extend(data["results"])
        has_more = data.get("has_more", False)
        next_cursor = data.get("next_cursor")

    return results


### 2> Notion 속성값을 타입에 맞게 추출
def extract_property_value(value):
    prop_type = value["type"]
    try:
        if prop_type == "title": # 제목 타입
            return value.get("title", [{}])[0].get("text", {}).get("content")
        elif prop_type == "rich_text": # 일반 텍스트
            return value.get("rich_text", [{}])[0].get("text", {}).get("content")
        elif prop_type == "select": # 단일 선택
            return value.get("select", {}).get("name")
        elif prop_type == "multi_select": # 다중 선택
            return [m["name"] for m in value.get("multi_select", [])]
        elif prop_type == "status": # 상태값
            return value.get("status", {}).get("name")
        elif prop_type == "date": # 날짜
            return value.get("date", {}).get("start")
        elif prop_type == "checkbox":  # 체크박스
            return value.get("checkbox")
        elif prop_type == "number": # 숫자
            return value.get("number")
        elif prop_type == "people": # 사람(담당자)
            return [p.get("name", p.get("id")) for p in value.get("people", [])]
        elif prop_type == "relation": # 관계형 데이터
            return [r.get("id") for r in value.get("relation", [])]
        elif prop_type == "email": # 이메일
            return value.get("email")
        elif prop_type == "phone_number":  # 전화번호
            return value.get("phone_number")
        elif prop_type == "url":  # URL
            return value.get("url")
        elif prop_type == "formula": # 수식 결과
            return value.get("formula", {}).get("string")
        elif prop_type == "files": # 파일
            return [f["name"] for f in value.get("files", [])]
        else: # 알 수 없는 타입
            return None
    except Exception:
        return None

### 3> Notion에서 가져온 데이터 -> 평탄화(Flat) 형태로 변환
def parse_flat(rows):
    parsed = []
    for row in rows:
        # 각 페이지 ID 저장
        row_data = {"notion_page_id": row.get("id")}
        props = row.get("properties", {})

        # 속성별 값 추출
        for key, value in props.items():
            row_data[key] = extract_property_value(value)

        # "상품 카인드"가 빈 경우 제외 -> 실 서비스에서는 사용해야 하는 코드
        # if not row_data.get("상품카인드"):
        #     continue

        parsed.append(row_data)
    return parsed

### 4> 변환된 데이터프레임을 BigQuery로 업로드
def upload_to_bigquery(df, project_id, dataset_id, table_id):
     # BigQuery 클라이언트 생성
    client = bigquery.Client(project=project_id)

    # 완전한 테이블 ID
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"

    # 데이터 업로드 작업 실행
    job = client.load_table_from_dataframe(
        dataframe=df,
        destination=full_table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",  # append 형태 #(참고:덮어쓰기는WRITE_TRUNCATE)
            autodetect=True                      # 스키마 자동 감지
        )
    )

    # 업로드 완료까지 대기
    job.result()
    logging.info(f"✓ BigQuery 테이블 업로드 완료: {full_table_id}")

def notion_to_bigquery():
    rows = query_notion_database(NOTION_TOKEN, DBID, NOTION_API_VERSION)
    parsed_rows = parse_flat(rows)
    df = pd.DataFrame(parsed_rows)
    df.columns = df.columns.str.strip()

    ## 원하는 컬럼만 가져오기
    df = df[['공개','대상 월드','패치 일자','상태','시작일','종료일','분류','제목','이벤트 카테고리','자체 결제']] ## 10개

    ## 컬럼명 영어로 변경
    df = df.rename(columns={
            '공개': 'isPublic',
            '대상 월드': 'world',
            '패치 일자': 'updateDate',
            '상태': 'status',
            '시작일': 'startDate',
            '종료일': 'endDate',
            '분류': 'category',
            '제목': 'title',
            '이벤트 카테고리' : 'eventCategory',
            '자체 결제' : 'isSelfPayment'
        })
    
    ### 변환된 데이터프레임을 BigQuery로 업로드
    upload_to_bigquery(df, PROJECT_ID, DATASET_ID, TABLE_ID)
    print("✅ Notion 데이터가 BigQuery로 업로드되었습니다.")


default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=10),
}

with DAG(
    dag_id='game_framework_gbtw_history',
    default_args=default_args,
    description='Process Databricks audit logs for aibiGenie',
    schedule='0 20 * * *',  # 매일 아침 9시 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['databricks', 'audit', 'genie'],
) as dag:
    


    truncate_bq_task = PythonOperator(
        task_id='truncate_bq_table',
        python_callable=truncate_table_bq,
        op_kwargs={
            'project_id': PROJECT_ID,
            'dataset_id': DATASET_ID,
            'table_id': TABLE_ID
        }
    )

    notion_to_bq_task = PythonOperator(
        task_id='notion_to_bigquery',
        python_callable=notion_to_bigquery
    )



    truncate_bq_task >> notion_to_bq_task

    