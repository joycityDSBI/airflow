# Airflow function
from multiprocessing import context
from airflow.models import Variable

from google.cloud import bigquery
from datetime import datetime, timedelta
import pytz
import json
from google.oauth2 import service_account



PROJECT_ID = "datahub-478802"
LOCATION = "US"


def get_gcp_credentials():
    """Airflow Variable에서 GCP 자격 증명을 로드합니다."""
    credentials_json = Variable.get('GOOGLE_CREDENTIAL_JSON')
    cred_dict = json.loads(credentials_json)
    if 'private_key' in cred_dict:
        cred_dict['private_key'] = cred_dict['private_key'].replace('\\n', '\n')
    
    # [수정] 스코프(Scopes)를 명시적으로 여러 개 추가합니다.
    SCOPES = [
        "https://www.googleapis.com/auth/cloud-platform",       # 기본 전체 권한
        "https://www.googleapis.com/auth/bigquery"             # BigQuery 권한
    ]
    
    return service_account.Credentials.from_service_account_info(
        cred_dict,
        scopes=SCOPES
    )


def init_clients():
    """Task 내부에서 실행되어 필요한 클라이언트들을 생성하여 반환합니다."""
    creds = get_gcp_credentials()
    
    # 1. GCP Clients
    bq_client = bigquery.Client(project=PROJECT_ID, credentials=creds)
    
    return {
        "bq_client": bq_client
    }

def calc_target_date(logical_date):
    """
    [핵심 로직]
    Airflow 실행 시점(logical_date)을 KST로 변환한 후,
    '하루 전(Yesterday)' 날짜를 계산하여 리스트 형태로 반환합니다.
    """
    utc = pytz.utc
    kst = pytz.timezone('Asia/Seoul')
    
    # 1. UTC 실행 시간을 KST로 변환
    run_date_kst = logical_date.replace(tzinfo=utc).astimezone(kst)
    
    # 2. KST 기준 하루 전 날짜 계산 (Yesterday)
    target_d = run_date_kst.date()
    
    # 문자열로 변환하여 return 
    target_date_str = target_d.strftime("%Y-%m-%d")
    
    return [target_date_str], run_date_kst


## 날짜가 포함되어 있으면 해당 날짜의 데이터를 밀어 넣게 되어 있음
def target_date_range(start_date_str, end_date_str):
    """날짜 데이터 백필용"""
    # 문자열을 datetime 객체로 변환
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    date_list = []
    current_date = start_date
    
    # 종료 날짜까지 하루씩 더하며 리스트에 추가
    while current_date <= end_date:
        date_list.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)
        
    return date_list