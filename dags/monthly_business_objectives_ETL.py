import gspread
import pandas as pd
import os
from airflow.models import Variable
from google.oauth2.service_account import Credentials
from google.oauth2 import service_account
from google.cloud import bigquery
import json
import pandas_gbq # 최신 방식 권장
from datetime import datetime, timedelta
from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
import requests
import time

MONTHLY_GOAL_SPREADSHEET_ID = '1FV9zhX8-A3WG4wgmwURY7stYL43jOoHyY-VUwg4Iq60'
MONTHLY_GOAL_SHEET_NAME = '시트1'

PROJECT_ID = "datahub-478802"
LOCATION = "US"

################### 유틸함수 #####################
def get_gcp_credentials():
    """Airflow Variable에서 GCP 자격 증명을 로드합니다."""
    credentials_json = Variable.get('GOOGLE_CREDENTIAL_JSON')
    cred_dict = json.loads(credentials_json)
    if 'private_key' in cred_dict:
        cred_dict['private_key'] = cred_dict['private_key'].replace('\\n', '\n')
    
    # [수정] 스코프(Scopes)를 명시적으로 여러 개 추가합니다.
    SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive',
        "https://www.googleapis.com/auth/cloud-platform",
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


# 월간 목표 dataframe 으로 가져오는 함수
def mothly_goal_get_gsheet_to_df(spreadsheet_id, sheet_name):
    # 1. 인증 설정 (서비스 계정 키 파일 경로)

    creds = get_gcp_credentials()
    client = gspread.authorize(creds)

    # 2. 스프레드시트 및 시트 오픈
    doc = client.open_by_key(spreadsheet_id)
    sheet = doc.worksheet(sheet_name)

    # 3. 데이터 가져오기 (A~K 열의 전체 데이터)
    # get_all_values()는 전체 데이터를 가져오지만 리스트 형태이므로 슬라이싱이 필요합니다.
    all_data = sheet.get('A:G')

    if not all_data:
        print("데이터가 없습니다.")
        return pd.DataFrame()

    # 에러 방지 로직: 헤더와 데이터 분리
    header = all_data[0]
    data = all_data[1:]

    # 만약 첫 번째 행(헤더)의 길이가 데이터 열 개수와 맞지 않는 경우 처리
    if len(header) == 1:
        # 헤더가 1개인데 데이터가 11개라면, 헤더 리스트가 제대로 안 만들어진 것임
        # 임의의 컬럼명을 부여하거나 데이터를 다시 확인해야 함
        header = [f'col_{i}' for i in range(len(data[0]))]

    df = pd.DataFrame(data, columns=header)

    df = df.rename(columns={
        "month":"year_month",
        "idx":"game_idx",
        "sales":"sales",
        "mru":"mru",
        "dau":"dau",
        "cost":"cost",
        "num_of_days":"num_of_days"
        })
    
    return df


def monthly_goal_truncate_and_insert_to_bigquery(project_id, dataset_id, table_id):

    df = mothly_goal_get_gsheet_to_df(MONTHLY_GOAL_SPREADSHEET_ID, MONTHLY_GOAL_SHEET_NAME)
    
    credentials = get_gcp_credentials()
    client = bigquery.Client(project=project_id, credentials=credentials)
    table_full_id = f"{project_id}.{dataset_id}.{table_id}"

    # 1. 데이터 비우기 (테이블 스키마/설정 유지)
    truncate_query = f"TRUNCATE TABLE `{table_full_id}`"
    client.query(truncate_query, location=LOCATION).result()
    print(f"🗑️ {table_full_id} 데이터가 초기화되었습니다.")
    
    # 2. 데이터 타입 클리닝 (Parquet 변환 에러 방지)
    df_final = df.astype(str)
    df_final = df_final.replace(['None', 'nan', 'NaN', '<NA>'], '')

    to_int_list = ['sales', 'mru', 'dau', 'cost', 'num_of_days']
    for cl in to_int_list:
        df_final[cl] = df_final[cl].str.replace(r'[₩,]', '', regex=True)
        df_final[cl] = pd.to_numeric(df_final[cl], errors='coerce')
        df_final[cl] = df_final[cl].fillna(0).astype(int)
    
    # 3. 데이터 삽입
    try:
        # TRUNCATE를 미리 했으므로 'append'를 써야 기존 스키마/파티션 설정이 유지됩니다.
        # df.to_gbq 대신 pandas_gbq.to_gbq 사용 권장
        pandas_gbq.to_gbq(
            df_final,
            destination_table=f"{dataset_id}.{table_id}",
            project_id=project_id,
            if_exists='append', 
            progress_bar=True,
            credentials=credentials
        )
        print(f"✅ {len(df_final)}행 데이터가 {table_full_id}에 성공적으로 Insert 되었습니다.")
    except Exception as e:
        print(f"❌ BigQuery 업로드 중 에러 발생: {e}")
        raise e # 에러 추적을 위해 raise 추가
    

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
}

with DAG(
    dag_id='monthly_goal_ETL',
    default_args=default_args,
    description='monthly_goal ETL',
    schedule='00 19 * * *',  # 매일 오전 04시 00분 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['ETL', 'monthly_goal', 'bigquery'],
) as dag:

    monthly_goal_task = PythonOperator(
        task_id='monthly_goal_task',
        python_callable=monthly_goal_truncate_and_insert_to_bigquery,
        op_kwargs={
            "project_id": "datahub-478802",
            "dataset_id": "datahub",
            "table_id": "statics_monthly_goal"
        },
        dag=dag,
    )

    monthly_goal_task