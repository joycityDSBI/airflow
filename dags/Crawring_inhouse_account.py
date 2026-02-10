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

WWMC_SPREADSHEET_ID = '1D7WghN05AOW6HRNscOnjW9JJ4P2-uWlGDK8bMcoAqKk'
WWMC_SHEET_NAME = 'TEST_ACCOUNT'

DRSG_SPREADSHEET_ID = '1CRbDxfF8pdGPxcvY-1-LHwsrN4xfXu-7LoEfce6_6-U'
DRSG_SHEET_NAME = 'TEST_ACCOUNT'

POTC_SPREADSHEET_ID = '16nZ8P-cxlARLoHwtXxDCr_awpqi9mCKG1R2s9AyYKkk'
POTC_SHEET_NAME = 'TEST_ACCOUNT' ### 시트가 잠금이 된 상태

GBTW_SPREADSHEET_ID = '1kLyYB1xZUzj1VPq8u123gMrWtB4GGYsVhGMQYft-b30'
GBTW_SHEET_NAME_1 = 'GW 1,2월드 마스터즈 관리'
GBTW_SHEET_NAME_2 = 'GW 3월드 마스터즈 관리'
GBTW_SHEET_NAME_3 = 'GW 외주사 마스터즈 관리'
GBTW_SHEET_NAME_4 = '비정상 이용자 제재 조치'


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



#################### WWMC 인하우스 계정 ETL 함수 #####################
def WWMC_from_spreadsheet_df(spreadsheet_id, sheet_name):

    creds = get_gcp_credentials()
    client = gspread.authorize(creds)

    doc = client.open_by_key(spreadsheet_id)
    sheet = doc.worksheet(sheet_name)
    all_data = sheet.get('A:D')

    if not all_data:
        print("데이터가 없습니다.")
        return pd.DataFrame()

    header = all_data[1]
    data = all_data[2:]

    if len(header) == 1:
        header = [f'col_{i}' for i in range(len(data[0]))]

    df = pd.DataFrame(data, columns=header)

    df = df.rename(columns={
        "build":"build",
        "userkey":"userkey",
        "charid":"charid",
        "type":"class"
        })

    selected_df = df[["build",
                      "userkey",
                      "charid",
                      "class"
                      ]]
    
    return selected_df


def WWMC_merge_to_bigquery(project_id, dataset_id, table_id):

    df = WWMC_from_spreadsheet_df(WWMC_SPREADSHEET_ID, WWMC_SHEET_NAME)
    credentials = get_gcp_credentials()
    client = bigquery.Client(project=project_id, credentials=credentials)
    table_full_id = f"{project_id}.{dataset_id}.{table_id}"

    # 1. 데이터 비우기 (테이블 스키마/설정 유지)
    truncate_query = f"TRUNCATE TABLE `{table_full_id}`"
    client.query(truncate_query, location=LOCATION).result()
    print(f"🗑️ {table_full_id} 데이터가 초기화되었습니다.")
    
    # 2. 데이터 타입 클리닝 (Parquet 변환 에러 방지)
    df_final = df.astype(str)
    
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
    

#################### DS 인하우스 계정 ETL 함수 #####################
def DRSG_from_spreadsheet_df(spreadsheet_id, sheet_name):

    creds = get_gcp_credentials()
    client = gspread.authorize(creds)

    doc = client.open_by_key(spreadsheet_id)
    sheet = doc.worksheet(sheet_name)
    all_data = sheet.get('A:E')

    if not all_data:
        print("데이터가 없습니다.")
        return pd.DataFrame()

    header = all_data[0]
    data = all_data[1:]

    if len(header) == 1:
        header = [f'col_{i}' for i in range(len(data[0]))]

    df = pd.DataFrame(data, columns=header)

    df = df.rename(columns={
        "빌드":"build",
        "서버명":"worldid",
        "계정번호":"charid",
        "구분":"class",
        "회원번호":"userkey"
        })

    selected_df = df[["build",
                      "userkey",
                      "charid",
                      "class",
                      "worldid"
                      ]]
    
    return selected_df


def DRSG_merge_to_bigquery(project_id, dataset_id, table_id):

    df = DRSG_from_spreadsheet_df(DRSG_SPREADSHEET_ID, DRSG_SHEET_NAME)
    credentials = get_gcp_credentials()
    client = bigquery.Client(project=project_id, credentials=credentials)
    table_full_id = f"{project_id}.{dataset_id}.{table_id}"

    # 1. 데이터 비우기 (테이블 스키마/설정 유지)
    truncate_query = f"TRUNCATE TABLE `{table_full_id}`"
    client.query(truncate_query, location=LOCATION).result()
    print(f"🗑️ {table_full_id} 데이터가 초기화되었습니다.")
    
    # 2. 데이터 타입 클리닝 (Parquet 변환 에러 방지)
    df_final = df.astype(str)
    
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


#################### GBTW 인하우스 계정 ETL 함수 #####################
def GBTW_from_spreadsheet_df(spreadsheet_id, sheet_name_1, sheet_name_2, sheet_name_3, sheet_name_4):

    creds = get_gcp_credentials()
    client = gspread.authorize(creds)

    #### 시트 1 : GW 1,2월드 마스터즈 관리
    doc = client.open_by_key(spreadsheet_id)
    sheet1 = doc.worksheet(sheet_name_1)
    all_data_1 = sheet1.get('C:E')

    if not all_data_1:
        print("데이터가 없습니다.")
        return pd.DataFrame()

    header = all_data_1[1]
    data = all_data_1[2:]

    if len(header) == 1:
        header = [f'col_{i}' for i in range(len(data[0]))]

    df1 = pd.DataFrame(data, columns=header)

    df1 = df1.rename(columns={
        "회원번호(Userkey) ":"userkey",
        "계정번호(UserID)":"charid",
        "구분":"class"
        })
    df1['world'] = 'GBTW'

    selected_df1 = df1[["world",
                    "userkey",
                    "charid",
                    "class"
                    ]]
    
    #### 시트 2 : GW 3월드 마스터즈 관리
    sheet2 = doc.worksheet(sheet_name_2)
    all_data_2 = sheet2.get('C:E')

    if not all_data_2:
        print("데이터가 없습니다.")
        return pd.DataFrame()

    header = all_data_2[1]
    data = all_data_2[2:]

    if len(header) == 1:
        header = [f'col_{i}' for i in range(len(data[0]))]
    
    df2 = pd.DataFrame(data, columns=header)

    df2 = df2.rename(columns={
        "회원번호(Userkey) ":"userkey",
        "계정번호(UserID)":"charid",
        "구분":"class"
        })
    df2['world'] = 'GBTW'

    selected_df2 = df2[["world",
                "userkey",
                "charid",
                "class"
                ]]


    #### 시트 3 : GW 외주사 마스터즈 관리
    sheet3 = doc.worksheet(sheet_name_3)
    all_data_3 = sheet3.get('C:E')

    if not all_data_3:
        print("데이터가 없습니다.")
        return pd.DataFrame()

    header = all_data_3[1]
    data = all_data_3[2:]

    if len(header) == 1:
        header = [f'col_{i}' for i in range(len(data[0]))]
    
    df3 = pd.DataFrame(data, columns=header)

    df3 = df3.rename(columns={
        "회원번호(Userkey) ":"userkey",
        "계정번호(UserID)":"charid",
        "구분":"class"
        })
    df3['world'] = 'GBTW'

    selected_df3 = df3[["world",
            "userkey",
            "charid",
            "class"
            ]]


    #### 시트 4 : 비정상 이용자 제재 조치
    sheet4 = doc.worksheet(sheet_name_4)
    all_data_4 = sheet4.get('A:F')

    if not all_data_4:
        print("데이터가 없습니다.")
        return pd.DataFrame()

    header = all_data_4[0]
    data = all_data_4[1:]

    if len(header) == 1:
        header = [f'col_{i}' for i in range(len(data[0]))]
    
    df4 = pd.DataFrame(data, columns=header)

    df4 = df4.rename(columns={
        "회원번호":"userkey",
        "제독번호":"charid",
        "제재 처리 유형":"class"
        })
    df4['world'] = 'GBTW'

    selected_df4 = df4[["world",
        "userkey",
        "charid",
        "class"
        ]]


    selected_df = pd.concat([selected_df1, selected_df2, selected_df3, selected_df4], ignore_index=True)
    
    return selected_df


def GBTW_merge_to_bigquery(project_id, dataset_id, table_id):

    df = GBTW_from_spreadsheet_df(GBTW_SPREADSHEET_ID, GBTW_SHEET_NAME_1, GBTW_SHEET_NAME_2, GBTW_SHEET_NAME_3, GBTW_SHEET_NAME_4)
    credentials = get_gcp_credentials()
    client = bigquery.Client(project=project_id, credentials=credentials)
    table_full_id = f"{project_id}.{dataset_id}.{table_id}"

    # 1. 데이터 비우기 (테이블 스키마/설정 유지)
    truncate_query = f"TRUNCATE TABLE `{table_full_id}`"
    client.query(truncate_query, location=LOCATION).result()
    print(f"🗑️ {table_full_id} 데이터가 초기화되었습니다.")
    
    # 2. 데이터 타입 클리닝 (Parquet 변환 에러 방지)
    df_final = df.astype(str)
    
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
    dag_id='inhouse_account_ETL',
    default_args=default_args,
    description='Inhouse Account ETL',
    schedule='30 19 * * *',  # 매일 오전 04시 50분 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['ETL', 'inhouse_account', 'bigquery'],
) as dag:

    WWMC_inhouse_account_task = PythonOperator(
        task_id='WWMC_inhouse_account_task',
        python_callable=WWMC_merge_to_bigquery,
        op_kwargs={
            "project_id": "data-science-division-216308",
            "dataset_id": "Account_Info",
            "table_id": "WWM_account_info"
        },
        dag=dag,
    )

    DRSG_inhouse_account_task = PythonOperator(
        task_id='DRSG_inhouse_account_task',
        python_callable=DRSG_merge_to_bigquery,
        op_kwargs={
            "project_id": "data-science-division-216308",
            "dataset_id": "Account_Info",
            "table_id": "DS_account_info"
        },
        dag=dag,
    )

    GBTW_inhouse_account_task = PythonOperator(
        task_id='GBTW_inhouse_account_task',
        python_callable=GBTW_merge_to_bigquery,
        op_kwargs={
            "project_id": "data-science-division-216308",
            "dataset_id": "Account_Info",
            "table_id": "GW_account_info"
        },
        dag=dag,
    )

    WWMC_inhouse_account_task >> DRSG_inhouse_account_task >> GBTW_inhouse_account_task