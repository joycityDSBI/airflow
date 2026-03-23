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
from airflow.providers.standard.operators.python import PythonOperator
import requests
import time

def get_var(key: str, default: str | None = None) -> str:
    """환경 변수 또는 Airflow Variable 조회"""
    return os.environ.get(key) or Variable.get(key, default_var=default)

# 시트정보
GBTW_SPREADSHEET_ID = '1mnsTzSupPOBhtk-rZSnxk4oALPqTDGd3vkGfS7gt8z0'
GBTW_SHEET_NAME = '상품요약(신)'

POTC_SPREADSHEET_ID = '121hBk4DKpD2Wfzd59hCPUtVu7zqibmiKhxAr8UkZWJQ'
POTC_SHEET_NAME = '메타데이터'

DRSG_SPREADSHEET_ID = '1CRbDxfF8pdGPxcvY-1-LHwsrN4xfXu-7LoEfce6_6-U'
DRSG_SHEET_NAME = '메타데이터'

WWMC_SPREADSHEET_ID = '1D7WghN05AOW6HRNscOnjW9JJ4P2-uWlGDK8bMcoAqKk'
WWMC_SHEET_NAME = 'PACKAGE_HISTORY'

NOTION_TOKEN = get_var("NOTION_TOKEN")
NOTION_DATABASE_ID = "23bea67a5681803db3c4f691c143a43d"


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


#################### GBTW 패키지 정보 ETL 함수 #####################
def GBTW_get_gsheet_to_df(spreadsheet_id, sheet_name):
    # 1. 인증 설정 (서비스 계정 키 파일 경로)

    creds = get_gcp_credentials()
    client = gspread.authorize(creds)

    # 2. 스프레드시트 및 시트 오픈
    doc = client.open_by_key(spreadsheet_id)
    sheet = doc.worksheet(sheet_name)

    # 3. 데이터 가져오기 (A~K 열의 전체 데이터)
    # get_all_values()는 전체 데이터를 가져오지만 리스트 형태이므로 슬라이싱이 필요합니다.
    all_data = sheet.get('A:Z')

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
        "재화구분":"goods_type",
        "상점 카테고리":"category_shop",
        "상품 카테고리":"category_package",
        "패키지명":"package_name",
        "PackageKind":"package_kind",
        "가격":"price",
        "가격(구글)":"price_aos",
        "가격(애플_국내)":"price_ios",
        "가격(원스토어)":"price_one",
        "IAP코드(구글)":"iap_code_google",
        "IAP코드(애플)":"iap_code_apple",
        "IAP코드(원스토어)":"iap_code_one",
        "상품반영일":"sale_date_start",
        "판매종료일":"sale_date_end"
        })

    selected_df = df[["goods_type",
        "category_shop",
        "category_package",
        "package_name",
        "package_kind",
        "price",
        "price_aos",
        "price_ios",
        "price_one",
        "iap_code_google",
        "iap_code_apple",
        "iap_code_one",
        "sale_date_start",
        "sale_date_end"]]
    
    return selected_df


def GBTW_truncate_and_insert_to_bigquery(project_id, dataset_id, table_id):

    df = GBTW_get_gsheet_to_df(GBTW_SPREADSHEET_ID, GBTW_SHEET_NAME)
    
    credentials = get_gcp_credentials()
    client = bigquery.Client(project=project_id, credentials=credentials)
    table_full_id = f"{project_id}.{dataset_id}.{table_id}"

    # 1. 데이터 비우기 (테이블 스키마/설정 유지)
    truncate_query = f"TRUNCATE TABLE `{table_full_id}`"
    client.query(truncate_query, location=LOCATION).result()
    print(f"🗑️ {table_full_id} 데이터가 초기화되었습니다.")
    
    # 2. 데이터 타입 클리닝 (Parquet 변환 에러 방지)
    df_final = df.astype(str)
    numeric_columns = ['package_kind'] # 실제 숫자형 컬럼 리스트로 수정하세요
    for col in numeric_columns:
        if col in df_final.columns:
            # 숫자로 변환하되, 변환 안되는 값(빈칸 등)은 NaN으로 처리 후 0으로 채움
            df_final[col] = pd.to_numeric(df_final[col], errors='coerce').fillna(0).astype(int)
    
    df_final = df_final.replace(['None', 'nan', 'NaN', '<NA>'], '')
    df_final['price_ios_china'] = None

    to_int_list = ['price', 'price_aos', 'price_ios', 'price_one']
    for cl in to_int_list:
        df_final[cl] = df_final[cl].str.replace(r'[₩,]', '', regex=True)
        df_final[cl] = pd.to_numeric(df_final[cl], errors='coerce')
        df_final[cl] = df_final[cl].fillna(0).astype(str)
    
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


#################### POTC 패키지 정보 ETL 함수 #####################
def POTC_get_gsheet_to_df(spreadsheet_id, sheet_name):

    creds = get_gcp_credentials()
    client = gspread.authorize(creds)

    doc = client.open_by_key(spreadsheet_id)
    sheet = doc.worksheet(sheet_name)
    all_data = sheet.get('A:K')

    if not all_data:
        print("데이터가 없습니다.")
        return pd.DataFrame()

    header = all_data[1]
    data = all_data[2:]

    if len(header) == 1:
        header = [f'col_{i}' for i in range(len(data[0]))]

    df = pd.DataFrame(data, columns=header)

    df = df.rename(columns={
        "재화구분":"goods_type",
        "상점 카테고리":"category_shop",
        "상품 카테고리":"category_package",
        "패키지명":"package_name",
        "PackageKind":"package_kind",
        "가격":"price",
        "IAP코드(구글)":"iap_code_google",
        "IAP코드(애플)":"iap_code_apple",
        "IAP코드(원스토어)":"iap_code_one",
        "상품반영일":"sale_date_start",
        "판매종료일":"sale_date_end"
        })

    selected_df = df[["goods_type",
        "category_shop",
        "category_package",
        "package_name",
        "package_kind",
        "price",
        "iap_code_google",
        "iap_code_apple",
        "iap_code_one",
        "sale_date_start",
        "sale_date_end"]]
    
    return selected_df

def POTC_truncate_and_insert_to_bigquery(project_id, dataset_id, table_id):

    df = POTC_get_gsheet_to_df(POTC_SPREADSHEET_ID, POTC_SHEET_NAME)
    credentials = get_gcp_credentials()
    client = bigquery.Client(project=project_id, credentials=credentials)
    table_full_id = f"{project_id}.{dataset_id}.{table_id}"

    # 1. 데이터 비우기 (테이블 스키마/설정 유지)
    truncate_query = f"TRUNCATE TABLE `{table_full_id}`"
    client.query(truncate_query, location=LOCATION).result()
    print(f"🗑️ {table_full_id} 데이터가 초기화되었습니다.")
    
    # 2. 데이터 타입 클리닝 (Parquet 변환 에러 방지)
    df_final = df.astype(str)
    numeric_columns = ['package_kind'] # 실제 숫자형 컬럼 리스트로 수정하세요
    for col in numeric_columns:
        if col in df_final.columns:
            # 숫자로 변환하되, 변환 안되는 값(빈칸 등)은 NaN으로 처리 후 0으로 채움
            df_final[col] = pd.to_numeric(df_final[col], errors='coerce').fillna(0).astype(int)

    to_int_list = ['price']
    for cl in to_int_list:
        df_final[cl] = df_final[cl].str.replace(r'[₩,]', '', regex=True)
        df_final[cl] = pd.to_numeric(df_final[cl], errors='coerce')
        df_final[cl] = df_final[cl].fillna(0).astype(str)
    
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


#################### DRSG 패키지 정보 ETL 함수 #####################
def DRSG_get_gsheet_to_df(spreadsheet_id, sheet_name):

    creds = get_gcp_credentials()
    client = gspread.authorize(creds)

    doc = client.open_by_key(spreadsheet_id)
    sheet = doc.worksheet(sheet_name)
    all_data = sheet.get('A:K')

    if not all_data:
        print("데이터가 없습니다.")
        return pd.DataFrame()

    header = all_data[1]
    data = all_data[2:]

    if len(header) == 1:
        header = [f'col_{i}' for i in range(len(data[0]))]

    df = pd.DataFrame(data, columns=header)

    df = df.rename(columns={
        "PackageKind":"package_kind",
        "패키지명":"package_name",
        "상점 카테고리":"category_shop",
        "IAP코드(구글)":"iap_code_google",
        "IAP코드(애플)":"iap_code_apple",
        "상품 카테고리":"category_package",
        "재화구분":"goods_type",
        "가격":"price",
        "상품반영일":"sale_date_start",
        "판매종료일":"sale_date_end",
        "IAP코드(원스토어)":"iap_code_one"
        })

    selected_df = df[["goods_type",
        "category_shop",
        "category_package",
        "package_name",
        "package_kind",
        "price",
        "iap_code_google",
        "iap_code_apple",
        "sale_date_start",
        "sale_date_end"]]
    
    return selected_df

def DRSG_truncate_and_insert_to_bigquery(project_id, dataset_id, table_id):

    df = DRSG_get_gsheet_to_df(DRSG_SPREADSHEET_ID, DRSG_SHEET_NAME)
    credentials = get_gcp_credentials()
    client = bigquery.Client(project=project_id, credentials=credentials)
    table_full_id = f"{project_id}.{dataset_id}.{table_id}"

    # 1. 데이터 비우기 (테이블 스키마/설정 유지)
    truncate_query = f"TRUNCATE TABLE `{table_full_id}`"
    client.query(truncate_query, location=LOCATION).result()
    print(f"🗑️ {table_full_id} 데이터가 초기화되었습니다.")
    
    # 2. 데이터 타입 클리닝 (Parquet 변환 에러 방지)
    df_final = df.astype(str)
    numeric_columns = ['package_kind'] # 실제 숫자형 컬럼 리스트로 수정하세요
    for col in numeric_columns:
        if col in df_final.columns:
            # 숫자로 변환하되, 변환 안되는 값(빈칸 등)은 NaN으로 처리 후 0으로 채움
            df_final[col] = pd.to_numeric(df_final[col], errors='coerce').fillna(0).astype(int)

    to_int_list = ['price']
    for cl in to_int_list:
        df_final[cl] = df_final[cl].str.replace(r'[₩,]', '', regex=True)
        df_final[cl] = pd.to_numeric(df_final[cl], errors='coerce')
        df_final[cl] = df_final[cl].fillna(0).astype(str)
    
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


#################### WWMC 패키지 정보 ETL 함수 #####################
def WWMC_get_gsheet_to_df(spreadsheet_id, sheet_name):

    creds = get_gcp_credentials()
    client = gspread.authorize(creds)

    doc = client.open_by_key(spreadsheet_id)
    sheet = doc.worksheet(sheet_name)
    all_data = sheet.get('A:O')

    if not all_data:
        print("데이터가 없습니다.")
        return pd.DataFrame()

    header = all_data[0]
    data = all_data[1:]

    if len(header) == 1:
        header = [f'col_{i}' for i in range(len(data[0]))]

    df = pd.DataFrame(data, columns=header)

    df = df.rename(columns={
        "Pkind":"package_kind",
        "PACKAGE_NAME":"package_name",
        "CATEGORY":"category_shop",
        "PRODUCTCODE_aos":"iap_code_google",
        "PRODUCTCODE_ios":"iap_code_apple",
        "GROUP":"category_package",
        "PRODUCTCODE_onestore":"iap_code_one",
        "재화구분":"goods_type",
        "가격":"price",
        "애플 등급":"grade_apple",
        "상품 반영일":"sale_date_start",
        "판매 종료일":"sale_date_end",
        })

    selected_df = df[["goods_type",
        "category_shop",
        "category_package",
        "package_name",
        "package_kind",
        "price",
        "iap_code_google",
        "iap_code_apple",
        "iap_code_one",
        "sale_date_start",
        "sale_date_end"]]
    
    return selected_df

def WWMC_truncate_and_insert_to_bigquery(project_id, dataset_id, table_id):

    df = WWMC_get_gsheet_to_df(WWMC_SPREADSHEET_ID, WWMC_SHEET_NAME)
    credentials = get_gcp_credentials()
    client = bigquery.Client(project=project_id, credentials=credentials)
    table_full_id = f"{project_id}.{dataset_id}.{table_id}"

    # 1. 데이터 비우기 (테이블 스키마/설정 유지)
    truncate_query = f"TRUNCATE TABLE `{table_full_id}`"
    client.query(truncate_query, location=LOCATION).result()
    print(f"🗑️ {table_full_id} 데이터가 초기화되었습니다.")
    
    # 2. 데이터 타입 클리닝 (Parquet 변환 에러 방지)
    df_final = df.astype(str)
    numeric_columns = ['package_kind'] # 실제 숫자형 컬럼 리스트로 수정하세요
    for col in numeric_columns:
        if col in df_final.columns:
            # 숫자로 변환하되, 변환 안되는 값(빈칸 등)은 NaN으로 처리 후 0으로 채움
            df_final[col] = pd.to_numeric(df_final[col], errors='coerce').fillna(0).astype(int)

    to_int_list = ['price']
    for cl in to_int_list:
        df_final[cl] = df_final[cl].str.replace(r'[₩,]', '', regex=True)
        df_final[cl] = pd.to_numeric(df_final[cl], errors='coerce')
        df_final[cl] = df_final[cl].fillna(0).astype(str)
    
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


#################### RESU 패키지 정보 ETL 함수 (NOTION DB) #####################
def RESU_extract_notion_data(NOTION_TOKEN, NOTION_DATABASE_ID):
    """Notion 데이터 추출"""
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": get_var("NOTION_API_VERSION", "2022-06-28"),
        "Content-Type": "application/json"
    }
    
    results = []
    has_more = True
    next_cursor = None

    while has_more:
        payload = {"page_size": 100}
        if next_cursor:
            payload["start_cursor"] = next_cursor

        # 2. API 호출
        response = requests.post(url, headers=headers, json=payload)
        
        # 에러 발생 시 멈추지 않고 확인하려면 try-except 블록 고려 (여기선 단순 처리)
        if response.status_code != 200:
            print(f"Error: {response.status_code}, {response.text}")
            break

        data = response.json()

        # 3. 데이터 저장
        # 이번 요청으로 가져온 데이터를 전체 리스트에 추가
        results.extend(data["results"])

        # 4. 다음 루프를 위한 변수 업데이트
        has_more = data["has_more"]        # 더 가져올 데이터가 남았는지 (True/False)
        next_cursor = data["next_cursor"]  # 다음 데이터의 시작 위치

        print(f"현재까지 수집된 데이터: {len(results)}개")
        
        # (선택사항) API 호출 제한(Rate Limit) 방지를 위해 아주 짧은 대기
        time.sleep(0.1) 

    print(f"✅ 총 {len(results)}개의 데이터를 모두 가져왔습니다.")

    # 2. 값 추출 함수 (데이터가 없거나 None일 경우 안전하게 처리)
    def get_text_value(props, prop_key):
        """rich_text 또는 title 타입에서 텍스트 추출"""
        prop = props.get(prop_key, {})
        # rich_text 또는 title 키 확인
        content_list = prop.get('rich_text', []) or prop.get('title', [])
        if content_list:
            return content_list[0].get('plain_text', '')
        return ''

    def get_select_name(props, prop_key):
        """select 또는 status 타입에서 이름 추출"""
        prop = props.get(prop_key, {})
        # select 또는 status 키 확인
        select_obj = prop.get('select') or prop.get('status')
        if select_obj:
            return select_obj.get('name', '')
        return ''

    def get_date_start(props, prop_key):
        """date 타입에서 시작일 추출"""
        prop = props.get(prop_key, {})
        date_obj = prop.get('date')
        if date_obj:
            return date_obj.get('start', '')
        return ''

    def get_number(props, prop_key):
        """number 타입에서 숫자 추출"""
        prop = props.get(prop_key, {})
        return prop.get('number', 0)
    
    list_data = []

    for ps in range(0, len(results)):
        props = results[ps].get('properties', {})
            
        # 3. 데이터 추출 실행
        result = {
            "Package_Name": get_text_value(props, '상품명'),
            "Package_Name_ENG": get_text_value(props, '상품명_영문'),
            "ShopBaseKind": get_text_value(props, 'ShopBaseKind'),
            "Package_Kind": get_text_value(props, '상품카인드'),
            "Goods_Type": get_select_name(props, '재화구분'),
            "IAP_CODE_GOOGLE": get_text_value(props, 'IAP_CODE_GOOGLE'),
            "IAP_CODE_APPLE": get_text_value(props, 'IAP_CODE_APPLE'),
            "IAP_CODE_ONESTORE": get_text_value(props, 'IAP_CODE_ONESTORE'),
            "Cat_Shop": get_select_name(props, '상점 카테고리'),
            "Cat_Package": get_select_name(props, '상품 카테고리'),
            "Price": get_number(props, '가격 (￦)'),
            "Start_Date": get_date_start(props, '판매 시작일'),
            "End_Date": get_date_start(props, '판매 종료일'),
            "Task_State": get_select_name(props, '상태'),
            }
        list_data.append(result)

    df = pd.DataFrame(list_data)
    print(df[["ShopBaseKind", "Package_Kind"]].head(5))

    # 컬럼 가공 전 빈 값 처리 (안전성 강화)
    df["ShopBaseKind"] = df["ShopBaseKind"].fillna("")
    df["Package_Kind"] = df["Package_Kind"].fillna("")
    
    # 조건부 가공
    df["Package_Kind"] = df.apply(
        lambda x: f"{x['ShopBaseKind']}_{x['Package_Kind']}" if x['ShopBaseKind'] and x['Package_Kind'] else x['Package_Kind'], 
        axis=1
    )
    print("조건부 가공 후 데이터 확인")
    print(df[["ShopBaseKind", "Package_Kind"]].head(5))
    
    # 불필요 행 제거 및 컬럼 삭제
    df = df[df['Package_Kind'] != '']
    df = df.drop(columns=['ShopBaseKind'])

    return df

def RESU_truncate_and_insert_to_bigquery(project_id, dataset_id, table_id):

    df = RESU_extract_notion_data(NOTION_TOKEN=NOTION_TOKEN, NOTION_DATABASE_ID=NOTION_DATABASE_ID)
    credentials = get_gcp_credentials()
    client = bigquery.Client(project=project_id, credentials=credentials)
    table_full_id = f"{project_id}.{dataset_id}.{table_id}"

    # 1. 데이터 비우기 (테이블 스키마/설정 유지)
    truncate_query = f"TRUNCATE TABLE `{table_full_id}`"
    client.query(truncate_query, location=LOCATION).result()
    print(f"🗑️ {table_full_id} 데이터가 초기화되었습니다.")
    
    # 2. 데이터 타입 클리닝 (Parquet 변환 에러 방지)
    df_final = df.astype(str)

    to_float_list = ['Price']
    for cl in to_float_list:
        df_final[cl] = df_final[cl].str.replace(r'[₩,]', '', regex=True)
        df_final[cl] = pd.to_numeric(df_final[cl], errors='coerce')
        df_final[cl] = df_final[cl].fillna(0).astype(float)

    date_columns = ['Start_Date', 'End_Date']
    
    for col in date_columns:
        if col in df_final.columns:
            # 1. 먼저 datetime 객체로 변환 (변환할 수 없는 값은 NaT로 강제 변환)
            df_final[col] = pd.to_datetime(df_final[col], errors='coerce')
            
            # 2. 'YYYY-MM-DD' 형식의 문자열로 변환 (시간 정보 제거)
            # dt.strftime을 쓰면 NaT는 NaN이 됩니다.
            df_final[col] = df_final[col].dt.strftime('%Y-%m-%d')
            
            # 3. 결측치(NaN) 처리
            # BigQuery DATE 컬럼에 NULL로 넣기 위해 None으로 치환
            # (주의: 날짜 컬럼은 0이나 빈 문자열 ''로 채우면 업로드 시 에러가 납니다)
            df_final[col] = df_final[col].replace({float('nan'): None}) # type: ignore
            df_final[col] = df_final[col].replace({'': None}) # type: ignore
    
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
    dag_id='Package_Info_ETL',
    default_args=default_args,
    description='Package Info ETL',
    schedule='00 19 * * *',  # 매일 오전 04시 50분 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['ETL', 'package_info', 'bigquery'],
) as dag:

    GBTW_package_info_task = PythonOperator(
        task_id='GBTW_package_info_task',
        python_callable=GBTW_truncate_and_insert_to_bigquery,
        op_kwargs={
            "project_id": "data-science-division-216308",
            "dataset_id": "PackageInfo",
            "table_id": "PackageInfo_GBTW"
        },
        dag=dag,
    )

    POTC_package_info_task = PythonOperator(
        task_id='POTC_package_info_task',
        python_callable=POTC_truncate_and_insert_to_bigquery,
        op_kwargs={
            "project_id": "data-science-division-216308",
            "dataset_id": "PackageInfo",
            "table_id": "PackageInfo_POTC"
        },
        dag=dag,
    )

    DRSG_package_info_task = PythonOperator(
        task_id='DRSG_package_info_task',
        python_callable=DRSG_truncate_and_insert_to_bigquery,
        op_kwargs={
            "project_id": "data-science-division-216308",
            "dataset_id": "PackageInfo",
            "table_id": "PackageInfo_DS"
        },
        dag=dag,
    )

    WWMC_package_info_task = PythonOperator(
        task_id='WWMC_package_info_task',
        python_callable=WWMC_truncate_and_insert_to_bigquery,
        op_kwargs={
            "project_id": "data-science-division-216308",
            "dataset_id": "PackageInfo",
            "table_id": "PackageInfo_WWM"
        },
        dag=dag,
    )

    RESU_package_info_task = PythonOperator(
        task_id='RESU_package_info_task',
        python_callable=RESU_truncate_and_insert_to_bigquery,
        op_kwargs={
            "project_id": "data-science-division-216308",
            "dataset_id": "PackageInfo",
            "table_id": "PackageInfo_Notion_RESU"
        },
        dag=dag,
    )

    GBTW_package_info_task >> POTC_package_info_task >> DRSG_package_info_task >> WWMC_package_info_task >> RESU_package_info_task