import os
import json
import logging
import time
import io
import math
from datetime import datetime, timedelta
from typing import List, Tuple, Any
from zoneinfo import ZoneInfo
from pathlib import Path

# Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.sdk import get_current_context

# Google Cloud
from google.cloud import bigquery, storage
from google.oauth2 import service_account
from google.genai import Client as GenAIClient
import vertexai

# Notion
from notion_client import Client as NotionClient

# Data & Visualization
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns

# Custom Modules (사용자 정의 모듈)
from game_framework_util import *
from game_framework_daily import *
from game_framework_inhouse import *
from game_framework_global_ua import *
from game_framework_rgroup_IAP_gem_ruby import *
from game_framework_longterm_sales import *
from game_framework_newuser_roas import *
from game_framework_summary import *

## 한글 폰트 설정 (Task 내부에서 실행하는 것이 안전하지만, 전역 설정이 필요하다면 유지)
try:
    setup_korean_font()
except Exception:
    pass # 폰트 설정 실패가 DAG 전체 실패로 이어지지 않게 처리

# ---------------------------------------------------------
# 1. 설정 및 상수 정의 (변수 선언만 수행)
# ---------------------------------------------------------
PROJECT_ID = "data-science-division-216308"
LOCATION = "us-central1"
MODEL_NAME = "gemini-2.5-flash"
BUCKET_NAME = 'game-framework1'

# 게임별 설정
GAME_IDX = 'GBTW'
JOYPLE_GAME_ID = 133
DATABASE_SCHEMA = 'GW'
SERVICE_SUB_LIST = [
    '1_daily_sales', '2_inhouse_sales', '3_global_ua',
    '4_detail_sales', '5_logterm_sales', '6_newuser_roas', '7_etc'
]

TEXT_PATH_LIST = [
    'response1_salesComment.text', 'response2_selfPaymentSales.text',
    'response3_revAndCostByCountry.text', 'response3_revAndCostByOs.text',
    'response4_RgroupSales.text', 'response4_salesByPackage.text',
    'response4_WeeklySales_Report.text', 'response5_dailyAvgRevenue.text',
    'response5_monthlyRgroup.text', 'response5_regyearRevenue.text',
    'response6_monthlyROAS.text'
]

SYSTEM_INSTRUCTION = [
    "You're a Game Data Analyst.",
    "Your task is to analyze the metrics of a given mobile game and identify the causes of any changes.",
    "Your answers must be in Korean.",
    "The unit of amount in the Sales or Revenue, Cost Data is Korean Won.",
    "You must answer in Notion's Markdown format, but do not use title syntax.",
]

# ---------------------------------------------------------
# 2. 유틸리티 함수 (클라이언트 생성 및 검증)
# ---------------------------------------------------------
def get_gcp_credentials():
    """Airflow Variable에서 GCP 자격 증명을 로드합니다."""
    credentials_json = Variable.get('GOOGLE_CREDENTIAL_JSON')
    cred_dict = json.loads(credentials_json)
    if 'private_key' in cred_dict:
        cred_dict['private_key'] = cred_dict['private_key'].replace('\\n', '\n')
    
    return service_account.Credentials.from_service_account_info(
        cred_dict,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )

def init_clients():
    """Task 내부에서 실행되어 필요한 클라이언트들을 생성하여 반환합니다."""
    creds = get_gcp_credentials()
    
    # 1. GCP Clients
    bq_client = bigquery.Client(project=PROJECT_ID, credentials=creds)
    storage_client = storage.Client(project=PROJECT_ID, credentials=creds)
    bucket = storage_client.bucket(BUCKET_NAME)
    
    # 2. Vertex AI Init
    vertexai.init(project=PROJECT_ID, location=LOCATION, credentials=creds)
    genai_client = GenAIClient(
            vertexai=True,
            project=PROJECT_ID,
            location=LOCATION,
            # credentials=creds  # google-genai 버전에 따라 이 인자가 필요 없거나, http_options 등으로 넘겨야 할 수 있습니다.
                                # 보통 vertexai=True와 project/location만 있으면 google.auth를 내부적으로 사용합니다.
        )

    # 3. Notion Client
    notion_token = Variable.get("MS_TEAM_NOTION_TOKEN")
    notion_client = NotionClient(auth=notion_token)
    
    # 4. Configs
    notion_version = Variable.get("NOTION_API_VERSION")
    headers_json = {
        "Authorization": f"Bearer {notion_token}",
        "Notion-Version": notion_version,
        "Content-Type": "application/json"
    }
    
    # 노션 DB ID
    # database_id = Variable.get("GAMEFRAMEWORK_GBTW_NOTION_DB_ID") 
    database_id = '256ea67a568180318e32ddc6f610ba39' 

    return {
        "bq_client": bq_client,
        "bucket": bucket,
        "genai_client": genai_client,
        "notion_client": notion_client,
        "headers_json": headers_json,
        "database_id": database_id,
        "notion_token": notion_token,
        "notion_version": notion_version
    }

def validate_path(path: Any, func_name: str, context_str: str):
    """결과 경로 유효성 검사 및 로깅. 실패 시 예외 발생."""
    logger = logging.getLogger("airflow.task")
    is_valid = False
    
    if isinstance(path, str) and len(path) > 0:
        is_valid = True
    elif isinstance(path, (list, tuple)) and len(path) > 0:
        is_valid = True
    elif path is not None:
        is_valid = True

    if is_valid:
        logger.info(f"✅ {context_str}: {func_name} 완료 (Path/Result: {path})")
    else:
        error_msg = f"❌ {context_str}: {func_name} 실패 (결과 없음)"
        logger.error(error_msg)
        raise AirflowException(error_msg)

# ---------------------------------------------------------
# 3. Task 함수 정의
# ---------------------------------------------------------

def make_gameframework_notion_page_task(**context):
    clients = init_clients()
    try:
        page_info = make_gameframework_notion_page(
            gameidx=GAME_IDX,
            NOTION_TOKEN=clients['notion_token'],
            DATABASE_ID=clients['database_id'],
            notion=clients['notion_client']
        )
        print(f"✅ {GAME_IDX} NOTION 페이지 생성 완료")
        context['task_instance'].xcom_push(key='page_info', value=page_info)
        
        return page_info
    except Exception as e:
        raise AirflowException(f"❌ 페이지 생성 실패: {e}")

def daily_data_task(**context):
    clients = init_clients()
    service_sub = SERVICE_SUB_LIST[0]
    
    st1 = Daily_revenue_query(joyplegameid=JOYPLE_GAME_ID, gameidx=GAME_IDX, bigquery_client=clients['bq_client'], bucket=clients['bucket'])
    validate_path(st1, "Daily_revenue_query", service_sub)

    st2 = Daily_revenue_YOY_query(joyplegameid=JOYPLE_GAME_ID, gameidx=GAME_IDX, bigquery_client=clients['bq_client'], bucket=clients['bucket'])
    validate_path(st2, "Daily_revenue_YOY_query", service_sub)

    st3 = Daily_revenue_target_revenue_query(joyplegameid=JOYPLE_GAME_ID, gameidx=GAME_IDX, bigquery_client=clients['bq_client'], bucket=clients['bucket'])
    validate_path(st3, "Daily_revenue_target_revenue_query", service_sub)
    
    img_gcs_path = merge_daily_graph(gameidx=GAME_IDX, daily_revenue_path=st1, daily_revenue_yoy_path=st2, bucket=clients['bucket'])
    validate_path(img_gcs_path, "merge_daily_graph", service_sub)

    # 오타 수정: MOEDEL_NAME -> MODEL_NAME
    daily_revenue_data_upload_to_notion(
        st1=st1, st2=st2, st3=st3,
        MOEDEL_NAME=MODEL_NAME, gameidx=GAME_IDX, service_sub=service_sub,
        genai_client=clients['genai_client'], MODEL_NAME=MODEL_NAME,
        SYSTEM_INSTRUCTION=SYSTEM_INSTRUCTION, notion=clients['notion_client'],
        bucket=clients['bucket'], headers_json=clients['headers_json']
    )

def inhouse_data_task(**context):
    clients = init_clients()
    service_sub = SERVICE_SUB_LIST[1]

    st1 = inhouse_sales_query(joyplegameid=JOYPLE_GAME_ID, gameidx=GAME_IDX, bigquery_client=clients['bq_client'], bucket=clients['bucket'])
    validate_path(st1, "inhouse_sales_query", service_sub)

    st2 = inhouse_sales_before24_query(joyplegameid=JOYPLE_GAME_ID, gameidx=GAME_IDX, bigquery_client=clients['bq_client'], bucket=clients['bucket'])
    validate_path(st2, "inhouse_sales_before24_query", service_sub)

    merged_img_path = merge_inhouse_graph(GAME_IDX, st1, st2, clients['bucket'])
    validate_path(merged_img_path, "merge_inhouse_graph", service_sub)

    inhouse_revenue_data_upload_to_notion(
        gameidx=GAME_IDX, st1=st1, st2=st2, service_sub=service_sub,
        genai_client=clients['genai_client'], MODEL_NAME=MODEL_NAME,
        SYSTEM_INSTRUCTION=SYSTEM_INSTRUCTION, notion=clients['notion_client'],
        bucket=clients['bucket'], headers_json=clients['headers_json'],
        NOTION_TOKEN=clients['notion_token'], NOTION_VERSION=clients['notion_version']
    )

def global_ua_data_task(**context):
    clients = init_clients()
    service_sub = SERVICE_SUB_LIST[2]
    bq = clients['bq_client']
    bk = clients['bucket']

    st1 = cohort_by_country_revenue(joyplegameid=JOYPLE_GAME_ID, gameidx=GAME_IDX, bigquery_client=bq, bucket=bk)
    validate_path(st1, "cohort_by_country_revenue", service_sub)

    st2 = cohort_by_country_cost(joyplegameid=JOYPLE_GAME_ID, gameidx=GAME_IDX, bigquery_client=bq, bucket=bk)
    validate_path(st2, "cohort_by_country_cost", service_sub)

    st3 = os_rev(joyplegameid=JOYPLE_GAME_ID, gameidx=GAME_IDX, bigquery_client=bq, bucket=bk)
    validate_path(st3, "os_rev", service_sub)

    st4 = os_cost(joyplegameid=JOYPLE_GAME_ID, gameidx=GAME_IDX, bigquery_client=bq, bucket=bk)
    validate_path(st4, "os_cost", service_sub)

    merged_country_graph = merge_contry_graph(gameidx=GAME_IDX, gcs_path_1=st1, gcs_path_2=st2, bucket=bk)
    validate_path(merged_country_graph, "merge_contry_graph", service_sub)

    merged_os_graph = merge_os_graph(gameidx=GAME_IDX, gcs_path_1=st3, gcs_path_2=st4, bucket=bk)
    validate_path(merged_os_graph, "merge_os_graph", service_sub)

    country_data_upload_to_notion(
        gameidx=GAME_IDX, st1=st1, st2=st2, service_sub=service_sub,
        genai_client=clients['genai_client'], MODEL_NAME=MODEL_NAME,
        SYSTEM_INSTRUCTION=SYSTEM_INSTRUCTION, notion=clients['notion_client'],
        bucket=bk, headers_json=clients['headers_json'],
        NOTION_TOKEN=clients['notion_token'], NOTION_VERSION=clients['notion_version']
    )

    os_data_upload_to_notion(
        gameidx=GAME_IDX, st1=st3, st2=st4, service_sub=service_sub,
        genai_client=clients['genai_client'], MODEL_NAME=MODEL_NAME,
        SYSTEM_INSTRUCTION=SYSTEM_INSTRUCTION, notion=clients['notion_client'],
        bucket=bk, headers_json=clients['headers_json'],
        NOTION_TOKEN=clients['notion_token'], NOTION_VERSION=clients['notion_version']
    )

    st5 = country_group_rev(joyplegameid=JOYPLE_GAME_ID, gameidx=GAME_IDX, bigquery_client=bq, bucket=bk)
    st6 = cohort_by_country_cost(joyplegameid=JOYPLE_GAME_ID, gameidx=GAME_IDX, bigquery_client=bq, bucket=bk)
    
    merged_country_group_graph = merge_country_group_df_draw(joyplegameid=JOYPLE_GAME_ID, gameidx=GAME_IDX, bigquery_client=bq, bucket=bk)
    validate_path(merged_country_group_graph, "merge_country_group_df_draw", service_sub)

    country_group_data_upload_to_notion(
        joyplegameid=JOYPLE_GAME_ID, gameidx=GAME_IDX, st1=st5, st2=st6,
        service_sub=service_sub, genai_client=clients['genai_client'],
        MODEL_NAME=MODEL_NAME, SYSTEM_INSTRUCTION=SYSTEM_INSTRUCTION,
        notion=clients['notion_client'], bigquery_client=bq, bucket=bk,
        headers_json=clients['headers_json'], NOTION_TOKEN=clients['notion_token'],
        NOTION_VERSION=clients['notion_version']
    )

def rgroup_iapgemruby_task(**context):
    clients = init_clients()
    service_sub = SERVICE_SUB_LIST[3]
    bq = clients['bq_client']
    bk = clients['bucket']

    path_rev_group_rev_pu = rev_group_rev_pu(joyplegameid=JOYPLE_GAME_ID, gameidx=GAME_IDX, bigquery_client=bq, bucket=bk)
    validate_path(path_rev_group_rev_pu, "rev_group_rev_pu", service_sub)

    path_iap_gem_ruby = iap_gem_ruby(joyplegameid=JOYPLE_GAME_ID, gameidx=GAME_IDX, databaseschema=DATABASE_SCHEMA, bigquery_client=bq, bucket=bk)
    path_iap_gem_ruby_history = iap_gem_ruby_history(gameidx=GAME_IDX, bigquery_client=bq, bucket=bk)
    path_iap_df = iap_df(joyplegameid=JOYPLE_GAME_ID, gameidx=GAME_IDX, databaseschema=DATABASE_SCHEMA, bigquery_client=bq, bucket=bk)
    path_gem_df = gem_df(joyplegameid=JOYPLE_GAME_ID, gameidx=GAME_IDX, bigquery_client=bq, bucket=bk)
    path_ruby_df = ruby_df(joyplegameid=JOYPLE_GAME_ID, gameidx=GAME_IDX, bigquery_client=bq, bucket=bk)
    
    path_weekly_iapcategory_rev, path_weekly_iapcategory_rev_cols = weekly_iapcategory_rev(
        joyplegameid=JOYPLE_GAME_ID, gameidx=GAME_IDX, databaseschema=DATABASE_SCHEMA, bigquery_client=bq, bucket=bk
    )
    
    path_top3_items_by_category = top3_items_by_category(
        joyplegameid=JOYPLE_GAME_ID, gameidx=GAME_IDX, service_sub=service_sub,
        databaseschema=DATABASE_SCHEMA, genai_client=clients['genai_client'],
        MODEL_NAME=MODEL_NAME, SYSTEM_INSTRUCTION=SYSTEM_INSTRUCTION,
        path_weekly_iapcategory_rev=path_weekly_iapcategory_rev,
        path_weekly_iapcategory_rev_cols=path_weekly_iapcategory_rev_cols,
        bigquery_client=bq, bucket=bk, PROJECT_ID=PROJECT_ID, LOCATION=LOCATION
    )
    
    path_rgroup_top3_pu = rgroup_top3_pu(joyplegameid=JOYPLE_GAME_ID, gameidx=GAME_IDX, databaseschema=DATABASE_SCHEMA, bigquery_client=bq, bucket=bk)
    path_rgroup_top3_rev = rgroup_top3_rev(joyplegameid=JOYPLE_GAME_ID, gameidx=GAME_IDX, databaseschema=DATABASE_SCHEMA, bigquery_client=bq, bucket=bk)
    
    dfs_for_graphs, path_top3_items_rev, gcs_paths_for_graphs = top3_items_rev(
        joyplegameid=JOYPLE_GAME_ID, gameidx=GAME_IDX, databaseschema=DATABASE_SCHEMA,
        service_sub=service_sub, path_weekly_iapcategory_rev=path_weekly_iapcategory_rev,
        genai_client=clients['genai_client'], MODEL_NAME=MODEL_NAME,
        SYSTEM_INSTRUCTION=SYSTEM_INSTRUCTION, bigquery_client=bq, bucket=bk
    )

    path_merge_rgroup_graph = merge_rgroup_graph(gameidx=GAME_IDX, path_group_rev_pu=path_rev_group_rev_pu, bucket=bk)
    validate_path(path_merge_rgroup_graph, "merge_rgroup_graph", service_sub)

    rgroup_rev_upload_notion(
        gameidx=GAME_IDX, path_rev_group_rev_pu=path_rev_group_rev_pu,
        rev_group_rev_pu_path=path_rev_group_rev_pu, service_sub=service_sub,
        genai_client=clients['genai_client'], MODEL_NAME=MODEL_NAME,
        SYSTEM_INSTRUCTION=SYSTEM_INSTRUCTION, notion=clients['notion_client'],
        bucket=bk, headers_json=clients['headers_json']
    )

    iap_gem_ruby_upload_notion(
        gameidx=GAME_IDX, joyplegameid=JOYPLE_GAME_ID, databaseschema=DATABASE_SCHEMA,
        path_iap_gem_ruby=path_iap_gem_ruby, path_iapgemruby_history=path_iap_gem_ruby_history,
        path_top3_items_by_category=path_top3_items_by_category,
        path_weekly_iapcategory_rev=path_weekly_iapcategory_rev,
        gcs_paths_for_graphs=gcs_paths_for_graphs, service_sub=service_sub,
        genai_client=clients['genai_client'], MODEL_NAME=MODEL_NAME,
        SYSTEM_INSTRUCTION=SYSTEM_INSTRUCTION, bigquery_client=bq,
        notion=clients['notion_client'], bucket=bk, headers_json=clients['headers_json']
    )

    iap_toggle_add(
        gameidx=GAME_IDX, service_sub=service_sub, MODEL_NAME=MODEL_NAME,
        SYSTEM_INSTRUCTION=SYSTEM_INSTRUCTION, path_iap_df=path_iap_df,
        path_iapgemruby_history=path_iap_gem_ruby_history, PROJECT_ID=PROJECT_ID,
        LOCATION=LOCATION, bucket=bk, notion=clients['notion_client'], headers_json=clients['headers_json']
    )
    
    gem_toggle_add(
        gameidx=GAME_IDX, service_sub=service_sub, MODEL_NAME=MODEL_NAME,
        SYSTEM_INSTRUCTION=SYSTEM_INSTRUCTION, path_gem_df=path_gem_df,
        path_iapgemruby_history=path_iap_gem_ruby_history, PROJECT_ID=PROJECT_ID,
        LOCATION=LOCATION, bucket=bk, notion=clients['notion_client'], headers_json=clients['headers_json']
    )

    ruby_toggle_add(
        gameidx=GAME_IDX, service_sub=service_sub, MODEL_NAME=MODEL_NAME,
        SYSTEM_INSTRUCTION=SYSTEM_INSTRUCTION, path_ruby_df=path_ruby_df,
        path_iapgemruby_history=path_iap_gem_ruby_history, PROJECT_ID=PROJECT_ID,
        LOCATION=LOCATION, bucket=bk, notion=clients['notion_client'], headers_json=clients['headers_json']
    )

    rgroup_top3_upload_notion(
        gameidx=GAME_IDX, service_sub=service_sub, MODEL_NAME=MODEL_NAME,
        SYSTEM_INSTRUCTION=SYSTEM_INSTRUCTION, path_rgroup_top3_pu=path_rgroup_top3_pu,
        path_rgroup_top3_rev=path_rgroup_top3_rev, PROJECT_ID=PROJECT_ID,
        LOCATION=LOCATION, bucket=bk, notion=clients['notion_client'], headers_json=clients['headers_json']
    )

def longterm_sales_task(**context):
    clients = init_clients()
    service_sub = SERVICE_SUB_LIST[4]
    bq = clients['bq_client']
    bk = clients['bucket']

    path_monthly_day_average_rev = monthly_day_average_rev(joyplegameid=JOYPLE_GAME_ID, gameidx=GAME_IDX, bigquery_client=bq, bucket=bk)
    path_rgroup_rev_DOD = rgroup_rev_DOD(joyplegameid=JOYPLE_GAME_ID, gameidx=GAME_IDX, bigquery_client=bq, bucket=bk)
    path_rgroup_rev_total = rgroup_rev_total(joyplegameid=JOYPLE_GAME_ID, gameidx=GAME_IDX, bigquery_client=bq, bucket=bk)
    path_regyearRevenue, path_regyearRevenue_pv2 = rev_cohort_year(joyplegameid=JOYPLE_GAME_ID, gameidx=GAME_IDX, bigquery_client=bq, bucket=bk)
    
    monthly_day_average_merge_graph(gameidx=GAME_IDX, path_monthly_day_average_rev=path_monthly_day_average_rev, bucket=bk)
    
    path_merge_rgroup_rev_pu_table = merge_rgroup_rev_pu_table(gameidx=GAME_IDX, path_rgroup_rev_DOD=path_rgroup_rev_DOD, bucket=bk)
    path_merge_rgroup_total_rev_pu_table = merge_rgroup_total_rev_pu_table(gameidx=GAME_IDX, path_rgroup_rev_total=path_rgroup_rev_total, bucket=bk)
    
    path_merge_merge_rgroup_total_rev_pu_ALL_table = merge_merge_rgroup_total_rev_pu_ALL_table(
        gameidx=GAME_IDX, bucket=bk,
        path_merge_rgroup_rev_pu_table=path_merge_rgroup_rev_pu_table,
        path_merge_rgroup_total_rev_pu_table=path_merge_rgroup_total_rev_pu_table
    )

    longterm_rev_upload_notion(
        gameidx=GAME_IDX, service_sub=service_sub,
        path_monthly_day_average_rev=path_monthly_day_average_rev,
        NOTION_TOKEN=clients['notion_token'], NOTION_VERSION=clients['notion_version'],
        MODEL_NAME=MODEL_NAME, SYSTEM_INSTRUCTION=SYSTEM_INSTRUCTION,
        notion=clients['notion_client'], bucket=bk, headers_json=clients['headers_json']
    )

    monthly_rgroup_upload_notion(
        gameidx=GAME_IDX, service_sub=service_sub,
        path_rgroup_rev_total=path_rgroup_rev_total, path_rgroup_rev_DOD=path_rgroup_rev_DOD,
        path_merge_merge_rgroup_total_rev_pu_ALL_table=path_merge_merge_rgroup_total_rev_pu_ALL_table,
        NOTION_TOKEN=clients['notion_token'], NOTION_VERSION=clients['notion_version'],
        MODEL_NAME=MODEL_NAME, SYSTEM_INSTRUCTION=SYSTEM_INSTRUCTION,
        notion=clients['notion_client'], bucket=bk, headers_json=clients['headers_json']
    )

    cohort_rev_upload_notion(
        gameidx=GAME_IDX, service_sub=service_sub,
        path_regyearRevenue=path_regyearRevenue, path_regyearRevenue_pv2=path_regyearRevenue_pv2,
        NOTION_TOKEN=clients['notion_token'], NOTION_VERSION=clients['notion_version'],
        MODEL_NAME=MODEL_NAME, SYSTEM_INSTRUCTION=SYSTEM_INSTRUCTION,
        notion=clients['notion_client'], bucket=bk, headers_json=clients['headers_json']
    )

def newuser_roas_task(**context):
    clients = init_clients()
    service_sub = SERVICE_SUB_LIST[5]
    bq = clients['bq_client']
    bk = clients['bucket']

    path_result6_monthlyROAS = result6_monthlyROAS(joyplegameid=JOYPLE_GAME_ID, gameidx=GAME_IDX, bigquery_client=bq, bucket=bk)
    path_result6_pLTV = result6_pLTV(joyplegameid=JOYPLE_GAME_ID, gameidx=GAME_IDX, bigquery_client=bq, bucket=bk)
    path_result6_return = result6_return(joyplegameid=JOYPLE_GAME_ID, gameidx=GAME_IDX, bigquery_client=bq, bucket=bk)
    path_result6_BEP = result6_BEP(joyplegameid=JOYPLE_GAME_ID, gameidx=GAME_IDX, bigquery_client=bq, bucket=bk)
    path_result6_roaskpi = result6_roaskpi(gameidx=GAME_IDX, bigquery_client=bq, bucket=bk)
    
    path_roas_kpi = roas_kpi(joyplegameid=JOYPLE_GAME_ID, gameidx=GAME_IDX, path_result6_roaskpi=path_result6_roaskpi, bigquery_client=bq, bucket=bk)
    
    path_roas_dataframe_preprocessing = roas_dataframe_preprocessing(
        gameidx=GAME_IDX, path_result6_monthlyROAS=path_result6_monthlyROAS,
        path_result6_pLTV=path_result6_pLTV, path_result6_return=path_result6_return,
        path_result6_BEP=path_result6_BEP, path_roas_kpi=path_roas_kpi, bucket=bk
    )

    roas_kpi_table_merge(
        gameidx=GAME_IDX, path_roas_dataframe_preprocessing=path_roas_dataframe_preprocessing,
        path_result6_monthlyROAS=path_result6_monthlyROAS, path_roas_kpi=path_roas_kpi,
        bucket=bk, gcs_bucket=BUCKET_NAME
    )

    retrieve_new_user_upload_notion(
        gameidx=GAME_IDX, service_sub=service_sub,
        path_monthlyBEP_ROAS=path_result6_monthlyROAS, path_roas_kpi=path_roas_kpi,
        path_roas_dataframe_preprocessing=path_roas_dataframe_preprocessing,
        MODEL_NAME=MODEL_NAME, SYSTEM_INSTRUCTION=SYSTEM_INSTRUCTION,
        NOTION_TOKEN=clients['notion_token'], NOTION_VERSION=clients['notion_version'],
        notion=clients['notion_client'], bucket=bk, headers_json=clients['headers_json']
    )

def summary_task(**context):
    clients = init_clients()
    service_sub = SERVICE_SUB_LIST[6]
    
    game_framework_summary_upload_notion(
        gameidx=GAME_IDX, service_sub=service_sub,
        genai_client=clients['genai_client'], bucket=clients['bucket'],
        text_path_list=TEXT_PATH_LIST, notion=clients['notion_client'],
        MODEL_NAME=MODEL_NAME
    )


# ---------------------------------------------------------
# 4. DAG 정의
# ---------------------------------------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
}

with DAG(
    dag_id='game_framework_gbtw_main',
    default_args=default_args,
    description='Databricks 데이터를 Notion DB에 동기화하는 DAG',
    schedule='30 20 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['notion', 'sync', 'gbtw'],
) as dag:

    # Task 정의
    t1_create_page = PythonOperator(
        task_id='make_gameframework_notion_page_wraper', # 이 ID를 내부 함수들이 참조하고 있음
        python_callable=make_gameframework_notion_page_task,
    )

    t2_daily = PythonOperator(
        task_id='daily_data_game_framework',
        python_callable=daily_data_task,
    )

    t3_inhouse = PythonOperator(
        task_id='inhouse_data_game_framework',
        python_callable=inhouse_data_task,
    )

    t4_global_ua = PythonOperator(
        task_id='global_ua_data_game_framework',
        python_callable=global_ua_data_task,
    )

    t5_rgroup = PythonOperator(
        task_id='rgroup_iapgemruby_data_game_framework',
        python_callable=rgroup_iapgemruby_task,
    )

    t6_longterm = PythonOperator(
        task_id='longterm_sales_data_game_framework',
        python_callable=longterm_sales_task,
    )

    t7_newuser_roas = PythonOperator(
        task_id='newuser_roas_data_game_framework',
        python_callable=newuser_roas_task,
    )

    t8_summary = PythonOperator(
        task_id='game_framework_summary',
        python_callable=summary_task,
    )

    # 의존성 설정
    t1_create_page >> t2_daily >> t3_inhouse >> t4_global_ua >> t5_rgroup >> t6_longterm >> t7_newuser_roas >> t8_summary