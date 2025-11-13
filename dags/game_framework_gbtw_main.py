import time
import pandas as pd
from google.cloud import bigquery
from google import genai
from google.genai import types
from google.cloud import storage
import vertexai
from google.genai import Client
from google.genai.types import GenerateContentConfig, Retrieval, Tool, VertexRagStore

# 인증관련
import google.auth
from google.auth.transport.requests import Request
import logging

# 그래프 관련 패키지
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, StrMethodFormatter, PercentFormatter, MultipleLocator
import matplotlib as mpl
import matplotlib.font_manager as fm
from matplotlib import cm
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont # 2가지 파일 합치기
import matplotlib.dates as mdates
import nest_asyncio
from jinja2 import Template
from playwright.async_api import async_playwright
import asyncio
import IPython.display as IPd
from bs4 import BeautifulSoup
from io import BytesIO
from typing import List, Tuple
from matplotlib import rcParams
from matplotlib.patches import Rectangle

# 전처리 관련 패키지
import numpy as np
import re
import os 
import math
import time
import pandas as pd
from notion_client import Client as notionClient
import requests
import json
from datetime import datetime, timezone, timedelta
from adjustText import adjust_text
from airflow.models import Variable
from airflow.operators.python import get_current_context
from zoneinfo import ZoneInfo  # Python 3.9 이상
from pathlib import Path
import io

# 게임 프레임워크 모듈
from game_framework_util import *
from game_framework_daily import *
from game_framework_inhouse import *
from game_framework_global_ua import *
from game_framework_rgroup_IAP_gem_ruby import *

# Airflow function
from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.models import Variable




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
    dag_id='game_framework_gbtw_main',
    default_args=default_args,
    description='Databricks 데이터를 Notion DB에 동기화하는 DAG',
    schedule= '0 23 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['notion', 'sync', 'databricks'],
) as dag:


    logger = logging.getLogger(__name__)

    # 환경 변수 가져오기
    def get_var(key: str, default: str = None) -> str:
        """환경 변수 또는 Airflow Variable 조회"""
        return os.environ.get(key) or Variable.get(key, default_var=default)

    # 변수 생성
    PROJECT_ID = "data-science-division-216308"
    LOCATION = "us-central1"
    MODEL_NAME = "gemini-2.5-flash"

    NOTION_TOKEN=get_var("MS_TEAM_NOTION_TOKEN") # MS팀 API 키
    NOTION_VERSION=get_var("NOTION_API_VERSION")
    DATABASE_ID = '256ea67a568180318e32ddc6f610ba39'   ##### TEST DB
    # DATABASE_ID=get_var("GAMEFRAMEWORK_GBTW_NOTION_DB_ID")  ###### 라이브 환경 DB
    CREDENTIALS_JSON = get_var('GOOGLE_CREDENTIAL_JSON')

    # GCP credential key 로드
    cred_dict = json.loads(CREDENTIALS_JSON)
    credentials, project_id = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    credentials.refresh(Request())

    ## vertexai 초기화 진행
    vertexai.init(project=PROJECT_ID, location=LOCATION)

    # 클라이언트 모음
    try:
        genai_client = Client()  # vertexai=True 제거
        print("✅ genai_client 초기화 성공")
    except Exception as e:
        print(f"❌ genai_client 초기화 실패: {e}")
        raise

    bigquery_client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
    
    try:
        notion = notionClient(auth=NOTION_TOKEN)
        print("✅ Notion 클라이언트 초기화 성공")
    except Exception as e:
        print(f"❌ Notion 클라이언트 초기화 실패: {e}")
        raise

    gcs_client = storage.Client.from_service_account_info(cred_dict)
    bucket = gcs_client.bucket('game-framework1')


    #### 제미나이 시스템 인스트럭션 
    SYSTEM_INSTRUCTION = [
                    "You're a Game Data Analyst.",
                    "Your task is to analyze the metrics of a given mobile game and identify the causes of any changes.",
                    "Your answers must be in Korean.",
                    "The unit of amount in the Sales or Revenue, Cost Data is Korean Won.",
                    "You must answer in Notion's Markdown format, but do not use title syntax.",
                ]

    ####  json header 값
    headers_json = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json"
    }

    # 게임별 주요 변수 값
    gameidx = 'GBTW'
    joyplegameid = 133
    service_sub = [
        '1_daily_sales',
        '2_inhouse_sales',
        '3_global_ua',
        '4_detail_sales',
        '5_logterm_sales',
        '6_newuser_roas',
        '7_etc'
    ]
    databaseschema='GW'

    ## 에러 출력 함수 
    def if_else_length(path: str, gameidx: str, service_sub: str, func_name: str):
        if len(path) > 0:
            print(f"✅ {gameidx}: {service_sub} {func_name} 완료")
        else:
            print(f"❌ {gameidx}: {service_sub} {func_name} 실패")

    ## 페이지 생성 함수 //////////// task 함수
    def make_gameframework_notion_page_wraper(**context):
        try:
            page_info = make_gameframework_notion_page(
                gameidx=gameidx,
                NOTION_TOKEN=NOTION_TOKEN,
                DATABASE_ID=DATABASE_ID,
                notion = notion
                )
            print(f"✅ {gameidx} NOTION 페이지 생성 완료")
            
            current_context = get_current_context()
            current_context['task_instance'].xcom_push(
                key='page_info',
                value=page_info
            )

            return page_info
        except Exception as e:
            print(f"❌ {gameidx} NOTION 페이지 생성 실패")
            print(f"🔴 {e}")
            

    ####### 일자별 게임 프레임 워크

    def daily_data_game_framework(joyplegameid:int, gameidx:str, service_sub:str, bigquery_client, notion, MODEL_NAME:str, SYSTEM_INSTRUCTION:list, genai_client, bucket, headers_json): 
        print(f"📧 RUN 데일리 데이터 게임 프로엠워크 시작: {gameidx}")
        
        st1 = Daily_revenue_query(joyplegameid=joyplegameid, gameidx=gameidx, bigquery_client=bigquery_client, bucket=bucket)
        if_else_length(path=st1, gameidx=gameidx, service_sub=service_sub, func_name="Daily_revenue_query")

        st2 = Daily_revenue_YOY_query(joyplegameid=joyplegameid, gameidx=gameidx, bigquery_client=bigquery_client, bucket=bucket)
        if_else_length(path=st2, gameidx=gameidx, service_sub=service_sub, func_name="Daily_revenue_YOY_query")

        st3 = Daily_revenue_target_revenue_query(joyplegameid=joyplegameid, gameidx=gameidx, bigquery_client=bigquery_client, bucket=bucket)
        if_else_length(path=st3, gameidx=gameidx, service_sub=service_sub, func_name="Daily_revenue_target_revenue_query")
        
        s_total = merge_daily_revenue(st1, st2, bucket=bucket)
        if_else_length(path=s_total, gameidx=gameidx, service_sub=service_sub, func_name="merge_daily_revenue")

        img_gcs_path = merge_daily_graph(gameidx=gameidx, daily_revenue_path=st1, daily_revenue_yoy_path=st2, bucket=bucket)
        if_else_length(path=img_gcs_path, gameidx=gameidx, service_sub=service_sub, func_name="merge_daily_graph")

        try :
            daily_revenue_data_upload_to_notion(
                st1=st1,
                st2=st2,
                MOEDEL_NAME=MODEL_NAME,
                gameidx=gameidx, 
                service_sub=service_sub[0], 
                genai_client=genai_client, 
                MODEL_NAME=MODEL_NAME, 
                SYSTEM_INSTRUCTION=SYSTEM_INSTRUCTION, 
                notion=notion, 
                bucket=bucket, 
                headers_json=headers_json,
            )
            print(f"✅ {gameidx}: {service_sub} daily_revenue_data_upload_to_notion 완료")
        except Exception as e:
            print(f"❌ {gameidx}: {service_sub} daily_revenue_data_upload_to_notion 실패 ")
            print(f"🔴 {e}")


    ###### 인하우스 게임 프레임워크
    def inhouse_data_game_framework(joyplegameid:int, gameidx:str, service_sub:str, bigquery_client, notion, MODEL_NAME:str, SYSTEM_INSTRUCTION:list, genai_client, bucket, headers_json): 
        print(f"📧 RUN 인하우스 데이터 게임 프로엠워크 시작: {gameidx}")

        st1 = inhouse_sales_query(joyplegameid=joyplegameid, gameidx=gameidx, bigquery_client=bigquery_client, bucket=bucket)
        if_else_length(path=st1, gameidx=gameidx, service_sub=service_sub, func_name="inhouse_sales_query")

        st2 = inhouse_sales_before24_query(joyplegameid=joyplegameid, gameidx=gameidx, bigquery_client=bigquery_client, bucket=bucket)
        if_else_length(path=st2, gameidx=gameidx, service_sub=service_sub, func_name="inhouse_sales_before24_query")

        merged_img_path = merge_inhouse_graph(gameidx, st1, st2, bucket)
        if_else_length(path=merged_img_path, gameidx=gameidx, service_sub=service_sub, func_name="merge_inhouse_graph")

        try:
            inhouse_revenue_data_upload_to_notion(
                gameidx=gameidx,
                st1 = st1,
                st2 = st2,
                service_sub=service_sub,
                genai_client=genai_client,
                MODEL_NAME = MODEL_NAME,
                SYSTEM_INSTRUCTION=SYSTEM_INSTRUCTION,
                notion=notion,
                bucket=bucket,
                headers_json=headers_json,
                NOTION_TOKEN=NOTION_TOKEN,
                NOTION_VERSION=NOTION_VERSION,
            )
        except Exception as e:
            print(f"❌ {gameidx}: {service_sub} daily_revenue_data_upload_to_notion 실패 ")
            print(f"🔴 {e}")


    ##### 글로벌 UA 프레임 워크
    def global_ua_data_game_framework(joyplegameid:int, gameidx:str, service_sub:str, bigquery_client, notion, MODEL_NAME:str, SYSTEM_INSTRUCTION:list, genai_client, bucket, headers_json): 
        print(f"📧 RUN 글로벌 UA 데이터 게임 프로엠워크 시작: {gameidx}")

        st1 = cohort_by_country_revenue(joyplegameid=joyplegameid, gameidx=gameidx, bigquery_client=bigquery_client, bucket=bucket)
        if_else_length(path=st1, gameidx=gameidx, service_sub=service_sub, func_name="cohort_by_country_revenue")

        st2 = cohort_by_country_cost(joyplegameid=joyplegameid, gameidx=gameidx, bigquery_client=bigquery_client, bucket=bucket)
        if_else_length(path=st2, gameidx=gameidx, service_sub=service_sub, func_name="cohort_by_country_cost")

        st3 = os_rev(joyplegameid=joyplegameid, gameidx=gameidx, bigquery_client=bigquery_client, bucket=bucket)
        if_else_length(path=st3, gameidx=gameidx, service_sub=service_sub, func_name="os_rev")

        st4 = os_cost(joyplegameid=joyplegameid, gameidx=gameidx, bigquery_client=bigquery_client, bucket=bucket)
        if_else_length(path=st4, gameidx=gameidx, service_sub=service_sub, func_name="os_cost")

        merged_country_graph = merge_contry_graph(gameidx=gameidx, gcs_path_1=st1, gcs_path_2=st2, bucket=bucket)
        if_else_length(path=merged_country_graph, gameidx=gameidx, service_sub=service_sub, func_name="merge_contry_graph")

        merged_os_graph = merge_os_graph(gameidx=gameidx, gcs_path_1=st3, gcs_path_2=st4, bucket=bucket)
        if_else_length(path=merged_os_graph, gameidx=gameidx, service_sub=service_sub, func_name="merge_os_graph")

        try:
            country_data_upload_to_notion(
                gameidx=gameidx,
                st1 = st1,
                st2 = st2,
                service_sub=service_sub,
                genai_client=genai_client,
                MODEL_NAME = MODEL_NAME,
                SYSTEM_INSTRUCTION=SYSTEM_INSTRUCTION,
                notion=notion,
                bucket=bucket,
                headers_json=headers_json,
                NOTION_TOKEN=NOTION_TOKEN,
                NOTION_VERSION=NOTION_VERSION,
            )
        except Exception as e:
            print(f"❌ {gameidx}: {service_sub} country_data_upload_to_notion 실패 ")
            print(f"🔴 {e}")

        try:
            os_data_upload_to_notion(
                gameidx=gameidx,
                st1 = st3,
                st2 = st4,
                service_sub=service_sub,
                genai_client=genai_client,
                MODEL_NAME = MODEL_NAME,
                SYSTEM_INSTRUCTION=SYSTEM_INSTRUCTION,
                notion=notion,
                bucket=bucket,
                headers_json=headers_json,
                NOTION_TOKEN=NOTION_TOKEN,
                NOTION_VERSION=NOTION_VERSION,
            )
        except Exception as e:
            print(f"❌ {gameidx}: {service_sub} os_data_upload_to_notion 실패 ")
            print(f"🔴 {e}")  

        st5 = country_group_rev(joyplegameid=joyplegameid, gameidx=gameidx, bigquery_client=bigquery_client, bucket=bucket)
        if_else_length(path=st5, gameidx=gameidx, service_sub=service_sub, func_name="country_group_rev")        

        st6 = cohort_by_country_cost(joyplegameid=joyplegameid, gameidx=gameidx, bigquery_client=bigquery_client, bucket=bucket)
        if_else_length(path=st6, gameidx=gameidx, service_sub=service_sub, func_name="cohort_by_country_cost")

        merged_country_group_graph = merge_country_group_df_draw(joyplegameid=joyplegameid, gameidx=gameidx, bigquery_client=bigquery_client, bucket=bucket)
        if_else_length(path=merged_country_group_graph, gameidx=gameidx, service_sub=service_sub, func_name="merge_country_group_df_draw")  

        try:
            country_group_data_upload_to_notion(
                joyplegameid=joyplegameid,
                gameidx=gameidx,
                st1 = st5,
                st2 = st6,
                service_sub=service_sub,
                genai_client=genai_client,
                MODEL_NAME = MODEL_NAME,
                SYSTEM_INSTRUCTION=SYSTEM_INSTRUCTION,
                notion=notion,
                bigquery_client=bigquery_client,
                bucket=bucket,
                headers_json=headers_json,
                NOTION_TOKEN=NOTION_TOKEN,
                NOTION_VERSION=NOTION_VERSION
            )
        except Exception as e:
            print(f"❌ {gameidx}: {service_sub} country_group_data_upload_to_notion 실패 ")
            print(f"🔴 {e}") 


    ##### R Group, IAP, GEM, RUBY 프레임 워크
    # def rgroup_iapgemruby_data_game_framework(joyplegameid:int, gameidx:str, service_sub:str, bigquery_client, notion, MODEL_NAME:str, SYSTEM_INSTRUCTION:list, genai_client, bucket, headers_json): 
    #     print(f"📧 RUN R Group, IAP, GEM, RUBY 데이터 게임 프로엠워크 시작: {gameidx}")

    #     st1 = rev_group_rev_pu()



########## TASK 설정 ##########

    create_gameframework_notion_page = PythonOperator(
        task_id='make_gameframework_notion_page_wraper',
        python_callable=make_gameframework_notion_page_wraper,
        dag=dag,
    )


    # daily_gameframework_run = PythonOperator(
    #     task_id='datily_data_game_framework',
    #     python_callable=daily_data_game_framework,
    #     op_kwargs={
    #         'joyplegameid':joyplegameid,
    #         'gameidx':gameidx,
    #         'service_sub':service_sub[0],
    #         'bigquery_client':bigquery_client,
    #         'MODEL_NAME': MODEL_NAME,
    #         'SYSTEM_INSTRUCTION': SYSTEM_INSTRUCTION,
    #         'bucket': bucket,
    #         'headers_json': headers_json,
    #         'genai_client': genai_client,
    #         'notion':notion
    #     },
    #     dag=dag,
    # )

    # inhouse_gameframework_run = PythonOperator(
    #     task_id='inhouse_data_game_framework',
    #     python_callable=inhouse_data_game_framework,
    #     op_kwargs={
    #         'joyplegameid':joyplegameid,
    #         'gameidx':gameidx,
    #         'service_sub':service_sub[1],
    #         'bigquery_client':bigquery_client,
    #         'MODEL_NAME': MODEL_NAME,
    #         'SYSTEM_INSTRUCTION': SYSTEM_INSTRUCTION,
    #         'bucket': bucket,
    #         'headers_json': headers_json,
    #         'genai_client': genai_client,
    #         'notion':notion
    #     },
    #     dag=dag,
    # )

    global_ua_gameframework_run = PythonOperator(
        task_id='global_ua_data_game_framework',
        python_callable=global_ua_data_game_framework,
        op_kwargs={
            'joyplegameid':joyplegameid,
            'gameidx':gameidx,
            'service_sub':service_sub[2],
            'bigquery_client':bigquery_client,
            'MODEL_NAME': MODEL_NAME,
            'SYSTEM_INSTRUCTION': SYSTEM_INSTRUCTION,
            'bucket': bucket,
            'headers_json': headers_json,
            'genai_client': genai_client,
            'notion':notion
        },
        dag=dag,
    )


create_gameframework_notion_page >> global_ua_gameframework_run

