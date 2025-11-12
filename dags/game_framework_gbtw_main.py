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

# 게임 프레임워크 모듈
from game_framework_util import *
from game_framework_daily import *

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


    # 클라이언트 모음
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
        '3'
    ]

    ## 페이지 생성 함수 //////////// task 함수

    def make_gameframework_notion_page_wraper(**context):
        try:
            make_gameframework_notion_page(
                gameidx=gameidx,
                NOTION_TOKEN=NOTION_TOKEN,
                DATABASE_ID=DATABASE_ID,
                notion = notion
                )
            print(f"✅ {gameidx} NOTION 페이지 생성 완료")
        except Exception as e:
            print(f"❌ {gameidx} NOTION 페이지 생성 실패")
            print(f"🔴 {e}")
            

    ####### 일자별 게임 프레임 워크

    def daily_data_game_framework(joyplegameid:int, gameidx:str, service_sub:str, bigquery_client, notion, MODEL_NAME:str, SYSTEM_INSTRUCTION:list, genai_client, bucket, headers_json): 
        
        print(f"📧 RUN 데일리 데이터 게임 프로엠워크 시작: {gameidx}")
        
        st1 = Daily_revenue_query(joyplegameid=joyplegameid, gameidx=gameidx, bigquery_client=bigquery_client, bucket=bucket)
        if st1 == True:
            print(f"✅ {gameidx}: {service_sub} Daily_revenue_query 완료")
        else :
            print(f"❌ {gameidx}: {service_sub} Daily_revenue_query 실패")

        st2 = Daily_revenue_YOY_query(joyplegameid=joyplegameid, gameidx=gameidx, bigquery_client=bigquery_client, bucket=bucket)
        if st2 == True:
            print(f"✅ {gameidx}: {service_sub} Daily_revenue_YOY_query 완료")
        else :
            print(f"❌ {gameidx}: {service_sub} Daily_revenue_YOY_query 실패")

        st3 = Daily_revenue_target_revenue_query(joyplegameid=joyplegameid, gameidx=gameidx, bigquery_client=bigquery_client, bucket=bucket)
        if st3 == True:
            print(f"✅ {gameidx}: {service_sub} Daily_revenue_target_revenue_query 완료")
        else :
            print(f"❌ {gameidx}: {service_sub} Daily_revenue_target_revenue_query 실패")
        
        s_total = merge_daily_revenue(st1, st2, bucket=bucket)
        if len(s_total) > 0:
            print(f"✅ {gameidx}: {service_sub} merge_daily_revenue 완료")
        else :
            print(f"❌ {gameidx}: {service_sub} merge_daily_revenue 실패")

        img_gcs_path = merge_daily_graph(joyplegameid=joyplegameid, gameidx=gameidx, bucket=bucket)
        if len(img_gcs_path) > 0:
            print(f"✅ {gameidx}: {service_sub} merge_daily_graph 완료")
        else :
            print(f"❌ {gameidx}: {service_sub} merge_daily_graph 실패")

        try :
            daily_revenue_data_upload_to_notion(
                gameidx=gameidx, 
                service_sub=service_sub[0], 
                genai_client=genai_client, 
                MODEL_NAME=MODEL_NAME, 
                SYSTEM_INSTRUCTION=SYSTEM_INSTRUCTION, 
                notion=notion, 
                bucket=bucket, 
                headers_json=headers_json
            )
            print(f"✅ {gameidx}: {service_sub} daily_revenue_data_upload_to_notion 완료")
        except Exception as e:
            print(f"❌ {gameidx}: {service_sub} daily_revenue_data_upload_to_notion 실패 ")
            print(f"🔴 {e}")



########## TASK 설정 ##########

    create_gameframework_notion_page = PythonOperator(
        task_id='make_gameframework_notion_page_wraper',
        python_callable=make_gameframework_notion_page_wraper,
        dag=dag,
    )


    daily_gameframework_run = PythonOperator(
        task_id='datily_data_game_framework',
        python_callable=daily_data_game_framework,
        op_kwargs={
            'joyplegameid':joyplegameid,
            'gameidx':gameidx,
            'service_sub':service_sub[0],
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

create_gameframework_notion_page >> daily_gameframework_run

