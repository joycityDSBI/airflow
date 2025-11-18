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
from notion_client import Client
import requests
import json
from datetime import datetime, timezone, timedelta
from adjustText import adjust_text
from airflow.models import Variable
from zoneinfo import ZoneInfo  # Python 3.9 이상
from pathlib import Path
from airflow.sdk import get_current_context
from game_framework_util import *



PROJECT_ID = "data-science-division-216308"
LOCATION = "us-central1"

def game_framework_summary_gemini(gameidx:str, genai_client, bucket, text_path_list:list, **context):
    RUN_ID = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    LABELS = {"datascience_division_service": "gameinsight_framework",
            "run_id": RUN_ID,
            "datascience_division_service_sub" : "7_etc"} ## 딕셔너리 형태로 붙일 수 있음.
    
    summary_list = []
    
    for text_path in text_path_list:
        text_list = text_path.split("/")
        print(f"📥 GCS에서 프롬프트 텍스트 불러오는 중...: {text_list[0]}")
        prompt_text = load_df_from_gcs(bucket=bucket, path=f"{gameidx}/{text_list[0]}")
        summary_list.append(prompt_text)
    print("✅ 프롬프트 텍스트 불러오기 완료.")


    print("🤖 제미나이 요약 생성 중...")
    response_summary = genai_client.models.generate_content(
    model=MODEL_NAME,
    contents = f"""
    아래 내용을 10줄 이내로 요약해줘.
    요약한 내용은 ~~ 입니다 이런말 하지말고 그냥 바로 요약한 내용만 알려줘.
    {summary_list}

    <서식 요구사항>
    1. 한문장당 줄바꿈 한번 해줘.
    3. 한 문장마다 노션의 마크다운 리스트 문법을 사용해줘. e.g. * 당월 매출은 이렇습니다.

    """
    ,
    config=types.GenerateContentConfig(
            system_instruction=[
                ""
            ],
            # tools=[RAG],
            temperature=0.5
            ,labels=LABELS

        )
    )

    print("🤖 제미나이 요약 생성 완료.")
    # 코멘트 출력
    print(response_summary.text)
    return response_summary.text


def game_framework_summary_upload_notion(gameidx:str, service_sub:str, genai_client, bucket, text_path_list:list, notion: Client, **context):

    current_context = get_current_context()
    
    PAGE_INFO=current_context['task_instance'].xcom_pull(
        task_ids = 'make_gameframework_notion_page_wraper',
        key='page_info'
    )

    notion.blocks.children.append(
        PAGE_INFO['id'],
        children=[
            {
                "object": "block",
                "type": "heading_2",
                "heading_2": {
                    "rich_text": [{"type": "text", "text": {"content": "⭐요약" }}]
                },
            }
        ],
    )

    ## 요약 내용
    print('📝 GEMINI 요약 내용 생성 중...')
    text = game_framework_summary_gemini(gameidx=gameidx, service_sub=service_sub, genai_client=genai_client, bucket=bucket, text_path_list=text_path_list, **context)
    print('📝 GEMINI 요약 내용 생성 완료!!!!')
    blocks = md_to_notion_blocks(text)

    notion.blocks.children.append(
        block_id=PAGE_INFO['id'],
        children=blocks
    )
