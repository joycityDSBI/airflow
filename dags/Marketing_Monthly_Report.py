from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# 폰트 캐시 재구축
import matplotlib.font_manager as fm

# 라이브러리 import
import os
import re
import random
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# 데이터 처리 및 시각화
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib as mpl
from matplotlib.ticker import PercentFormatter
import dataframe_image as dfi
from PIL import Image

# Google Cloud 관련
from google.cloud import bigquery
from google.cloud import aiplatform
import pandas_gbq

# Gemini AI 관련
# import vertexai
# from vertexai.generative_models import GenerativeModel
# import google.generativeai as genai
# from google.generativeai import GenerativeModel as GeminiModel # 이름 충돌 방지를 위해 별칭 사용

#Gemini 3.0 관련
# !pip install --upgrade google-genai
from google.genai import Client as GeminiClient
from google.genai.types import GenerateContentConfig
from google.genai import types

# Notion API
from notion_client import Client as NotionClient

# 웹 관련 (HTML, CSS 렌더링 등)
import nest_asyncio
from jinja2 import Template
from playwright.async_api import async_playwright
import asyncio

# IPython 디스플레이
from IPython.display import display

def get_var(key: str, default: str = None) -> str:
    """환경 변수 또는 Airflow Variable 조회"""
    return os.environ.get(key) or Variable.get(key, default_var=default)

# gemini 설정
os.environ['GOOGLE_CLOUD_PROJECT'] = 'data-science-division-216308'
os.environ['GOOGLE_CLOUD_LOCATION'] = 'us-central1'  #global

# 한글 폰트 지정: 먼저 설치된 것을 우선으로, 없으면 다음 후보로 폴백
mpl.rcParams["font.family"] = ["Noto Sans CJK KR", "NanumGothic", "DejaVu Sans"]
mpl.rcParams["axes.unicode_minus"] = False  # 마이너스 깨짐 방지

names = sorted({f.name for f in fm.fontManager.ttflist})
[k for k in names if "Noto" in k or "Nanum" in k][:50]
names

NOTION_TOKEN = get_var("NOTION_TOKEN_MS")  # Airflow Variable에 저장된 Notion 통합 토큰
NOTION_VERSION = "2022-06-28"

### beta released
NOTION_PAGE_ID = "24cea67a56818059a90aee3f616bc263" # 분석 결과를 작성할 Notion 페이지의 ID
NOTION_DATABASE_ID = "279ea67a5681807fb943e9894bad5c57"
author_person_id = 'a1a4ce7f-cf37-40b2-a1ef-8f00877e76ae'  #작성자

ref_person_ids= [ 'ebd0514a-939d-4c80-bb34-f1413478d9d9',  #오치성
                  '7b68811e-e587-45a1-8ad2-940c87dadf9a',  #이한나리
                 '5e777130-5039-4f71-9ac7-64645f674737' , #박준승
                 '262e5f51-9d68-4444-9713-5f1506b3eead' , #이병선
                 '23c62fe1-a573-4b3f-b12c-c7df7dbe8c9b' , #진정완
                 'ae87a94b-cf69-41fd-ae37-b0385b4e4bdf' , #박민재
                  '299d872b-594c-8174-9e5a-00028da23485', # 김도영
                  '645651d3-c051-40ae-b551-b0c4ef4b49f1', #계동균
                  '096802f3-3ae8-4e2d-bc06-911d6dc4052c', #신정엽
                 '8658b12e-cf6b-4247-abc2-c346381951ad'  #전자람
]

## 마크다운 형식을 노션에 그대로 적용시켜주는 함수
# 👉 Markdown 내 **굵게** 처리 변환
def parse_rich_text(md_text):
    """
    '**굵게**' → Notion rich_text [{"text": {...}, "annotations": {"bold": True}}]
    """
    parts = re.split(r"(\*\*.*?\*\*)", md_text)  # **...** 기준 split
    rich_text = []
    for part in parts:
        if part.startswith("**") and part.endswith("**"):
            rich_text.append({
                "type": "text",
                "text": {"content": part[2:-2]},
                "annotations": {"bold": True}
            })
        else:
            if part:
                rich_text.append({
                    "type": "text",
                    "text": {"content": part}
                })
    return rich_text

# 👉 Markdown을 Notion Blocks로 변환
def md_to_notion_blocks(md_text, blank_blocks=3):
    blocks = []
    lines = md_text.splitlines()
    stack = [blocks]  # 현재 계층 추적

    def detect_indent_unit(lines):
        indents = []
        for line in lines:
            if line.lstrip().startswith(("* ", "- ", "+ ")):  # 리스트 문법 감지
                indent = len(line) - len(line.lstrip())
                if indent > 0:
                    indents.append(indent)
        return min(indents) if indents else 4  # fallback = 4칸
    indent_unit = detect_indent_unit(lines)

    i = 0
    while i < len(lines):
        line = lines[i].rstrip()
        if not line:
            i += 1
            continue

        # Heading 처리
        if line.startswith("# "):
            stack = [blocks]
            stack[-1].append({
                "object": "block",
                "type": "heading_1",
                "heading_1": {"rich_text": parse_rich_text(line[2:])}
            })
        elif line.startswith("## "):
            stack = [blocks]
            stack[-1].append({
                "object": "block",
                "type": "heading_2",
                "heading_2": {"rich_text": parse_rich_text(line[3:])}
            })
        elif line.startswith("### "):
            stack = [blocks]
            stack[-1].append({
                "object": "block",
                "type": "heading_3",
                "heading_3": {"rich_text": parse_rich_text(line[4:])}
            })

        # 리스트 처리
        elif line.lstrip().startswith(("* ", "- ", "+ ")):
            indent = len(line) - len(line.lstrip())  # 들여쓰기 레벨
            content = line.strip()[2:].strip()

            # 다음 줄이 들여쓰기가 더 깊은지 확인하여 자식 블록이 있는지 판단
            has_children = False
            if i + 1 < len(lines):
                next_line = lines[i+1]
                next_indent = len(next_line) - len(next_line.lstrip())
                if next_indent > indent:
                    has_children = True

            block_data = {
                "rich_text": parse_rich_text(content),
            }
            # 자식 블록이 있을 경우에만 'children' 키를 추가
            if has_children:
                block_data["children"] = []

            block = {
                "object": "block",
                "type": "bulleted_list_item",
                "bulleted_list_item": block_data
            }

            # indent 기반 계층 처리
            level = indent // indent_unit + 1
            while len(stack) > level:
                stack.pop()
            stack[-1].append(block)

            # 자식 블록이 있는 경우에만 스택에 자식 목록을 추가
            if has_children:
                stack.append(block["bulleted_list_item"]["children"])
            else:
                # 자식 블록이 없는 경우 현재 레벨로 스택 재설정
                stack = stack[:level]

        else:
            stack = [blocks]
            # 일반 문단
            stack[-1].append({
                "object": "block",
                "type": "paragraph",
                "paragraph": {"rich_text": parse_rich_text(line.strip())}
            })

        i += 1

    # ✅ 마지막에 빈 블록 추가 (개수는 파라미터 blank_blocks로 제어)
    for _ in range(blank_blocks):
        blocks.append({
            "object": "block",
            "type": "paragraph",
            "paragraph": {"rich_text": []}
        })

    return blocks

def mkt_monthly_report_total():
    from datetime import datetime, timezone, timedelta
    RUN_ID = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    LABELS = {"datascience_division_service": "monthly_mkt_framework",
            "run_id": RUN_ID,
            "datascience_division_service_sub" : "mkt_monthly_1_total_roas"} ## 딕셔너리 형태로 붙일 수 있음.
    print("RUN_ID=", RUN_ID, "LABEL_ID=", LABELS)

    client = bigquery.Client()
    query = """
    WITH revraw AS(
    select  JoypleGameID, Month
    ,sum(RU) as RU,#
    sum(Sales_D1) as sales_D1,
    sum(Sales_D3) as sales_D3,
    sum(Sales_D7) as sales_D7,
    CASE WHEN COUNTIF(sales_D14 IS NULL) >= 1 THEN null ELSE sum(Sales_D14) END as Sales_D14,
    CASE WHEN COUNTIF(Sales_D30 IS NULL) >= 1 THEN null ELSE sum(Sales_D30) END as Sales_D30,
    CASE WHEN COUNTIF(Sales_D60 IS NULL) >= 1 THEN null ELSE sum(Sales_D60) END as Sales_D60,
    CASE WHEN COUNTIF(Sales_D90 IS NULL) >= 1 THEN null ELSE sum(Sales_D90) END as Sales_D90,
    CASE WHEN COUNTIF(Sales_D120 IS NULL) >= 1 THEN null ELSE sum(Sales_D120) END as Sales_D120,
    CASE WHEN COUNTIF(Sales_D150 IS NULL) >= 1 THEN null ELSE sum(Sales_D150) END as Sales_D150,
    CASE WHEN COUNTIF(Sales_D180 IS NULL) >= 1 THEN null ELSE sum(Sales_D180) END as Sales_D180,
    CASE WHEN COUNTIF(Sales_D210 IS NULL) >= 1 THEN null ELSE sum(Sales_D210) END as Sales_D210,
    CASE WHEN COUNTIF(Sales_D240 IS NULL) >= 1 THEN null ELSE sum(Sales_D240) END as Sales_D240,
    CASE WHEN COUNTIF(Sales_D270 IS NULL) >= 1 THEN null ELSE sum(Sales_D270) END as Sales_D270,
    CASE WHEN COUNTIF(Sales_D300 IS NULL) >= 1 THEN null ELSE sum(Sales_D300) END as Sales_D300,
    CASE WHEN COUNTIF(Sales_D330 IS NULL) >= 1 THEN null ELSE sum(Sales_D330) END as Sales_D330,
    CASE WHEN COUNTIF(Sales_D360 IS NULL) >= 1 THEN null ELSE sum(Sales_D360) END as Sales_D360,
    from(
    select JoypleGameID, RegdateAuthAccountDateKST,
    FORMAT_DATE('%Y-%m' ,RegdateAuthAccountDateKST) as Month,
    sum(RU) as RU,
    IFNULL(sum(rev_D1),0) as Sales_D1,
    IFNULL(sum(rev_D3),0) as Sales_D3,
    IFNULL(sum(rev_D7),0) as Sales_D7,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -15 DAY) AS Date)  THEN  IFNULL(sum(rev_D14),0) ELSE  null END as Sales_D14,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -31 DAY) AS Date)  THEN  IFNULL(sum(rev_D30),0) ELSE  null END as Sales_D30,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -61 DAY) AS Date)  THEN  IFNULL(sum(rev_D60),0) ELSE  null END as Sales_D60,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -91 DAY) AS Date)  THEN  IFNULL(sum(rev_D90),0) ELSE  null END as Sales_D90,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -121 DAY) AS Date)  THEN  IFNULL(sum(rev_D120),0) ELSE  null END as Sales_D120,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -151 DAY) AS Date)  THEN  IFNULL(sum(rev_D150),0) ELSE  null END as Sales_D150,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -181 DAY) AS Date)  THEN  IFNULL(sum(rev_D180),0) ELSE  null END as Sales_D180,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -211 DAY) AS Date)  THEN  IFNULL(sum(rev_D210),0) ELSE  null END as Sales_D210,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -241 DAY) AS Date)  THEN  IFNULL(sum(rev_D240),0) ELSE  null END as Sales_D240,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -271 DAY) AS Date)  THEN  IFNULL(sum(rev_D270),0) ELSE  null END as Sales_D270,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -301 DAY) AS Date)  THEN  IFNULL(sum(rev_D300),0) ELSE  null END as Sales_D300,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -331 DAY) AS Date)  THEN  IFNULL(sum(rev_D330),0) ELSE  null END as Sales_D330,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -361 DAY) AS Date)  THEN  IFNULL(sum(rev_D360),0) ELSE  null END as Sales_D360
    from `dataplatform-reporting.DataService.T_0420_0000_UAPerformanceRaw_V1`
        where JoypleGameID in (131,133,30001,30003)
    and (JoypleGameID = 131 AND RegdateAuthAccountDateKST BETWEEN '2021-01-01' AND DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 8 DAY))
    OR (JoypleGameID = 133 AND RegdateAuthAccountDateKST BETWEEN '2021-01-01' AND DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 8 DAY))
    OR (JoypleGameID = 30001 AND RegdateAuthAccountDateKST BETWEEN '2022-05-01' AND DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 8 DAY))
    OR (JoypleGameID = 30003 AND RegdateAuthAccountDateKST BETWEEN '2024-01-01' AND DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 8 DAY))
    group by JoypleGameID, RegdateAuthAccountDateKST
    ) group by JoypleGameID, month
    )


    , final AS(
    select  JoypleGameID,  Month, RU,
    Sales_D1/RU as D1_LTV,
    Sales_D3/RU as D3_LTV,
    Sales_D7/RU as D7_LTV,
    Sales_D14/RU as D14_LTV,
    Sales_D30/RU as D30_LTV,
    Sales_D60/RU as D60_LTV,
    Sales_D90/RU as D90_LTV,
    Sales_D120/RU as D120_LTV,
    Sales_D150/RU as D150_LTV,
    Sales_D180/RU as D180_LTV,
    Sales_D210/RU as D210_LTV,
    Sales_D240/RU as D240_LTV,
    Sales_D270/RU as D270_LTV,
    Sales_D300/RU as D300_LTV,
    Sales_D330/RU as D330_LTV,
    Sales_D360/RU as D360_LTV,
    Sales_D14_p/RU as D14_LTV_p,
    Sales_D30_p/RU as D30_LTV_p,
    Sales_D60_p/RU as D60_LTV_p,
    Sales_D90_p/RU as D90_LTV_p,
    Sales_D120_p/RU as D120_LTV_p,
    Sales_D150_p/RU as D150_LTV_p,
    Sales_D180_p/RU as D180_LTV_p,
    Sales_D210_p/RU as D210_LTV_p,
    Sales_D240_p/RU as D240_LTV_p,
    Sales_D270_p/RU as D270_LTV_p,
    Sales_D300_p/RU as D300_LTV_p,
    Sales_D330_p/RU as D330_LTV_p,
    Sales_D360_p/RU as D360_LTV_p,
    D1D3_avg,  D3D7_avg , Sales_D7/RU as kpi_d7
    from(
    select *,
    CASE WHEN Sales_D14 is not null then null  ELSE  Sales_D7*D7D14_avg END as Sales_D14_p,
    CASE WHEN Sales_D30 is not null then null
    WHEN Sales_D30 is null and Sales_D14 is not null THEN Sales_D14*D14D30_avg
    ELSE Sales_D7*D7D14_avg*D14D30_avg END as Sales_D30_p,
    CASE WHEN Sales_D60 is not null then null
    WHEN Sales_D30 is not null THEN Sales_D30*D30D60_avg
    WHEN Sales_D14 is not null  THEN Sales_D14*D14D30_avg*D30D60_avg
    ELSE Sales_D7*D7D14_avg*D14D30_avg*D30D60_avg END as Sales_D60_p,
    CASE WHEN Sales_D90 is not null then null
    WHEN Sales_D60 is not null THEN Sales_D60*D60D90_avg
    WHEN Sales_D30 is not null THEN Sales_D30*D30D60_avg*D60D90_avg
    WHEN Sales_D14 is not null  THEN Sales_D14*D14D30_avg*D30D60_avg*D60D90_avg
    ELSE Sales_D7*D7D14_avg*D14D30_avg*D30D60_avg*D60D90_avg END as Sales_D90_p,
    CASE WHEN Sales_D120 is not null then null
    WHEN Sales_D90 is not null THEN Sales_D90*D90D120_avg
    WHEN Sales_D60 is not null THEN Sales_D60*D60D90_avg*D90D120_avg
    WHEN Sales_D30 is not null THEN Sales_D30*D30D60_avg*D60D90_avg*D90D120_avg
    WHEN Sales_D14 is not null  THEN Sales_D14*D14D30_avg*D30D60_avg*D60D90_avg*D90D120_avg
    ELSE Sales_D7*D7D14_avg*D14D30_avg*D30D60_avg*D60D90_avg*D90D120_avg  END as Sales_D120_p,
    CASE WHEN Sales_D150 is not null then null
    WHEN Sales_D120 is not null THEN Sales_D120*D120D150_avg
    WHEN Sales_D90 is not null THEN Sales_D90*D90D120_avg*D120D150_avg
    WHEN Sales_D60 is not null THEN Sales_D60*D60D90_avg*D90D120_avg*D120D150_avg
    WHEN Sales_D30 is not null THEN Sales_D30*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg
    WHEN Sales_D14 is not null  THEN Sales_D14*D14D30_avg*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg
    ELSE Sales_D7*D7D14_avg*D14D30_avg*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg   END as Sales_D150_p,
    CASE WHEN Sales_D180 is not null then null
    WHEN Sales_D150 is not null THEN Sales_D150*D150D180_avg
    WHEN Sales_D120 is not null THEN Sales_D120*D120D150_avg*D150D180_avg
    WHEN Sales_D90 is not null THEN Sales_D90*D90D120_avg*D120D150_avg*D150D180_avg
    WHEN Sales_D60 is not null THEN Sales_D60*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg
    WHEN Sales_D30 is not null THEN Sales_D30*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg
    WHEN Sales_D14 is not null  THEN Sales_D14*D14D30_avg*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg
    ELSE Sales_D7*D7D14_avg*D14D30_avg*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg    END as Sales_D180_p,
    CASE WHEN Sales_D210 is not null then null
    WHEN Sales_D180 is not null THEN Sales_D180*D180D210_avg
    WHEN Sales_D150 is not null THEN Sales_D150*D150D180_avg*D180D210_avg
    WHEN Sales_D120 is not null THEN Sales_D120*D120D150_avg*D150D180_avg*D180D210_avg
    WHEN Sales_D90 is not null THEN Sales_D90*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg
    WHEN Sales_D60 is not null THEN Sales_D60*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg
    WHEN Sales_D30 is not null THEN Sales_D30*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg
    WHEN Sales_D14 is not null  THEN Sales_D14*D14D30_avg*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg
    ELSE Sales_D7*D7D14_avg*D14D30_avg*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg     END as Sales_D210_p,
    CASE WHEN Sales_D240 is not null then null
    WHEN Sales_D210 is not null THEN Sales_D210*D210D240_avg
    WHEN Sales_D180 is not null THEN Sales_D180*D180D210_avg*D210D240_avg
    WHEN Sales_D150 is not null THEN Sales_D150*D150D180_avg*D180D210_avg*D210D240_avg
    WHEN Sales_D120 is not null THEN Sales_D120*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg
    WHEN Sales_D90 is not null THEN Sales_D90*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg
    WHEN Sales_D60 is not null THEN Sales_D60*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg
    WHEN Sales_D30 is not null THEN Sales_D30*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg
    WHEN Sales_D14 is not null  THEN Sales_D14*D14D30_avg*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg
    ELSE Sales_D7*D7D14_avg*D14D30_avg*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg      END as Sales_D240_p,
    CASE WHEN Sales_D270 is not null then null
    WHEN Sales_D240 is not null THEN Sales_D240*D240D270_avg
    WHEN Sales_D210 is not null THEN Sales_D210*D210D240_avg*D240D270_avg
    WHEN Sales_D180 is not null THEN Sales_D180*D180D210_avg*D210D240_avg*D240D270_avg
    WHEN Sales_D150 is not null THEN Sales_D150*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg
    WHEN Sales_D120 is not null THEN Sales_D120*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg
    WHEN Sales_D90 is not null THEN Sales_D90*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg
    WHEN Sales_D60 is not null THEN Sales_D60*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg
    WHEN Sales_D30 is not null THEN Sales_D30*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg
    WHEN Sales_D14 is not null  THEN Sales_D14*D14D30_avg*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg
    ELSE Sales_D7*D7D14_avg*D14D30_avg*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg       END as Sales_D270_p,
    CASE WHEN Sales_D300 is not null then null
    WHEN Sales_D270 is not null THEN Sales_D270*D270D300_avg
    WHEN Sales_D240 is not null THEN Sales_D240*D240D270_avg*D270D300_avg
    WHEN Sales_D210 is not null THEN Sales_D210*D210D240_avg*D240D270_avg*D270D300_avg
    WHEN Sales_D180 is not null THEN Sales_D180*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg
    WHEN Sales_D150 is not null THEN Sales_D150*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg
    WHEN Sales_D120 is not null THEN Sales_D120*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg
    WHEN Sales_D90 is not null THEN Sales_D90*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg
    WHEN Sales_D60 is not null THEN Sales_D60*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg
    WHEN Sales_D30 is not null THEN Sales_D30*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg
    WHEN Sales_D14 is not null  THEN Sales_D14*D14D30_avg*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg
    ELSE Sales_D7*D7D14_avg*D14D30_avg*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg        END as Sales_D300_p,
    CASE WHEN Sales_D330 is not null then null
    WHEN Sales_D300 is not null THEN Sales_D300*D300D330_avg
    WHEN Sales_D270 is not null THEN Sales_D270*D270D300_avg*D300D330_avg
    WHEN Sales_D240 is not null THEN Sales_D240*D240D270_avg*D270D300_avg*D300D330_avg
    WHEN Sales_D210 is not null THEN Sales_D210*D210D240_avg*D240D270_avg*D270D300_avg*D300D330_avg
    WHEN Sales_D180 is not null THEN Sales_D180*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg*D300D330_avg
    WHEN Sales_D150 is not null THEN Sales_D150*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg*D300D330_avg
    WHEN Sales_D120 is not null THEN Sales_D120*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg*D300D330_avg
    WHEN Sales_D90 is not null THEN Sales_D90*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg*D300D330_avg
    WHEN Sales_D60 is not null THEN Sales_D60*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg*D300D330_avg
    WHEN Sales_D30 is not null THEN Sales_D30*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg*D300D330_avg
    WHEN Sales_D14 is not null  THEN Sales_D14*D14D30_avg*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg*D300D330_avg
    ELSE Sales_D7*D7D14_avg*D14D30_avg*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg*D300D330_avg         END as Sales_D330_p,
    CASE WHEN Sales_D360 is not null then null
    WHEN Sales_D330 is not null THEN Sales_D330*D330D360_avg
    WHEN Sales_D300 is not null THEN Sales_D300*D300D330_avg *D330D360_avg
    WHEN Sales_D270 is not null THEN Sales_D270*D270D300_avg*D300D330_avg  *D330D360_avg
    WHEN Sales_D240 is not null THEN Sales_D240*D240D270_avg*D270D300_avg*D300D330_avg   *D330D360_avg
    WHEN Sales_D210 is not null THEN Sales_D210*D210D240_avg*D240D270_avg*D270D300_avg*D300D330_avg    *D330D360_avg
    WHEN Sales_D180 is not null THEN Sales_D180*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg*D300D330_avg *D330D360_avg
    WHEN Sales_D150 is not null THEN Sales_D150*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg*D300D330_avg    *D330D360_avg
    WHEN Sales_D120 is not null THEN Sales_D120*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg*D300D330_avg  *D330D360_avg
    WHEN Sales_D90 is not null THEN Sales_D90*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg*D300D330_avg *D330D360_avg
    WHEN Sales_D60 is not null THEN Sales_D60*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg*D300D330_avg   *D330D360_avg
    WHEN Sales_D30 is not null THEN Sales_D30*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg*D300D330_avg     *D330D360_avg
    WHEN Sales_D14 is not null  THEN Sales_D14*D14D30_avg*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg*D300D330_avg     *D330D360_avg
    ELSE Sales_D7*D7D14_avg*D14D30_avg*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg*D300D330_avg*D330D360_avg          END as Sales_D360_p
    from(

    select *
    ,  LAST_VALUE(d1d3_avg3 IGNORE NULLS ) over(partition by joyplegameid ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  as D1D3_avg
    ,  LAST_VALUE(d3d7_avg3 IGNORE NULLS ) over(partition by joyplegameid ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  as D3D7_avg
    ,  LAST_VALUE(d7d14_avg3 IGNORE NULLS ) over(partition by joyplegameid ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  as D7D14_avg
    ,  LAST_VALUE(d14d30_avg3 IGNORE NULLS ) over(partition by joyplegameid ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as D14D30_avg
    ,  LAST_VALUE(d30d60_avg3 IGNORE NULLS ) over(partition by joyplegameid ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as D30D60_avg
    ,  LAST_VALUE(d60d90_avg3 IGNORE NULLS ) over(partition by joyplegameid ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as D60D90_avg
    ,  LAST_VALUE(d90d120_avg3 IGNORE NULLS ) over(partition by joyplegameid ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as d90d120_avg
    ,  LAST_VALUE(d120d150_avg3 IGNORE NULLS ) over(partition by joyplegameid ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as d120d150_avg
    ,  LAST_VALUE(d150d180_avg3 IGNORE NULLS ) over(partition by joyplegameid ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as d150d180_avg
    ,  LAST_VALUE(d180d210_avg3 IGNORE NULLS ) over(partition by joyplegameid ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as d180d210_avg
    ,  LAST_VALUE(d210d240_avg3 IGNORE NULLS ) over(partition by joyplegameid ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as d210d240_avg
    ,  LAST_VALUE(d240d270_avg3 IGNORE NULLS ) over(partition by joyplegameid ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as d240d270_avg
    ,  LAST_VALUE(d270d300_avg3 IGNORE NULLS ) over(partition by joyplegameid ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as d270d300_avg
    ,  LAST_VALUE(d300d330_avg3 IGNORE NULLS ) over(partition by joyplegameid ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as d300d330_avg
    ,  LAST_VALUE(d330d360_avg3 IGNORE NULLS ) over(partition by joyplegameid ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as d330d360_avg

    from(
    select *,
    CASE WHEN Sales_D3 is null THEN null ELSE AVG(Sales_D3/Sales_D1) OVER (partition by joyplegameid ORDER BY month ROWS BETWEEN 3 PRECEDING AND  1 PRECEDING ) END AS d1d3_avg3, -- 현재월제외 kpi계산용
    CASE WHEN Sales_D7 is null THEN null ELSE AVG(Sales_D7/Sales_D3) OVER (partition by joyplegameid ORDER BY month ROWS BETWEEN 3 PRECEDING AND  1 PRECEDING) END AS d3d7_avg3, -- 현재월제외 kpi계산용
    CASE WHEN Sales_D14 is null THEN null ELSE AVG(Sales_D14/Sales_D7) OVER (partition by joyplegameid ORDER BY month ROWS BETWEEN 2 PRECEDING AND  CURRENT ROW ) END AS d7d14_avg3,
    CASE WHEN Sales_D30 is null THEN null ELSE AVG(Sales_D30/Sales_D14) OVER (partition by joyplegameid ORDER BY month ROWS BETWEEN 2 PRECEDING AND  CURRENT ROW ) END AS d14d30_avg3,
    CASE WHEN Sales_D60 is null THEN null ELSE AVG(Sales_D60/Sales_D30) OVER (partition by joyplegameid ORDER BY month ROWS BETWEEN 2 PRECEDING AND  CURRENT ROW ) END AS d30d60_avg3,
    CASE WHEN Sales_D90 is null THEN null ELSE AVG(Sales_D90/Sales_D60) OVER (partition by joyplegameid ORDER BY month ROWS BETWEEN 2 PRECEDING AND  CURRENT ROW ) END AS d60d90_avg3,
    CASE WHEN Sales_D120 is null THEN null ELSE AVG(Sales_D120/Sales_D90) OVER (partition by joyplegameid ORDER BY month ROWS BETWEEN 2 PRECEDING AND  CURRENT ROW ) END AS d90d120_avg3,
    CASE WHEN Sales_D150 is null THEN null ELSE AVG(Sales_D150/Sales_D120) OVER (partition by joyplegameid ORDER BY month ROWS BETWEEN 2 PRECEDING AND  CURRENT ROW ) END AS d120d150_avg3,
    CASE WHEN Sales_D180 is null THEN null ELSE AVG(Sales_D180/Sales_D150) OVER (partition by joyplegameid ORDER BY month ROWS BETWEEN 2 PRECEDING AND  CURRENT ROW ) END AS d150d180_avg3,
    CASE WHEN Sales_D210 is null THEN null ELSE AVG(Sales_D210/Sales_D180) OVER  (partition by joyplegameid ORDER BY month ROWS BETWEEN 2 PRECEDING AND  CURRENT ROW ) END AS d180d210_avg3,
    CASE WHEN Sales_D240 is null THEN null ELSE AVG(Sales_D240/Sales_D210) OVER  (partition by joyplegameid ORDER BY month ROWS BETWEEN 2 PRECEDING AND  CURRENT ROW )END AS d210d240_avg3,
    CASE WHEN Sales_D270 is null THEN null ELSE AVG(Sales_D270/Sales_D240) OVER  (partition by joyplegameid ORDER BY month ROWS BETWEEN 2 PRECEDING AND  CURRENT ROW ) END AS d240d270_avg3,
    CASE WHEN Sales_D300 is null THEN null ELSE AVG(Sales_D300/Sales_D270) OVER  (partition by joyplegameid ORDER BY month ROWS BETWEEN 2 PRECEDING AND  CURRENT ROW ) END AS d270d300_avg3,
    CASE WHEN Sales_D330 is null THEN null ELSE AVG(Sales_D330/Sales_D300) OVER  (partition by joyplegameid ORDER BY month ROWS BETWEEN 2 PRECEDING AND  CURRENT ROW ) END AS d300d330_avg3,
    CASE WHEN Sales_D360 is null THEN null ELSE AVG(Sales_D360/Sales_D330) OVER  (partition by joyplegameid ORDER BY month ROWS BETWEEN 2 PRECEDING AND  CURRENT ROW )END AS d330d360_avg3
    from revraw
    )
    )
    )
    )


    ,final2 AS(
    select a.*, b.cost, b.cost_exclude_credit
    from final as a
    left join (
    select joyplegameid,  format_date('%Y-%m', cmpgndate) as month
    , sum(costcurrency) as cost, sum(costcurrencyuptdt) as cost_exclude_credit
    from  `dataplatform-reporting.DataService.V_0410_0000_CostCampaignRule_V`
    where joyplegameid in (131,133,30001,30003)
    and cmpgndate >='2021-01-01'
    and cmpgndate <= DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 8 DAY)
    group by joyplegameid,  format_date('%Y-%m', cmpgndate)

    ) as b
    on a.joyplegameid = b.joyplegameid
    and a.month = b.month
    )



    select joyplegameid, month, cost, cost_exclude_credit, ru, cost/ru as cpru,
    ru*d1_ltv/cost_exclude_credit as d1_roas,
    ru*d3_ltv/cost_exclude_credit as d3_roas,
    ru*d7_ltv/cost_exclude_credit as d7_roas,
    ru*d14_ltv/cost_exclude_credit as d14_roas,
    ru*d30_ltv/cost_exclude_credit as d30_roas,
    ru*d60_ltv/cost_exclude_credit as d60_roas,
    ru*d90_ltv/cost_exclude_credit as d90_roas,
    ru*d120_ltv/cost_exclude_credit as d120_roas,
    ru*d150_ltv/cost_exclude_credit as d150_roas,
    ru*d180_ltv/cost_exclude_credit as d180_roas,
    ru*d210_ltv/cost_exclude_credit as d210_roas,
    ru*d240_ltv/cost_exclude_credit as d240_roas,
    ru*d270_ltv/cost_exclude_credit as d270_roas,
    ru*d300_ltv/cost_exclude_credit as d300_roas,
    ru*d330_ltv/cost_exclude_credit as d330_roas,
    ru*d360_ltv/cost_exclude_credit as d360_roas,
    ru*d14_ltv_p/cost_exclude_credit as d14_roas_p,
    ru*d30_ltv_p/cost_exclude_credit as d30_roas_p,
    ru*d60_ltv_p/cost_exclude_credit as d60_roas_p,
    ru*d90_ltv_p/cost_exclude_credit as d90_roas_p,
    ru*d120_ltv_p/cost_exclude_credit as d120_roas_p,
    ru*d150_ltv_p/cost_exclude_credit as d150_roas_p,
    ru*d180_ltv_p/cost_exclude_credit as d180_roas_p,
    ru*d210_ltv_p/cost_exclude_credit as d210_roas_p,
    ru*d240_ltv_p/cost_exclude_credit as d240_roas_p,
    ru*d270_ltv_p/cost_exclude_credit as d270_roas_p,
    ru*d300_ltv_p/cost_exclude_credit as d300_roas_p,
    ru*d330_ltv_p/cost_exclude_credit as d330_roas_p,
    ru*d360_ltv_p/cost_exclude_credit as d360_roas_p
    from final2
    """

    query_result_pltv_growth = client.query(query, job_config=bigquery.QueryJobConfig(labels=LABELS)).to_dataframe()


    ### 2> pLTV_D360
    client = bigquery.Client()
    query = """with perfo_raw AS(
    select a.*
    , b.countrycode, b.os
    , b.gcat, b.mediacategory, b.class, b.media, b.adsetname, b.adname, b.optim, b.oscam, b.geocam, b.targetgroup
    from(
    select *,
    case when logdatekst < current_date('Asia/Seoul') then pricekrw else daypred_low end as combined_rev_low,
    case when logdatekst < current_date('Asia/Seoul') then pricekrw else daypred_upp end as combined_rev_upp,
    FROM `data-science-division-216308.VU.Performance_pLTV`
    where authaccountregdatekst>='2024-01-01'
    and authaccountregdatekst <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -8 DAY) AS Date)
    and joyplegameid in (131,133)
    ) as a
    left join (select  *
    from `dataplatform-reporting.DataService.V_0316_0000_AuthAccountInfo_V`
    ) as b
    on a.authaccountname = b.authaccountname
    and a.joyplegameid = b.joyplegameid
    )



    select joyplegameid,  format_datE('%Y-%m',AuthAccountRegDateKST) as regmonth
    ,count(distinct if(daysfromregisterdate = 0, authaccountname, null)) as ru
    ,sum(if(daysfromregisterdate <= 360, combined_rev, null)) as pred_d360
    , max(authaccountregdatekst) as maxdate
    from perfo_raw
    group by joyplegameid, format_datE('%Y-%m',AuthAccountRegDateKST)
    """
    query_result_pltv_model = client.query(query, job_config=bigquery.QueryJobConfig(labels=LABELS)).to_dataframe()




    ### 3> 복귀유저
    client = bigquery.Client()
    query = """
    with raw AS(
    select *
    , sum(d90diff) over(partition by joyplegameid, authaccountname order by logdatekst) as cum_d90diff
    from(
    select *
    , date_diff(logdatekst,AuthAccountLastAccessBeforeDateKST, day ) as daydiff_beforeaccess   -- authaccountlastaccessbeforedatekst : Access 기준으로 로깅
    , case when  date_diff(logdatekst,AuthAccountLastAccessBeforeDateKST, day )  >= 90 then 1 else 0  end as d90diff
    FROM `dataplatform-reporting.DataService.T_0317_0000_AuthAccountPerformance_V`
    WHERE joyplegameid in (131,133,30001,30003)
    and logdatekst >= '2023-01-01'
    and DaysFromRegisterDate >= 0 -- 가입일이 이후에 찍힌 case제외
    )
    )

    , raw2 AS(
    select *, date_diff(logdatekst, returndate, day) as daydiff_re -- 복귀일 cohort
    -- , if(returndate = AuthAccountRegDateKST, 0,1) as return_yn -- 가입일이 먼저 찍힌 case 포함
    , if(cum_d90diff = 0, 0,1) as return_yn -- 가입일이 먼저 찍힌 case 포함
    from(
    select *
    , first_value(logdatekst) over(partition by joyplegameid, authaccountname, cum_d90diff order by logdatekst) as returndate
    from raw
    )
    )

    , ru_raw AS(
    -- 신규 유저 기준
    select joyplegameid,  format_date('%Y-%m',authaccountregdatekst) as regmonth
    , count(distinct authaccountname) as ru
    , sum(if(DaysFromRegisterDate<=360, pricekrw, null)) as d360rev
    from raw2
    where  AuthAccountRegDateKST  >= '2023-01-01'
    and AuthAccountRegDateKST <= DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 8 DAY)

    group by joyplegameid,    format_date('%Y-%m',authaccountregdatekst)
    )

    , return_raw AS(
    -- 복귀유저
    select joyplegameid,  format_date('%Y-%m', returndate) as regmonth
    , count(distinct if(daydiff_re = 0 , authaccountname, null)) as ru
    , sum(if(daydiff_re<=360, pricekrw, null)) as d360rev_all
    from raw2
    where  returndate  >= '2023-01-01'
    and returndate <= DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 8 DAY)
    group by joyplegameid,   format_date('%Y-%m', returndate)
    )


    ,final AS(
    select
    ifnull(ifnull(a.joyplegameid , b.joyplegameid) , c.joyplegameid)  as joyplegameid
    ,ifnull(ifnull(a.regmonth , b.regmonth)  , c.regmonth) as regmonth
    , a.ru ,b.ru as ru_all,
    d360rev  AS rev_D360,
    d360rev_all  AS rev_D360_all
    ,  cost
    ,  cost_exclude_credit
    , d360rev_all - d360rev as rev_D360_return ,
    case WHEN DATE_DIFF(
            DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY),
            LAST_DAY(DATE(CONCAT(ifnull(ifnull(a.regmonth , b.regmonth)  , c.regmonth) , '-01'))),
            DAY
            ) >= 360 THEN 'mature'
        ELSE 'notmature'
    END AS status
    from ru_raw  as a
    full join return_raw as b
    on a.joyplegameid = b.joyplegameid
    and a.regmonth = b.regmonth
    full join (
    select joyplegameid,  format_date('%Y-%m',cmpgndate) as regmonth, sum(costcurrency) as cost, sum(costcurrencyuptdt) as cost_exclude_credit
    from  `dataplatform-reporting.DataService.V_0410_0000_CostCampaignRule_V`
    where joyplegameid in (131,133,30001,30003)
    and cmpgndate >='2023-01-01'
    and cmpgndate <= DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 8 DAY)
    group by  joyplegameid,  format_date('%Y-%m',cmpgndate)
    ) as c
    on a.joyplegameid = c.joyplegameid
    and a.regmonth = c.regmonth
    )


    #notmature 구간 최근 6개월 평균
    #POTC 2024/4 ~ 6월 로그인 이슈로 복귀유저 기여도 낮아 제외
    , return_user_proas AS(
    select joyplegameid, avg(rev_D360_return/cost_exclude_credit) as d360_return_roas
    , approx_quantiles(rev_D360_return/cost_exclude_credit, 2)[OFFSET(1)] as d360_return_roas_med
    from(
    select *, row_number() over (partition by joyplegameid order by regmonth desc) as rownum
    from final
    where status = 'mature'
    and ((joyplegameid = 131 and regmonth not in ('2024-04','2024-05','2024-06'))
    or joyplegameid in (133,30001,30003))
    )
    where rownum <= 6 -- 최근 6개월
    group by joyplegameid
    )

    select a.*
    , rev_D360_return/cost_exclude_credit as d360_plus_return_actual
    , case when status = 'mature' then rev_D360_return/cost_exclude_credit
    else b.d360_return_roas_med  end as d360_plus_return_expected

    from final  as a
    left join return_user_proas as b
    on a.joyplegameid = b.joyplegameid

    """
    query_result_return = client.query(query, job_config=bigquery.QueryJobConfig(labels=LABELS)).to_dataframe()



    ### 4> BEP
    client = bigquery.Client()
    query = """
    with raw AS(
    select a.*, b.value
    , case when b.value is not null then b.value
    when b.value is null and a.PGName = 'Google' then 0.3
    when b.value is null and a.PGName = 'Apple' and a.joyplegameid = 131 then 0.33
    when b.value is null and a.PGName = 'Apple' and a.joyplegameid = 133 then 0.32
    when b.value is null and a.PGName = 'Apple' and a.joyplegameid = 30001 then 0.32
    when b.value is null and a.PGName = 'Apple' and a.joyplegameid = 30003 then 0.31
    when b.value is null and a.PGName = 'Xsolla' and a.joyplegameid = 131 then 0.15
    when b.value is null and  a.PGName = 'Xsolla' and a.joyplegameid = 133 then 0.10
    when b.value is null and  a.PGName = 'Xsolla' and a.joyplegameid = 30001 then 0.09
    when b.value is null and  a.PGName = 'Xsolla' and a.joyplegameid = 30003 then 0.08
    when b.value is null and  a.PGName = 'Danal' then 0.03
    when b.value is null and  a.PGName = 'One Store' and a.joyplegameid = 131 then 0.3
    when b.value is null and  a.PGName = 'One Store' and a.joyplegameid = 133 then 0.24
    when b.value is null and  a.PGName = 'One Store' and a.joyplegameid = 30001 then 0.24
    when b.value is null and  a.PGName = 'One Store' and a.joyplegameid = 30003 then 0.24
    when b.value is null and  a.PGName = 'Facebook Gaming' then 0.3
    when b.value is null and a.PGName = 'Steam' then 0.3
    else 0.3
    end as commission_rate
    from(
    select JoypleGameID, format_date('%Y-%m', authaccountregdatekst) as regmonth , t2.PGName, sum(t2.PGPriceKRW) as sales
    from  dataplatform-reporting.DataService.V_0317_0000_AuthAccountPerformance_V AS t1,
    UNNEST(t1.PaymentDetailArrayStruct) AS t2
    where joyplegameid in (131,133,30001,30003)
    and authaccountregdatekst >='2024-01-01'
    group by JoypleGameID, format_date('%Y-%m', authaccountregdatekst) , t2.PGName
    ) as a
    left join (
    select joyplegameid, regmonth, PGName,  value
    , case when PGName = 'One_Store' then 'One Store'
    when PGName = 'Facebook_Gaming' then 'Facebook Gaming'
    else PGName end as pgname2
    from `data-science-division-216308.Common.pg_commission_rate_copy2`
    unpivot (
    value for PGName in (Google,Apple, Xsolla	,Danal,	One_Store	,Facebook_Gaming,	Steam)
    )
    ) as b
    on a.joyplegameid = b.joyplegameid
    and a.regmonth = b.regmonth
    and a.pgname = b.pgname2
    )

    -- BEP 계산
    select * , sum(sales) over(partition by joyplegameid , regmonth) as cumsales
    , sales /  sum(sales) over(partition by joyplegameid , regmonth) as sales_p
    , sum(commission) over(partition by joyplegameid , regmonth) as cumcommission
    , sum(commission) over(partition by joyplegameid , regmonth) /  sum(sales) over(partition by joyplegameid , regmonth) as total_commssion_rate
    , case when joyplegameid in (131,133) then 1/(1- sum(commission) over(partition by joyplegameid , regmonth) /  sum(sales) over(partition by joyplegameid , regmonth))
    when joyplegameid in (30001,30003) then 1.08/(1- sum(commission) over(partition by joyplegameid , regmonth) /  sum(sales) over(partition by joyplegameid , regmonth))
    end as bep_commission
    from(
    select  *, sales*commission_rate as commission
    from raw
    )
    """

    query_result_pg = client.query(query, job_config=bigquery.QueryJobConfig(labels=LABELS)).to_dataframe()

    # 5> kpi roas (NY_추가)
    client = bigquery.Client()
    query = """select * from data-science-division-216308.MetaData.roas_kpi
    where userType = '신규유저'and operationStatus = '운영 중'"""

    query_result_kpi = client.query(query, job_config=bigquery.QueryJobConfig(labels=LABELS)).to_dataframe()
    query_result_pltv_growth= query_result_pltv_growth.rename(columns={'month':'regmonth'})

    # 1. dXXX_roas 패턴의 컬럼 찾기
    roas_cols = [col for col in query_result_pltv_growth.columns if re.fullmatch(r"d\d+_roas", col)]

    # 2. 각 roas 컬럼에 대해 대응하는 _p 컬럼이 있으면 _growth 열 생성
    for col in roas_cols:
        p_col = f"{col}_p"
        if p_col in query_result_pltv_growth.columns:
            growth_col = col.replace("_roas", "roas_growth")
            query_result_pltv_growth[growth_col] = query_result_pltv_growth[col].fillna(query_result_pltv_growth[p_col])

    # 1.  최종 선택 컬럼
    base_cols = ['joyplegameid','regmonth', 'cost', 'cost_exclude_credit', 'ru','cpru', 'd7_roas']
    growth_cols = [col for col in query_result_pltv_growth.columns if re.fullmatch(r"d\d+roas_growth", col)]
    selected_cols = base_cols + growth_cols

    # 4. 필터링된 테이블 추출
    query_result_pltv_growth2 = query_result_pltv_growth[selected_cols]


    final = pd.merge(query_result_pltv_growth2, query_result_pltv_model[['joyplegameid','regmonth','pred_d360']]
                                    ,    on=['joyplegameid', 'regmonth'],how = 'left' )


    final = pd.merge(final, query_result_return[['joyplegameid','regmonth','d360_plus_return_actual','d360_plus_return_expected','status']]
            ,on = ['joyplegameid','regmonth'], how = 'left')


    query_result_pg= query_result_pg.rename(columns={'month':'regmonth'})
    query_result_pg= query_result_pg.rename(columns={'JoypleGameID':'joyplegameid'})
    pg_distinct = (
        query_result_pg.loc[:, ["regmonth", "joyplegameid", "bep_commission"]]
                    .drop_duplicates(ignore_index=True)
    )

    final = pd.merge(final, pg_distinct,on = ['joyplegameid','regmonth'], how = 'left')

    ##game_name
    mapping = {
        131:   "1.POTC",
        133:   "2.GBTW",
        30001: "3.WWMC",
        30003: "4.DRSG",
    }

    final["game_name"] = final["joyplegameid"].map(mapping)  # 매핑 없으면 NaN

    final['bep_base'] = final['game_name'].map({
        '1.POTC': 1.429,
        '2.GBTW': 1.429,
        '3.WWMC': 1.543,
        '4.DRSG': 1.543
    })


    ## 기간 필터

    # 기준일과 래그
    asof = pd.Timestamp.today().normalize()   # <-- 여기 핵심 (tz 없음)
    LAG_DAYS = 8
    obs_end_candidate = asof - pd.Timedelta(days=LAG_DAYS)  # naive

    # 최근 12개월만 필터링
    start_month = (asof.to_period("M") - 12).to_timestamp()
    final['regmonth_ts'] = pd.to_datetime(final['regmonth'] + "-01")

    final2 = final.copy()


    ## 당월 예상 COST
    # 월초/월말, 월일수
    final2['month_start'] = pd.to_datetime(final2['regmonth'] + '-01')
    final2['days_in_month'] = final2['month_start'].dt.daysinmonth
    final2['month_end'] = final2['month_start'] + pd.to_timedelta(final2['days_in_month'] - 1, unit='D')

    # 1) 우선 월말과 obs_end_candidate 중 작은 값을 obs_end로
    final2['obs_end'] = final2['month_end'].where(
        final2['month_end'] <= obs_end_candidate,  # 둘 다 Timestamp
        other=obs_end_candidate
    )

    # 2) obs_end_candidate가 월초보다 빠르면(=그 달은 아직 집계 시작 전), NaT로
    final2.loc[final2['month_start'] > obs_end_candidate, 'obs_end'] = pd.NaT

    # 관측시작일은 월초로 가정
    final2['obs_start'] = final2['month_start']

    # 관측일수(음수/NaT 방지)
    final2['observed_days'] = (
        (final2['obs_end'] - final2['obs_start']).dt.days + 1
    ).clip(lower=0).fillna(0).astype(int)

    # 월 일할 예상비용
    final2['cost_raw'] = final2['cost_exclude_credit']  # 원본 보관(옵션)
    final2['cost_exclude_credit'] = np.where(
        final2['observed_days'] > 0,
        final2['cost_raw'] * final2['days_in_month'] / final2['observed_days'],
        np.nan
    )

    # 원본 보관
    final2['regmonth_base'] = final2['regmonth']

    # 전체에서 마지막 월(YYYY-MM)
    last_month_str = final2['month_start'].max().to_period('M').strftime('%Y-%m')

    def _label_last_global(r):
        if r['regmonth_base'] == last_month_str and pd.notna(r['obs_end']):
            return f"{r['regmonth_base']} ( ~ {r['obs_end'].strftime('%m/%d')})"
        else:
            return r['regmonth_base']

    final2['regmonth'] = final2.apply(_label_last_global, axis=1)

    final3 = final2.eval("""
                        d360roas_pltv = pred_d360/cost_raw
                        d360roas_growth_plus_return_real = d360roas_growth+ d360_plus_return_actual
                        d360roas_growth_plus_return_expected = d360roas_growth + d360_plus_return_expected
                        bep_diff = bep_commission - bep_base
                        """)
    '''
    # 보조 컬럼 정리
    drop_cols = ['month_start','month_end','obs_start','obs_end','observed_days','days_in_month','regmonth_ts','cost','cost_raw']
    final3 = final3.drop(columns=drop_cols)

    final3.head()
    '''

    df = final3.copy()
    df = final3[final3['regmonth_ts'] >= start_month].copy()

    # 보조 컬럼 정리
    drop_cols = ['month_start','month_end','obs_start','obs_end','observed_days','days_in_month','regmonth_ts','cost','cost_raw']
    df = df.drop(columns=drop_cols)

    # DRSG만 d180roas_growth두기
    if "d180roas_growth" in df.columns:
        mask = df["game_name"].astype(str).str.contains(r"\bDRSG\b", case=False, na=False)
        df.loc[~mask, "d180roas_growth"] = np.nan


    ## 한글깨짐 방지를 위해 폰트 지정
    font_path = "/usr/share/fonts/truetype/nanum/NanumGothic.ttf"
    if Path(font_path).exists():
        fm.fontManager.addfont(font_path)       # 수동 등록
        mpl.rc('font', family='NanumGothic')    # 기본 폰트 지정
        mpl.rc('axes', unicode_minus=False)     # 마이너스 깨짐 방지
    else:
        print("⚠️ NanumGothic 설치 실패. 다른 폰트를 써야 합니다.")




    # 필요 컬럼 존재 체크(없으면 KeyError 방지용으로 걸러냄)
    line_cols_all = [
        "bep_commission",
        "d360roas_growth",
        "d360roas_pltv",
        "d360roas_growth_plus_return_real",
        "d360roas_growth_plus_return_expected",
        "d180roas_growth",
    ]
    line_cols = [c for c in line_cols_all if c in df.columns]

    # 컬럼별 색상(원하는 색/HEX로 바꿔도 됨)
    line_colors = {
        "bep_commission": "grey",
        "d360roas_growth": "DarkOrange",
        "d180roas_growth": "DarkOrange",
        "d360roas_pltv": "green",
        "d360roas_growth_plus_return_real": "brown",
        "d360roas_growth_plus_return_expected": "brown"
    }


    # 점선으로 그릴 대상
    linestyles = {
        "bep_commission": "--",
        "d360roas_growth_plus_return_real": "--",
        "d180roas_growth": "--",
    }


    # 정렬 및 x라벨 준비
    # 1) 정렬
    df["regmonth_dt"] = pd.to_datetime(df["regmonth"], errors="coerce")
    df = df.sort_values(["game_name", "regmonth_dt", "regmonth"], kind="mergesort").reset_index(drop=True)

    # 2) 고유 x 좌표(정수)와 라벨 만들기 -- 첫달만 게임명 표기
    df["xpos"] = np.arange(len(df))
    first_mask = df.groupby("game_name").cumcount() == 0
    xticklabels = np.where(
        first_mask,
        df["game_name"].astype(str) + " | " + df["regmonth"].astype(str),  # 게임별 첫 달
        df["regmonth"].astype(str)                                         # 나머지 달
    )


    ## graph
    fig, ax1 = plt.subplots(figsize=(15, 8))
    handles, labels = [], []
    fig.suptitle(
        "Monthly ROAS & Cost",           # 메인 제목
        fontsize=16, fontweight="bold", y=0.98
    )


    # 선: 게임별로 segment를 나눠 그리므로 게임 사이가 자동으로 "끊김"
    for j, col in enumerate([c for c in line_cols if c in df.columns]):
        first_for_legend = True
        for g, gdf in df.groupby("game_name", sort=False):
            ln, = ax1.plot(
                gdf["xpos"], gdf[col],
                linestyle=linestyles.get(col, "-"),
                linewidth=2.0,
                #marker="o", markersize=3.5,
                color=line_colors.get(col),
                label=(col if first_for_legend else None)
            )
            first_for_legend = False
        handles.append(ln); labels.append(col)

    # 비율 y축(0~300% 고정, 퍼센트 표기)
    ax1.set_ylim(0, 3.0) #y축 300%까지
    ax1.yaxis.set_major_formatter(PercentFormatter(xmax=1.0, decimals=0))
    ax1.grid(axis="y", alpha=0.25)
    ax1.set_ylabel("ROAS (%, lines)")

    # 이중축 막대(cost)
    ax2 = ax1.twinx()

    bar_color = "#adb5bd"        # 연회색
    edge_color = "#495057"       # 진회색 테두리
    bar = ax2.bar(df["xpos"], df["cost_exclude_credit"], alpha=0.35, label="cost"
                ,color=bar_color, edgecolor=edge_color)
    handles += [bar]; labels += ["cost"]
    ax2.set_ylabel("Cost")


    # x축 눈금에 커스텀 라벨 적용
    ax1.set_xticks(df["xpos"])
    ax1.set_xticklabels(xticklabels, rotation=45, ha="right")

    # (선택) 게임 경계에 세로 구분선
    boundary_pos = df.index[df["game_name"].ne(df["game_name"].shift()) & (df.index != 0)]
    for bp in boundary_pos:
        ax1.axvline(bp-0.5, color="lightgray", lw=1, alpha=0.6)


    ax1.legend(handles, labels, loc="best")
    plt.tight_layout()
    plt.savefig('roas_graph.png', dpi=160)
    plt.show()

    # 특정 파일의 절대 경로 확인
    import os
    file_path = os.path.abspath("roas_graph.png")
    print("저장된 파일 절대 경로:", file_path)

    mapping = {"POTC": "1.POTC", "GBTW": "2.GBTW", "WWM": "3.WWMC", "DRSG": "4.DRSG", "RESU" : "5.RESU"}
    query_result_kpi['game'] = query_result_kpi['project'].map(mapping)
    cols = ['game', 'kpi_d1', 'kpi_d3', 'kpi_d7', 'kpi_d14', 'kpi_d30', 'kpi_d60',
    'kpi_d90', 'kpi_d120', 'kpi_d150', 'kpi_d180', 'kpi_d210',
    'kpi_d240', 'kpi_d270', 'kpi_d300', 'kpi_d330', 'kpi_d360']

    kpi = query_result_kpi[cols]


    kpi_by_game = {
        "1.POTC": kpi[kpi["game"] == "1.POTC"],
        "2.GBTW": kpi[kpi["game"] == "2.GBTW"],
        "3.WWMC": kpi[kpi["game"] == "3.WWMC"],
        "4.DRSG": kpi[kpi["game"] == "4.DRSG"],
    }

    for game in kpi_by_game.keys():

        df_tmp = kpi_by_game[game].copy()

        # 숫자 컬럼 리스트 (game 제외)
        num_cols = [c for c in df_tmp.columns if c != 'game']

        # "%" 형태로 변환
        df_tmp[num_cols] = df_tmp[num_cols].applymap(
            lambda x: f"{x*100:.2f}%" if pd.notna(x) else ""
        )

        kpi_by_game[game] = df_tmp

    base_cols = ['game_name','regmonth','cost_exclude_credit','cpru','d7_roas','regmonth_ts']
    growth_cols = [col for col in final3.columns if re.fullmatch(r"d\d+roas_growth", col)]
    base_cols2 = ['d360roas_pltv','d360_plus_return_actual','d360roas_growth_plus_return_real'
                ,'d360_plus_return_expected','d360roas_growth_plus_return_expected'
                ,'bep_base','bep_commission','bep_diff']

    selected_cols = base_cols + growth_cols+base_cols2

    final4 = final3[selected_cols].sort_values(by = ['game_name', 'regmonth'])
    final4 = final4[final4['regmonth_ts'] >= '2024-01-01']

    df_numeric = final4.copy()
    df_numeric = df_numeric.reset_index(drop=True)
    df_numeric = df_numeric.drop(columns='regmonth_ts')


    nest_asyncio.apply()


    ########### growth 예측치 회색 음영 반영
    # 예측 기준 dn을 각 row(regmonth)별로 추론
    def infer_cohort_dn_map(df):
        cohort_map = {}
        for idx, row in df.iterrows():
            regmonth = idx[1] if isinstance(idx, tuple) else row.get('regmonth', None)
            for col in df.columns:
                if re.fullmatch(r'd\d+_roas_p', col):
                    if pd.notna(row[col]):
                        dn = int(re.findall(r'\d+', col)[0])
                        # 예측치가 존재하는 가장 작은 dn 값을 기준으로 설정
                        if regmonth not in cohort_map or dn < cohort_map[regmonth]:
                            cohort_map[regmonth] = dn
        return cohort_map


    # 예측 cohort 확인
    cohort_dn_map = infer_cohort_dn_map(query_result_pltv_growth)
    cohort_dn_map

    # 컬럼 이름에서 dn 값 추출
    def extract_dn(col):
        match = re.match(r'd(\d+)', col)  # d 뒤의 숫자만 추출
        return int(match.group(1)) if match else None


    dn_growth_columns = [col for col in df_numeric.columns if re.fullmatch(r'd\d+roas_growth', col)]
    dn_values = {col: extract_dn(col) for col in dn_growth_columns}


    # regmonth 문자안에 포함되었는지 확인
    def resolve_cohort_dn(regmonth, cohort_dn_map):
        r = str(regmonth or "")
        # 포함 매칭되는 모든 키 중 가장 긴 키(더 구체적)를 우선
        candidates = [(k, v) for k, v in cohort_dn_map.items() if k and str(k) in r]
        if not candidates:
            return np.inf
        # 키 길이 기준으로 최장 매칭 우선
        k, v = max(candidates, key=lambda kv: len(str(kv[0])))
        return v

    ## 테스트 코드

    # --- [1] 컬럼명 변경 맵 정의 ---
    rename_dict = {
        "d7_roas": "growth_d7",
        "d360roas_pltv": "pltv_d360",
        "d360_plus_return_actual": "return_d360",
        "d360roas_growth_plus_return_real": "gr_plus_ret_act_d360",
        "d360_plus_return_expected": "return_exp_d360",
        "d360roas_growth_plus_return_expected": "gr_plus_ret_exp_d360"
    }
    for d in [14, 30, 60, 90, 120, 150, 180, 210, 240, 270, 300, 330, 360]:
        rename_dict[f"d{d}roas_growth"] = f"growth_d{d}"

    # --- [2] 데이터프레임 컬럼명 선제 변경 ---
    df_numeric = df_numeric.rename(columns=rename_dict)

    # --- [3] 변경된 이름 기준 헬퍼 변수 재설정 ---
    # 이제 growth_d로 시작하는 컬럼에서 숫자를 추출합니다.
    def extract_dn_new(col):
        match = re.search(r'd(\d+)', col)
        return int(match.group(1)) if match else None

    # 바뀐 컬럼명 기반으로 dn_values 생성
    dn_growth_columns = [col for col in df_numeric.columns if col.startswith('growth_d')]
    dn_values = {col: extract_dn_new(col) for col in dn_growth_columns}

    # --- [4] 스타일 함수들 업데이트 (새 이름 반영) ---
    def highlight_based_on_dn(row):
        regmonth = row['regmonth']
        cohort_dn = resolve_cohort_dn(regmonth, cohort_dn_map)
        return [
            (
                'background-color: lightgray'
                if (
                    col.startswith('growth_d') and # 조건 변경
                    isinstance(dn_values.get(col), (int, float)) and
                    isinstance(cohort_dn, (int, float)) and
                    dn_values[col] >= cohort_dn and
                    pd.notna(row[col])
                ) else ''
            )
            for col in row.index
        ]

    def highlight_roas_vs_bep(row):
        styles = []
        for col in row.index:
            style = ""
            try:
                val = row[col]
                # % 처리 로직은 동일
                roas_val = float(val.replace('%', '')) / 100 if isinstance(val, str) and val.endswith('%') else val

                # growth_d 로 시작하는 컬럼 체크
                if col.startswith("growth_d") and pd.notnull(row.get("bep_base")):
                    if pd.notnull(roas_val) and roas_val > row["bep_base"]:
                        style = "background-color: #fbe4e6"

                # 바뀐 d360 플러스 컬럼명 체크
                elif col in ["gr_plus_ret_act_d360", "gr_plus_ret_exp_d360"] and pd.notnull(row.get("bep_commission")):
                    if pd.notnull(roas_val) and roas_val > row["bep_commission"]:
                        style = "background-color: #fbe4e6"
            except: pass
            styles.append(style)
        return styles

    def highlight_over_kpi(row, kpi_df):
        game = row['game_name']
        target = kpi_df[kpi_df['game'] == game]
        if target.empty: return [''] * len(row)
        target_row = target.iloc[0]
        styles = []

        for col in row.index:
            base_style = ""
            # d7_roas -> growth_d7
            if col == "growth_d7":
                kpi_val = target_row.get("kpi_d7")
                if pd.notna(row[col]) and pd.notna(kpi_val) and row[col] >= kpi_val:
                    base_style = "color: red; font-weight: bold;"

            # growth_d?? 형태 체크
            elif col.startswith("growth_d"):
                dn = extract_dn_new(col)
                kpi_col = f"kpi_d{dn}"
                if kpi_col in target_row:
                    kpi_val = target_row[kpi_col]
                    if pd.notna(row[col]) and pd.notna(kpi_val) and row[col] >= kpi_val:
                        base_style = "color: red; font-weight: bold;"

            styles.append(base_style)
        return styles

    # --- [5] 게임별 표 분리 및 Styler/RAW 생성 ---
    game_groups = df_numeric.groupby('game_name')
    styled_tables = {}
    raw_df_by_game = {}

    # 포맷팅 대상 컬럼 리스트 (새로운 컬럼명 기준)
    format_percent_cols = [
        "growth_d7", "pltv_d360", "return_d360", "bep_base", "bep_commission", "bep_diff",
        "gr_plus_ret_act_d360", "gr_plus_ret_exp_d360", "return_exp_d360",
    ]
    format_comma_cols = ["cost_exclude_credit", "cpru"]

    for game, game_df in game_groups:
        # A. 이미지용 스타일 생성 (이전과 동일)
        growth_cols = [col for col in game_df.columns if col.startswith("growth_d")]

        styled_game = game_df.style\
            .format({
                "cost_exclude_credit": "{:,.0f}",
                "cpru": "{:,.0f}",
                # 모든 growth 관련 및 지표 컬럼에 % 적용
                **{col: "{:.2%}" for col in game_df.columns if col.startswith("growth") or any(x in col for x in ["pltv", "return", "ret", "bep"])}
            })\
            .bar(subset=['cost_exclude_credit', 'bep_diff'], color='#f4cccc')\
            .bar(subset=['cpru'], color='#b6d7a8')\
            .bar(subset=growth_cols, color='#c9daf8')\
            .bar(subset=['return_d360', 'return_exp_d360'], color='#ffe599')\
            .set_table_styles([
                {'selector': 'th', 'props': [('background-color', '#f0f0f0'), ('font-weight', 'bold')]}
            ])\
            .apply(highlight_based_on_dn, axis=1)\
            .apply(highlight_roas_vs_bep, axis=1)\
            .apply(highlight_over_kpi, axis=1, kpi_df=kpi)

        styled_tables[game] = styled_game

        # B. 노션 업로드용 RAW 데이터 포맷팅 (실제 값을 문자열로 변환)
        notion_df = game_df.copy()

        # 1. 콤마 적용 (비용, CPRU)
        for col in format_comma_cols:
            if col in notion_df.columns:
                notion_df[col] = notion_df[col].map(lambda x: f"{x:,.0f}" if pd.notnull(x) else "")

        # 2. 퍼센트 적용 (모든 growth_ 관련 및 KPI 관련)
        for col in notion_df.columns:
            # 컬럼명이 growth로 시작하거나, 퍼센트 대상 리스트에 포함된 경우
            if col.startswith("growth") or col in format_percent_cols:
                notion_df[col] = notion_df[col].map(lambda x: f"{x:.2%}" if pd.notnull(x) else "")

        raw_df_by_game[game] = notion_df

    # styled_tables["1.POTC"]
    # print(f"Checking {game}: {styled_table.columns.tolist()[:30]}...")

    ## 노션 테이블 형태로 도표 형성
    def df_to_table_rows(df, max_rows=100):
        rows = []

        # header
        rows.append({
            "object": "block",
            "type": "table_row",
            "table_row": {
                "cells": [
                    [{"type": "text", "text": {"content": str(col)}}]
                    for col in df.columns
                ]
            }
        })

        # data
        for _, r in df.head(max_rows - 1).iterrows():
            rows.append({
                "object": "block",
                "type": "table_row",
                "table_row": {
                    "cells": [
                        [{"type": "text", "text": {"content": "" if pd.isna(v) else str(v)}}]
                        for v in r.tolist()
                    ]
                }
            })

        return rows

    # HTML 템플릿 정의
    # (NY_추가) 테이블 크기 조정 & 테이블 추가
    html_template = """
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <style>
            body { font-family: Arial, sans-serif; padding: 20px; }
            table { border-collapse: collapse; font-size: 20px; }
            th, td {
                border: 1px solid #999;
                padding: 6px 10px;
                text-align: right;
            }
            th { background-color: #f0f0f0; }
        </style>
    </head>
    <body>

        <h2>{{ game_name }} 월간 ROAS raw</h2>
        {{ table | safe }}

        <hr>

        <h2>{{ game_name }} KPI Table</h2>
        {{ kpi_table | safe }}
    </body>
    </html>
    """


    # 각 게임별로 HTML 파일 저장
    for game, styled_table in styled_tables.items():
        # 테이블을 HTML로 변환
        table_html = styled_table.to_html()

        #(NY_추가) ROAS kpi 도표도 하단에 추가
        kpi_html = kpi_by_game[game].to_html(index=False, classes="kpi-table",
        escape=False   # % 기호 유지
                                            )

        # HTML 파일로 저장
        rendered_html = Template(html_template).render(game_name=game, table=table_html, kpi_table=kpi_html)

        # 저장할 파일 경로 설정
        html_path = f"{game}_roas_table.html"

    # HTML 파일 저장
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(rendered_html)

        print(f"{game} 테이블 HTML로 저장 완료: {html_path}")

        # dfi.export(
        #     styled_table,
        #     html_path,
        #     table_conversion='playwright', # 혹은 'playwright'
        #     dpi=500
        # )

        # print(f"{game} 테이블 이미지 저장 완료 (dfi 방식): {html_path}")


    # 이미지 캡처 비동기 함수
    # 수정된 capture_html_to_image 함수
    async def capture_html_to_image(html_path, output_image_path):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)

            # device_scale_factor를 2 또는 3으로 설정 (숫자가 높을수록 고화질)
            # 너비(width)도 테이블이 잘리지 않도록 넉넉하게 설정하세요.
            context = await browser.new_context(
                viewport={"width": 600, "height": 400},
                device_scale_factor= 1  # 4배 선명하게 캡처
            )
            page = await context.new_page()

            await page.goto("file://" + os.path.abspath(html_path), wait_until="networkidle")

            # 이미지 캡쳐
            await page.screenshot(path=output_image_path, full_page=True, animations="disabled")

            await browser.close()

    # # 게임별 HTML을 이미지로 저장하기
    async def save_images_for_all_games():
        for game in styled_tables.keys():
            html_path = f"{game}_roas_table.html"  # HTML 파일 경로
            output_image_path = f"{game}_roas_table.png"  # 저장될 이미지 경로

            await capture_html_to_image(html_path, output_image_path)
            print(f"{game} 테이블을 이미지로 저장 완료: {output_image_path}")

    # 비동기 실행 (Spyder나 GCP 환경에서)
    asyncio.get_event_loop().run_until_complete(save_images_for_all_games())

    #### gemini 분석 데이터 필터
    # 최근 6개월 데이터만 추출
    recent_data = final3.sort_values(['game_name', 'regmonth']).groupby('game_name').tail(6)
    #recent_data = final3.sort_values(['game_name', 'regmonth']).groupby('game_name').tail(7).sort_values(['game_name', 'regmonth']).groupby('game_name').head(6)

    exclude_cols = ['d360_plus_return_actual', 'd360_plus_return_expected','joyplegameid','pred_d360','cost','cost_raw','d360roas_pltv','ru','d360roas_growth_plus_return_real']
    recent_data = recent_data.drop(columns=exclude_cols)


    # 테이블 형태로 문자열화 (Markdown table)
    def df_to_md(df):
        header = "| " + " | ".join(df.columns) + " |"
        sep = "| " + " | ".join(["---"]*len(df.columns)) + " |"
        rows = "\n".join("| " + " | ".join(map(str, row)) + " |" for row in df.values)
        return "\n".join([header, sep, rows])

    recent_data_md = df_to_md(recent_data)

    PROJECTS = [
        {"project": "POTC", "database_id": "1eeea67a56818058a431dc9a754beeab"},
        {"project": "GBTW", "database_id": "1eeea67a568180f18048dcc7769a3621"},
        {"project": "WWMC", "database_id": "1eeea67a568180449ca2d06c15779d6e"},
        {"project": "DRSG", "database_id": "1eeea67a5681809b8da6ddb8e6e9d0d5"},
    ]


    # Notion API 레이트리밋(평균 3rps 권장) → 안전슬립
    NOTION_SLEEP_SEC = 0.4


    # 기준일(최근 8일 전) — 월간 리포트 탐색 시 기준이 되는 날짜
    REF_DT = datetime.now(ZoneInfo("Asia/Seoul")) - timedelta(days=8)

    notion = NotionClient(auth=NOTION_TOKEN, timeout_ms=60_000, log_level=logging.WARNING, notion_version=NOTION_VERSION)
    print(NOTION_VERSION)

    def sleep():
        time.sleep(NOTION_SLEEP_SEC + random.uniform(0, 0.25))  # + 지터


    # 목적: 특정 데이터베이스 스키마에서 "title" 타입 속성의 실제 속성명을 조회
    # 입력: database_id (str)
    # 출력: 제목 속성명 (str) — 예: "Name", "제목" 등
    def get_title_prop_name(database_id: str) -> str:
        schema = notion.databases.retrieve(database_id=database_id)
        sleep()
        print(NOTION_VERSION)

        for name, prop in schema.get("properties", {}).items():
            if prop.get("type") == "title":
                return name
        raise RuntimeError("title 타입 컬럼을 찾지 못했습니다.")

    # 목적: datetime → 'YY년 M월' 한국식 연/월 문자열로 변환
    # 입력: dt (datetime)
    # 출력: 예) '25년 9월'
    def format_kor_year_month(dt: datetime) -> str:
        # '25년 9월'
        yy = dt.year % 100
        m  = dt.month
        return f"{yy}년 {m}월"

    # 목적: datetime → 'YYYY-MM' 형태의 월 키 생성(메타 키/리스트릭트 용)
    # 입력: dt (datetime)
    # 출력: 예) '2025-09'
    def ym_key(dt: datetime) -> str:
        # 'YYYY-MM' (restricts/메타 키)
        return f"{dt.year:04d}-{dt.month:02d}"


    # 목적: 프로젝트 월간 리포트 페이지 1건을 Notion DB에서 제목 패턴으로 검색
    #  - 제목 패턴: f"[{project}] {YY}년 {M}월 누적 UA 리포트"
    #  - '9월'/'09월' 모두 매칭(OR)
    # 입력: database_id (str), project (str), ref_dt (datetime|None)
    # 출력: {project, page_id, ym_title, ym, page_url} 또는 None
    def find_month_page_by_title(database_id: str, project: str, ref_dt: Optional[datetime] = None):
        if ref_dt is None:
            ref_dt = datetime.now(ZoneInfo("Asia/Seoul")) - timedelta(days=8)
        ym_kor = format_kor_year_month(ref_dt)       # '25년 9월'
        ym_kor_zero = f"{ref_dt.year%100}년 {ref_dt.month:02d}월"  # '25년 09월' (제목에 0패딩일 때 대비)

        title_prop = get_title_prop_name(database_id)

        # contains AND (project / 고정문구) + (월 표기는 OR)
        filt = {
            "and": [
                {"property": title_prop, "title": {"contains": f"[{project}]"}},
                {"property": title_prop, "title": {"contains": "누적 UA 리포트"}},
                {"or": [
                    {"property": title_prop, "title": {"contains": ym_kor}},
                    {"property": title_prop, "title": {"contains": ym_kor_zero}},
                ]}
            ]
        }
        print(NOTION_VERSION)

        q = notion.databases.query(
            **{
                "database_id": database_id,
                "filter": filt,
                "sorts": [{"timestamp": "last_edited_time", "direction": "descending"}],
                "page_size": 1
            }
        )
        sleep()
        results = q.get("results", [])
        if not results:
            return None
        page = results[0]
        page_id = page["id"]
        return {
            "project": project,
            "page_id": page_id,
            "ym_title": ym_kor,
            "ym": ym_key(ref_dt),
            "page_url": f"https://www.notion.so/{page_id.replace('-','')}",


        }

    ################################ 텍스트 추출
    # ==== 2) 페이지 → 섹션(주간) 추출(재귀 + 페이지네이션) =========================
    def list_all_children(block_id: str):
        results, cursor = [], None
        while True:
            resp = notion.blocks.children.list(block_id=block_id, start_cursor=cursor)
            results.extend(resp["results"])
            sleep()
            if not resp.get("has_more"):
                break
            cursor = resp.get("next_cursor")
        return results

    def rts_to_text(rts):  # rich_text → plain
        return "".join(rt.get("plain_text","") for rt in (rts or []))

    def block_to_text(block, include_children=True) -> str:
        t = block["type"]; b = block.get(t, {}); lines: List[str] = []
        def add(x):
            if x and x.strip():
                lines.append(x.strip())

        if t in ("paragraph","heading_1","heading_2","heading_3",
                "bulleted_list_item","numbered_list_item","to_do","quote","callout","code"):
            txt = rts_to_text(b.get("rich_text", []))
            if t=="bulleted_list_item" and txt: txt = "- " + txt
            if t=="numbered_list_item" and txt: txt = "1. " + txt
            if t=="to_do": txt = f"{'[x]' if b.get('checked') else '[ ]'} {txt}"
            add(txt)
        elif t=="bookmark":
            add(b.get("url",""))
        elif t=="table":
            # table_row children → cell 텍스트 ' | '로 결합
            for row in list_all_children(block["id"]):
                if row["type"]=="table_row":
                    cells = row["table_row"]["cells"]
                    row_txt = " | ".join(rts_to_text(c) for c in cells)
                    add(row_txt)

            # has_children이 True일 때만 자식 블록을 처리
        if include_children and block.get("has_children"):
            for ch in list_all_children(block["id"]):
                sub = block_to_text(ch, include_children=True)
                if sub:
                    lines.append(sub)

        return "\n".join(lines)


    def detect_heading_level(top_blocks):
        """top 블록들에서 사용할 헤딩 레벨을 자동 선택"""
        for ht in ("heading_1", "heading_2", "heading_3"):
            if any(b["type"] == ht for b in top_blocks):
                return ht
        return None

    def extract_sections_by_heading_auto(page_id: str, include_preface=False):
        """
        - heading_1 있으면 그걸로 섹션화
        - 없으면 heading_2 → 없으면 heading_3
        - 어떤 헤딩도 없으면 전체 내용을 1개 섹션으로 반환
        - include_preface=True면 첫 헤딩 전 문단을 '(서문)' 섹션으로 포함
        """
        top = list_all_children(page_id)
        heading_type = detect_heading_level(top)

        # 헤딩이 전혀 없으면 전체 묶어서 반환
        if not heading_type:
            all_lines = []
            for blk in top:
                t = block_to_text(blk, include_children=True)
                if t: all_lines.append(t)
            content = "\n".join(all_lines).strip()
            return [{"title": "(전체)", "content": content}] if content else []

        sections = []
        current = None
        preface_lines = []

        i = 0
        while i < len(top):
            blk = top[i]
            btype = blk["type"]

            if btype == heading_type:
                # 이전 섹션 마감
                if current:
                    sections.append(current)

                # 제목(자식 제외)
                title = block_to_text(blk, include_children=False) or "Untitled"
                current = {"title": title, "content_lines": []}

                # 이 헤딩의 자식(토글/표/리스트 등) 포함
                if blk.get("has_children"):
                    for ch in list_all_children(blk["id"]):
                        sub = block_to_text(ch, include_children=True)
                        if sub:
                            current["content_lines"].append(sub)

                i += 1
                continue

            # 첫 헤딩 나오기 전 문단은 프리페이스로 모을 수 있음
            if current is None and include_preface:
                txt = block_to_text(blk, include_children=True)
                if txt:
                    preface_lines.append(txt)
            elif current is not None:
                # 다음 동일 레벨 헤딩 전까지 형제 블록을 본문에 포함
                txt = block_to_text(blk, include_children=True)
                if txt:
                    current["content_lines"].append(txt)

            i += 1

        # 마지막 섹션 마감
        if current:
            sections.append(current)

        # 프리페이스 섹션 삽입
        if include_preface and preface_lines:
            preface = "\n".join(preface_lines).strip()
            if preface:
                sections.insert(0, {"title": "(서문)", "content": preface})

        # content_lines 병합 + 빈 섹션 제거
        out = []
        for s in sections:
            content = "\n".join(s.get("content_lines", [])).strip()
            if content:
                out.append({"title": s["title"], "content": content})

        return out

    ############################### 노션 Weekly Report 전문 추출

    weekly_report_db3 = []

    # 목적: 각 프로젝트별 월간 페이지를 찾아 섹션 텍스트를 추출 → LLM 요약 → 요약본 DB에 적재

    for p in PROJECTS:
        print(f"\n=== {p['project']} ===")
        sel = find_month_page_by_title(p["database_id"], p["project"], ref_dt=REF_DT)
        print(f"  [SELECT] {sel}")
        if not sel:
            print("  [WARN] 당월 제목 패턴 페이지를 찾지 못했습니다.")
            weekly_report_db3.append({
                "project": p['project'],
                "summary": "(해당 월 주간 리포트 컨텍스트 없음)"
            })
            continue

        page_id = sel["page_id"]
        sections = extract_sections_by_heading_auto(page_id, include_preface=False)

        if not sections:
            print("  [HINT] 동일 레벨 헤딩이 없고 본문이 children에만 있을 수 있어 최상위 블록 구조를 재점검하세요.")
            weekly_report_db3.append({
                "project": p['project'],
                "summary": "추출 가능한 본문 내용이 없습니다."
            })
            continue

        # 전체 섹션 내용을 하나의 문자열로 결합
        full_report_text = "\n\n".join([s['content'] for s in sections])

        ## gemini 3.0 high 위한 코드 추가
        weekly_report_db3.append({
            "project": p['project'],
            "full_report_text": full_report_text
        })

    # 시스템 지시_페르소나 및 제약조건

    system_instruction = """
    너는 전문 마케팅 데이터 분석가야.
    주어진 ROAS 데이터와 퍼포먼스팀의 원문 리포트를 **절대 오류 없이 분석**하고, 요청된 **모든 출력 형식 규칙**을 엄격하게 준수하여 리포트를 작성해야해.

    [데이터 정합성 최우선 규칙]
    1. 모든 수치 비교 (BEP 달성, 증감률 계산)는 오직 제공된 테이블 데이터만을 기반으로 수행해
    2. 테이블에 없는 데이터나 추론은 엄금하며, 비교 대상은 동일한 게임 내에서 서로 다른 시점(월)의 동일한 지표(열)이야

    [표기법 규칙]
    - cost, install ru, CPI, cpru는 천단위 쉼표(,)를 사용
    - ROAS 관련 지표는 소수점 첫째 자리까지 표기하고 '%' 단위를 사용
    - 증감률을 이야기할 때는 +- 기호 대신 🔺(상승) 또는 🔻(하락) 기호를 숫자앞에 사용해줘
    - 변수명에 대한 언급 제외

    [출력형식 규칙]
    - 마크다운 포맷: 노션 마크다운 포맷을 사용해
    - 리포트 작성 완료했다는 내용은 별도로 언급하지마

    """
    # 2. 데이터 학습
    prompt_description_3_optimized = f"""
    ## 데이터 입력
    SLG 게임 4종의 월별 ROAS 현황 데이터 및 주요 지표 설명이야

    [주요 변수 설명]
    - cost_exclude_credit: 크레딧 제외 월별 마케팅비 (roas 계산 기준, 당월은 일할계산 추정 월 cost)
    - cpru: 단가
    - d360roas_growth: 복귀유저 미포함 신규유저 d360 예측치
    - d360roas_growth_plus_return_expected: 복귀유저 포함 d360 예측치
    - bep_commission: 수수료 고려한 BEP

    --- ROAS 데이터 테이블 ---
    실제 월별 마케팅 데이터야
    지표에 대해 언급할 때는 해당 테이블 수치만을 사용해
    {recent_data_md}

    --- 퍼포먼스팀 주간 리포트 원문 내용 ---
    퍼포먼스팀에서 작성한 주간 리포트 내용이야
    당월의 주요 이슈사항을 아래 리포트 내용을 참고 해
    {weekly_report_db3}
    """

    prompt_parts_3_final = [
        prompt_description_3_optimized,
        """
    ### 마케팅 분석 리포트 작성 요청

    주어진 ROAS 데이터 및 원문 리포트를 기반으로 다음의 4가지 항목에 대해 게임별 분석 리포트를 작성해줘

    1. BEP 달성 여부 판단 가장 최근월의 복귀유저 포함 D360 ROAS와 수수료 고려 BEP(bep_commission)를 비교하여 달성 여부를 명확히 판단해
    2. 신규 유저 ROAS BEP 초과한 최소 Cohort 분석: 가장 최근월 복귀유저 고려하지 않은 신규유저 d360 ROAS(d360roas_growth)가 BEP를 초과한 경우에는d90roas_growth부터 d360roas_growth까지의 cohort 중 BEP(bep_commission) 이상인 최소 cohort dn을 언급해줘. 단, d360roas_growth가 BEP를 초과하지 않았다면 이 항목은 언급하지 마
    3. 전월 대비 증감률 계산: 비용(cost_exclude_credit), 단가(cpru), 복귀유저 포함 ROAS, 신규유저 ROAS의 전월 대비 증감률을 계산해. 전월 수치와 당월 수치도 함께 표기하고, 특이점을 간결하게 언급해
    4. 주요 이슈 정리: 퍼포먼스팀의 주간 리포트 원문 내용을 참고해서 당월의 주요 이슈나 히스토리를 정리해서 추가해줘

    작성 시 아래의 형태를 지켜서 작성 부탁해

    1. **BEP 달성 여부:** …
    2. **신규 유저 ROAS BEP 초과 여부:** …
    3. **전월 대비 증감률:**
        - 비용: …
        - 단가: …
        - 복귀유저 포함 ROAS: …
        - 신규유저 ROAS: …
        - 특이점 : ...
    4. **주요 이슈:** …
    """]

    genai_client = GeminiClient(
        vertexai=True,
        location="global"      # genai 호출용location 변경
    )

    config_3_optimized = GenerateContentConfig(
        temperature=1.0,
        thinking_config=types.ThinkingConfig(thinking_level="high"),
        system_instruction=system_instruction,
        labels=LABELS
    )

    response5 = genai_client.models.generate_content(
        model="gemini-3-pro-preview",   # Vertex AI 모델명
        contents = prompt_parts_3_final
        ,config=config_3_optimized
    )
    print(response5.text)

    # Notion API 클라이언트 초기화
    notion = NotionClient(auth=NOTION_TOKEN, notion_version=NOTION_VERSION)

    title_prop: str = "이름"
    page_title = f"SLG 월별 마케팅 현황 리뷰_{datetime.today().strftime('%y%m%d')}"
    project_list = ["GBTW","POTC","DRSG","WWM"]


    # DB 속성 구성
    props = {
        title_prop: {"title": [{"text": {"content": page_title}}]},
        "등록 날짜": {"date": {"start": datetime.today().isoformat()}},

        # '프로젝트' 속성 (Rich Text 또는 Select)
        "프로젝트": {"multi_select": [{"name": project} for project in project_list  ] },
        "리포트 종류": {"multi_select": [{"name": "마케팅분석"}]}}
    #  if author_person_id:
    #      props["작성자"] = {"people": [{"id": author_person_id}]}


    # if ref_person_id:
    #     props["참조자"] = {"people": [{"id": ref_person_id}]}

    # 나윤 테스트 베이스
    NOTION_DATABASE_ID = "2ccea67a56818069b6abc52e5b5ca372"

    # 페이지 생성
    new_page = notion.pages.create(
        parent={"database_id": NOTION_DATABASE_ID},
    #   properties=props
    )

    # 생성된 페이지 ID 가져오기
    PAGE_ID = new_page["id"]

    # 생성된 페이지 URL 출력
    print("✅ Notion 페이지 생성 완료:", new_page["url"])
    print("🆔 생성된 페이지 ID:", PAGE_ID)


    ############## 이미지 업로드##############

    import os, time, json, requests
    from pathlib import Path


    # 업로드용 경로 변수
    IMG_PATH = Path("roas_graph.png").resolve()  # 절대경로로 변환(권장)
    assert IMG_PATH.exists() and IMG_PATH.stat().st_size > 0

    hdr_json = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }

    # 1) 업로드 오브젝트 생성 (upload_url 받기)
    resp = requests.post(
        "https://api.notion.com/v1/file_uploads",
        json={"filename": IMG_PATH.name, "content_type": "image/png"},
        headers=hdr_json
    )
    resp.raise_for_status()
    fu = resp.json()
    file_upload_id = fu["id"]
    upload_url = fu.get("upload_url")


    # 2) 실제 전송 (multipart/form-data) ⇒ status 가 uploaded 여야 첨부 가능
    with open(IMG_PATH, "rb") as f:
        r2 = requests.post(
            # 권장: 명시적 send 엔드포인트 사용
            f"https://api.notion.com/v1/file_uploads/{file_upload_id}/send",
            # 또는 upload_url 사용 가능 (동일 동작): upload_url,
            headers={"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": NOTION_VERSION},
            files={"file": (IMG_PATH.name, f, "image/png")}
        )
    r2.raise_for_status()


    # (옵션) 상태 확인 및 폴링: uploaded 될 때까지 잠깐 대기
    for _ in range(10):
        r_chk = requests.get(
            f"https://api.notion.com/v1/file_uploads/{file_upload_id}",
            headers={"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": NOTION_VERSION}
        )
        r_chk.raise_for_status()
        status = r_chk.json().get("status")
        if status == "uploaded":
            break
        time.sleep(0.4)
    assert status == "uploaded", f"업로드 상태가 {status} 입니다. (uploaded 여야 첨부 가능)"

    ## 본문 기존
    ########### (1) 제목
    notion.blocks.children.append(
        PAGE_ID,
        children=[
            {
                "object": "block",
                "type": "heading_1",
                "heading_1": {
                    "rich_text": [{"type": "text", "text": {"content": "1) 전체 유저 ROAS 현황" }}]
                },
            }
        ],
    )

    ########### (2) 그래프 첨부
    notion.blocks.children.append(
        PAGE_ID,
        children=[
            {
                "object": "block",
                "type": "image",
                "image": {
                    "type": "file_upload",
                    "file_upload": {"id": file_upload_id},
                    "caption": [
                        {"type": "text", "text": {"content": "당월의 COST는 집계기간내 COST 기반 일할 계산된 추정 당월 소진 COST입니다."}}
                    ]
                },
            }
        ]
    )

    ########### (3) 표 첨부
    # 업로드용 경로
    IMG_PATHS = [
        Path("1.POTC_roas_table.png").resolve(),
        Path("2.GBTW_roas_table.png").resolve(),
        Path("3.WWMC_roas_table.png").resolve(),
        Path("4.DRSG_roas_table.png").resolve(),
    ]


    # 업로드 체크 및 헤더
    hdr_json = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }

    # 업로드된 파일들에 대한 ID 목록
    file_upload_ids = []

    # 1) 각 이미지 파일에 대해 업로드 처리
    for img_path in IMG_PATHS:
        assert img_path.exists() and img_path.stat().st_size > 0  # 파일이 존재하고 크기가 0보다 커야함

        # 업로드 오브젝트 생성 (upload_url 받기)
        resp = requests.post(
            "https://api.notion.com/v1/file_uploads",
            json={"filename": img_path.name, "content_type": "image/png"},
            headers=hdr_json
        )
        resp.raise_for_status()
        fu = resp.json()
        file_upload_id = fu["id"]
        upload_url = fu.get("upload_url")

        # 2) 실제 파일 전송 (multipart/form-data)
        with open(img_path, "rb") as f:
            r2 = requests.post(
                f"https://api.notion.com/v1/file_uploads/{file_upload_id}/send",
                headers={"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": NOTION_VERSION},
                files={"file": (img_path.name, f, "image/png")}
            )
        r2.raise_for_status()

        # (옵션) 상태 확인 및 폴링: uploaded 될 때까지 대기
        for _ in range(10):
            r_chk = requests.get(
                f"https://api.notion.com/v1/file_uploads/{file_upload_id}",
                headers={"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": NOTION_VERSION}
            )
            r_chk.raise_for_status()
            status = r_chk.json().get("status")
            if status == "uploaded":
                break
            time.sleep(0.4)

        assert status == "uploaded", f"업로드 상태가 {status} 입니다. (uploaded 여야 첨부 가능)"

        # 업로드된 파일 ID 저장
        file_upload_ids.append(file_upload_id)

    # 3) 토글 블록 생성 (상세 ROAS 표)
    toggle_block = notion.blocks.children.append(
        PAGE_ID,
        children=[
            {
                "object": "block",
                "type": "toggle",
                "toggle": {
                    "rich_text": [{"type": "text", "text": {"content": "상세 ROAS 표(클릭)"}}],
                    "children": []  # 일단 비워둠
                }
            }
        ]
    )

    toggle_id = toggle_block["results"][0]["id"]

    # 4) 이미지와 도표 추가하는 방식으로 변경
    for img_path, file_upload_id in zip(IMG_PATHS, file_upload_ids):
    # 이미지 추가 로직
        notion.blocks.children.append(
            toggle_id,
            children=[
                {
                    "object": "block",
                    "type": "image",
                    "image": {
                        "type": "file_upload",
                        "file_upload": {"id": file_upload_id},
                        "caption": [
                            {
                                "type": "text",
                                "text": {
                                    "content": "ROAS 표 - 당월의 COST는 집계기간내 COST 기반 일할 계산된 추정 당월 소진 COST입니다."
                                }
                            }
                        ]
                    }
                }
            ]
        )

        # 이미지파일의 앞글자 기준 Game 매칭
        game_key = img_path.name[:6]
        raw_df = raw_df_by_game[game_key]

        # Table RAW 업데이트
        raw_toggle = notion.blocks.children.append(
            toggle_id,
            children=[
                {
                    "object": "block",
                    "type": "toggle",
                    "toggle": {
                        "rich_text": [
                            {"type": "text", "text": {"content": f"Table_RAW ({game_key})"}}
                        ],
                        "children": [
                            {
                                "object": "block",
                                "type": "table",
                                "table": {
                                    "table_width": len(raw_df.columns),
                                    "has_column_header": True,
                                    "has_row_header": False,
                                    "children": df_to_table_rows(raw_df, max_rows=100)
                                }
                            }
                        ]
                    }
                }
            ]
        )

    ########### (4) gemini분석 내용 첨부
    ##(NY수정) 노션에 업데이트 되는 글자가 100줄을 넘으면 에러가 발생함. 이부분 끊어서 갈 수 있도록 하는 로직 추가함

    # 1) Gemini 결과 → Notion 블록 변환
    blocks = md_to_notion_blocks(response5.text + "\n\n\n")

    # 2) Notion API의 children ≤ 100 제한 해결 → 100개씩 나눠 넣기
    def chunk_list(lst, size=100):
        for i in range(0, len(lst), size):
            yield lst[i:i+size]

    # 3) 100개씩 append
    for chunk in chunk_list(blocks, 100):
        notion.blocks.children.append(
            block_id=PAGE_ID,
            children=chunk
        )

    print("✅ Append 완료")

    from datetime import datetime, timezone, timedelta
    RUN_ID = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    LABELS = {"datascience_division_service": "monthly_mkt_framework",
            "run_id": RUN_ID,
            "datascience_division_service_sub" : "mkt_monthly_2_os_roas"} ## 딕셔너리 형태로 붙일 수 있음.
    print("RUN_ID=", RUN_ID, "LABEL_ID=", LABELS)

    ### 1> os 별
    client = bigquery.Client()
    query = """
    WITH revraw AS(
    select  JoypleGameID, regmonth, osuser
    ,sum(RU) as RU,
    sum(Sales_D1) as sales_D1,
    sum(Sales_D3) as sales_D3,
    sum(Sales_D7) as sales_D7,
    CASE WHEN COUNTIF(sales_D14 IS NULL) >= 1 THEN null ELSE sum(Sales_D14) END as Sales_D14,
    CASE WHEN COUNTIF(Sales_D30 IS NULL) >= 1 THEN null ELSE sum(Sales_D30) END as Sales_D30,
    CASE WHEN COUNTIF(Sales_D60 IS NULL) >= 1 THEN null ELSE sum(Sales_D60) END as Sales_D60,
    CASE WHEN COUNTIF(Sales_D90 IS NULL) >= 1 THEN null ELSE sum(Sales_D90) END as Sales_D90,
    CASE WHEN COUNTIF(Sales_D120 IS NULL) >= 1 THEN null ELSE sum(Sales_D120) END as Sales_D120,
    CASE WHEN COUNTIF(Sales_D150 IS NULL) >= 1 THEN null ELSE sum(Sales_D150) END as Sales_D150,
    CASE WHEN COUNTIF(Sales_D180 IS NULL) >= 1 THEN null ELSE sum(Sales_D180) END as Sales_D180,
    CASE WHEN COUNTIF(Sales_D210 IS NULL) >= 1 THEN null ELSE sum(Sales_D210) END as Sales_D210,
    CASE WHEN COUNTIF(Sales_D240 IS NULL) >= 1 THEN null ELSE sum(Sales_D240) END as Sales_D240,
    CASE WHEN COUNTIF(Sales_D270 IS NULL) >= 1 THEN null ELSE sum(Sales_D270) END as Sales_D270,
    CASE WHEN COUNTIF(Sales_D300 IS NULL) >= 1 THEN null ELSE sum(Sales_D300) END as Sales_D300,
    CASE WHEN COUNTIF(Sales_D330 IS NULL) >= 1 THEN null ELSE sum(Sales_D330) END as Sales_D330,
    CASE WHEN COUNTIF(Sales_D360 IS NULL) >= 1 THEN null ELSE sum(Sales_D360) END as Sales_D360,
    from(
    select JoypleGameID, RegdateAuthAccountDateKST,
    FORMAT_DATE('%Y-%m' ,RegdateAuthAccountDateKST) as regmonth
    ,  osuser ,
    sum(RU) as RU,
    IFNULL(sum(rev_D1),0) as Sales_D1,
    IFNULL(sum(rev_D3),0) as Sales_D3,
    IFNULL(sum(rev_D7),0) as Sales_D7,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -15 DAY) AS Date)  THEN  IFNULL(sum(rev_D14),0) ELSE  null END as Sales_D14,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -31 DAY) AS Date)  THEN  IFNULL(sum(rev_D30),0) ELSE  null END as Sales_D30,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -61 DAY) AS Date)  THEN  IFNULL(sum(rev_D60),0) ELSE  null END as Sales_D60,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -91 DAY) AS Date)  THEN  IFNULL(sum(rev_D90),0) ELSE  null END as Sales_D90,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -121 DAY) AS Date)  THEN  IFNULL(sum(rev_D120),0) ELSE  null END as Sales_D120,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -151 DAY) AS Date)  THEN  IFNULL(sum(rev_D150),0) ELSE  null END as Sales_D150,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -181 DAY) AS Date)  THEN  IFNULL(sum(rev_D180),0) ELSE  null END as Sales_D180,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -211 DAY) AS Date)  THEN  IFNULL(sum(rev_D210),0) ELSE  null END as Sales_D210,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -241 DAY) AS Date)  THEN  IFNULL(sum(rev_D240),0) ELSE  null END as Sales_D240,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -271 DAY) AS Date)  THEN  IFNULL(sum(rev_D270),0) ELSE  null END as Sales_D270,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -301 DAY) AS Date)  THEN  IFNULL(sum(rev_D300),0) ELSE  null END as Sales_D300,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -331 DAY) AS Date)  THEN  IFNULL(sum(rev_D330),0) ELSE  null END as Sales_D330,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -361 DAY) AS Date)  THEN  IFNULL(sum(rev_D360),0) ELSE  null END as Sales_D360
    from(
    select *, case when countrycode = 'KR' then '1.KR'
        when countrycode = 'US' then '2.US'
        when countrycode = 'JP' then '3.JP'
        when countrycode in ('UK','FR','DE','GB') then '4.WEU'
        else '5.ETC' end as geo_user_group
        , case when OS = 'android' then 'And' when OS = 'ios' then 'IOS' else OS end as osuser
    from `dataplatform-reporting.DataService.T_0420_0000_UAPerformanceRaw_V1`
        where JoypleGameID in (131,133,30001,30003)
        and RegdateAuthAccountDateKST between '2025-01-01' and DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 8 DAY)
    )
    group by JoypleGameID, RegdateAuthAccountDateKST,  osuser
    ) group by JoypleGameID, regmonth,  osuser
    )




    , cost_raw AS(
    select joyplegameid,gameid,  format_date('%Y-%m', cmpgndate) as regmonth   , os
    , sum(costcurrency) as cost, sum(costcurrencyuptdt) as cost_exclude_credit,
    from(
    select *, case when countrycode = 'KR' then '1.KR'
        when countrycode = 'US' then '2.US'
        when countrycode = 'JP' then '3.JP'
        when countrycode in ('UK','FR','DE','GB') then '4.WEU'
        else '5.ETC' end as geo_user_group
    from  `dataplatform-reporting.DataService.V_0410_0000_CostCampaignRule_V`
    where joyplegameid in (131,133,30001,30003)
    and cmpgndate between '2025-01-01' and DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 8 DAY)
    )
    group by  joyplegameid,gameid,  format_date('%Y-%m', cmpgndate)  , os
    )



    select
    ifnull(a.joyplegameid , b.joyplegameid) as joyplegameid
    ,ifnull(a.regmonth , b.regmonth) as regmonth
    , ifnull(a.osuser, b.os) as os
    , a.ru
    ,a.sales_D1, a.sales_D3, a.sales_D7, a.sales_D14, a.sales_D30, a.sales_D60, a.sales_D90 , a.sales_D120, a.sales_D150, a.sales_D180
    , a.sales_D210, a.sales_D240, a.sales_D270, a.sales_D300, a.sales_D330, a.sales_D360
    , b.cost, b.cost_exclude_credit
    from revraw as a
    full join cost_raw as b
    on a.joyplegameid = b.joyplegameid
    and a.regmonth = b.regmonth
    and a.osuser = b.os

    """

    query_result_raw_os = client.query(query, job_config=bigquery.QueryJobConfig(labels=LABELS)).to_dataframe()

    ############# OS 전처리
    # ROAS 계산 (sales_d1 / cost, sales_d3 / cost, ..., sales_d360 / cost)
    cohort_columns = ['D1', 'D3', 'D7', 'D14', 'D30', 'D60', 'D90', 'D120', 'D150', 'D180', 'D210', 'D240', 'D270', 'D300', 'D330', 'D360']

    # 각 cohort에 대해 ROAS 계산
    for cohort in cohort_columns:
        query_result_raw_os[f'{cohort.lower()}_roas'] = query_result_raw_os[f'sales_{cohort}'] / query_result_raw_os['cost_exclude_credit']


    query_result_raw_os['cpru'] = query_result_raw_os['cost_exclude_credit'] / query_result_raw_os['ru']

    # os별 cost 비율 계산
    game_total_cost = query_result_raw_os.groupby(['joyplegameid', 'regmonth'])['cost_exclude_credit'].sum().reset_index(name='game_total_cost')

    # merge os_total_cost into query_result_raw2
    query_result_raw_os2 = pd.merge(query_result_raw_os, game_total_cost, on=['joyplegameid', 'regmonth'], how='left')

    query_result_raw_os2['os_cost_ratio'] = query_result_raw_os2['cost_exclude_credit'] / query_result_raw_os2['game_total_cost']

    mapping = {131:   "1.POTC", 133:   "2.GBTW", 30001: "3.WWMC", 30003: "4.DRSG",} # (NY수정) 유실된 mapping 데이터 추가
    query_result_raw_os2["game_name"] = query_result_raw_os2["joyplegameid"].map(mapping)  # 매핑 없으면 NaN
    query_result_raw_os2 = query_result_raw_os2[query_result_raw_os2['os'].isin(['And', 'IOS'])]

    select_cols = ['game_name' ,'regmonth','os','cost_exclude_credit','os_cost_ratio','d1_roas','d3_roas','d7_roas','d14_roas','d30_roas','d60_roas','d90_roas','d120_roas','d150_roas']
    query_result_raw_os3 = query_result_raw_os2[select_cols]

    # 테이블 형태로 문자열화 (Markdown table)
    def df_to_md(df):
        header = "| " + " | ".join(df.columns) + " |"
        sep = "| " + " | ".join(["---"]*len(df.columns)) + " |"
        rows = "\n".join("| " + " | ".join(map(str, row)) + " |" for row in df.values)
        return "\n".join([header, sep, rows])

    df_os = df_to_md(query_result_raw_os3.sort_values(['game_name', 'regmonth']).groupby('game_name').tail(12))

    # 데이터 입력
    prompt_description = f"""
    ## 데이터 입력
    SLG 게임 4종의 월별 OS별 ROAS 현황을 나타내는 데이터들이야.
    값이 NA인 Cohort변수(dn_roas)는 아직 mature되지 않은 지표야.

    --- OS별 ROAS 데이터 테이블 ---
    {df_os}

    """

    # Gemini에 전달할 전체 프롬프트 구성
    prompt_parts = [
        prompt_description,
        """
    ### OS별 마케팅 트렌드 분석 리포트 작성 요청

    주어진 OS별 ROAS 데이터를 기반으로 게임별로 다음 3가지 항목에 대해 분석 리포트를 작성해줘.

    1. **월별 OS 트렌드:** 게임별로 최근 3개월의 월별 iOS, Android의 Cost 비중, ROAS 성과가 증가하는지 감소하는지 트렌드를 언급해줘.
    2. **OS별 ROAS 비교 (Cohort 트렌드):** 초반 Cohort (d7, d14 등)와 장기 Cohort (d90, d150 등)의 ROAS를 비교하여 OS 간 차이가 어떻게 나타나는지 (예: '초반은 Android가 높으나 장기 코호트는 iOS가 높다') 상세히 설명해줘.
    3. **가장 최근월(당월) 현황:**
        - 가장 최근월의 OS Cost 비중 현황을 언급해줘.
        - 가장 최근월의 OS별 ROAS가 Android 대비 iOS가 높은지 낮은지 명확히 언급해줘.

    작성 시 아래의 예시 출력 형태를 참고하여 작성 부탁해.

    --- 예시 출력 형태 (마크다운 포맷) ---

    ## 게임명 (예: ## 1.POTC)

    1. **월별 OS 트렌드:** iOS Cost 비중은 X월부터 Y월까지 🔺상승(또는 🔻하락)하는 추세이며, iOS or Android의ROAS 가 X월부터 개선되는 흐름을 보임
    2. **OS별 ROAS Cohort 트렌드:** 초반 Cohort(d7~d30)의 ROAS는 Android가 iOS 대비 평균적으로 높으나, 장기 Cohort(d90~)로 갈수록 iOS가 Android 대비 높은 ROAS를 보임.
    3. **가장 최근월 현황:**
        - Cost 비중: 당월 iOS Cost 비중은 00%임.
        - ROAS 비교: 당월 초반 Cohort ROAS는 Android 대비 iOS가 낮고, 장기 Cohort ROAS는 Android 대비 iOS가 높음.

    """
    ]
    # Gemini API 호출 (사용자의 원래 호출 방식)
    # response_os = model.generate_content(prompt_parts, labels=LABELS)
    # print(response_os.text)

    genai_client = GeminiClient(
        vertexai=True,
        location="global"      # genai 호출용location 변경
    )

    config_3_optimized = GenerateContentConfig(
        temperature=1.0,
        thinking_config=types.ThinkingConfig(thinking_level="high"),
        system_instruction=system_instruction,
        labels=LABELS
    )

    response_os = genai_client.models.generate_content(
        model="gemini-3-pro-preview",   # Vertex AI 모델명
        contents = prompt_parts
        ,config=config_3_optimized
    )
    print(response_os.text)

    pivot_df = query_result_raw_os3.pivot_table(
        index=['game_name', 'regmonth'] ,
        columns='os',
        values=['cost_exclude_credit', 'os_cost_ratio',  'd7_roas', 'd14_roas', 'd30_roas'],
        aggfunc='first'
    )

    '''
    pivot_df = pivot_df[['total_cost', 'os_cost_ratio', 'd7_roas', 'd14_roas', 'd30_roas']]

    nest_asyncio.apply()


    styled_df = pivot_df.style \
        .format({
            "total_cost": "{:,.0f}",  # total_cost를 천 단위로 포맷
            **{col: "{:.1%}" for col in ['os_cost_ratio', 'd7_roas', 'd14_roas', 'd30_roas']}  # 비율 포맷
        }) \
        .bar(subset=['total_cost'], color='#fbe4e6') \
        .bar(subset=['d7_roas', 'd14_roas', 'd30_roas'], color='#b6d7a8') \
        .background_gradient(subset=['os_cost_ratio'], cmap='Reds')  # os_cost_ratio에 빨간색 그라데이션
    '''

    nest_asyncio.apply()

    # 컬럼 이름을 단일 인덱스로 평탄화
    pivot_df.columns = [f'{col}_{idx}' for col, idx in pivot_df.columns]

    pivot_df = pivot_df[['cost_exclude_credit_And', 'cost_exclude_credit_IOS', 'os_cost_ratio_IOS'
    ,'d7_roas_And', 'd7_roas_IOS', 'd14_roas_And', 'd14_roas_IOS', 'd30_roas_And', 'd30_roas_IOS']]


    '''
    # 각 월별로 And vs iOS 중 더 큰 값을 빨간색으로 표시
    def highlight_max(s):
        is_max = s == s.max()
        return ['color: red; font-weight: bold' if v else '' for v in is_max]



    # format과 스타일 적용
    styled_df = pivot_df.style \
        .format({
            "cost_exclude_credit_And": "{:,.0f}",  # total_cost를 천 단위로 포맷
                    "cost_exclude_credit_IOS": "{:,.0f}",  # total_cost를 천 단위로 포맷

            **{col: "{:.1%}" for col in ['os_cost_ratio_IOS', 'd7_roas_And', 'd14_roas_And', 'd30_roas_And', 'd7_roas_IOS', 'd14_roas_IOS', 'd30_roas_IOS']}  # 비율 포맷
        }) \
        .bar(subset=['total_cost_And','total_cost_IOS'], color='#fbe4e6') \
        .bar(subset=['d7_roas_And', 'd14_roas_And', 'd30_roas_And', 'd7_roas_IOS', 'd14_roas_IOS', 'd30_roas_IOS'], color='#b6d7a8') \
        .apply(highlight_max, subset=["d7_roas_And","d7_roas_IOS"], axis=1)
        .apply(highlight_max, subset=["d14_roas_And","d14_roas_IOS"], axis=1)
        .apply(highlight_max, subset=["d30_roas_And","d30_roas_IOS"], axis=1)
        .background_gradient(subset=['os_cost_ratio_IOS'], cmap='Reds')
    '''

    def highlight_max_bg(s):
        vmax = s.max(skipna=True)
        return [
            'background-color: pink' if (not pd.isna(v) and v == vmax) else ''
            for v in s
        ]



    styled_df = (
        pivot_df
        .style
        .format({
            "cost_exclude_credit_And": "{:,.0f}",
            "cost_exclude_credit_IOS": "{:,.0f}",
            **{col: "{:.1%}" for col in [
                "os_cost_ratio_IOS",
                "d7_roas_And","d14_roas_And","d30_roas_And",
                "d7_roas_IOS","d14_roas_IOS","d30_roas_IOS"
            ]}
        })
        .bar(subset=['cost_exclude_credit_And', 'cost_exclude_credit_IOS'], color="#fbe4e6")
        .bar(subset=[
            "d7_roas_And","d14_roas_And","d30_roas_And",
            "d7_roas_IOS","d14_roas_IOS","d30_roas_IOS"
        ], color="#b6d7a8")
        .apply(highlight_max_bg, subset=["d7_roas_And","d7_roas_IOS"], axis=1)
        .apply(highlight_max_bg, subset=["d14_roas_And","d14_roas_IOS"], axis=1)
        .apply(highlight_max_bg, subset=["d30_roas_And","d30_roas_IOS"], axis=1)
        .background_gradient(subset=["os_cost_ratio_IOS"], cmap="Reds")
    )



    nest_asyncio.apply()

    # HTML 템플릿 정의
    html_template = """
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <style>
            body { font-family: Arial, sans-serif; padding: 20px; }
            table { border-collapse: collapse; font-size: 13px; }
            th, td {
                border: 1px solid #999;
                padding: 6px 10px;
                text-align: right;
            }
            th { background-color: #f0f0f0; }
        </style>
    </head>
    <body>
        <h2>OS별 현황</h2>
        {{ table | safe }}
    </body>
    </html>
    """

    # HTML 렌더링 및 저장
    table_html = styled_df.to_html()
    rendered_html = Template(html_template).render(table=table_html)

    html_path = "os_roas.html"
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(rendered_html)

    # 이미지 캡처 비동기 함수
    async def capture_html_to_image():
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page(viewport={"width": 1600, "height": 1000})
            await page.goto("file://" + os.path.abspath(html_path))
            await page.screenshot(path="os_roas.png", full_page=True)
            await browser.close()

    # Spyder or GCP Notebook 환경 대응
    asyncio.get_event_loop().run_until_complete(capture_html_to_image())


    ########### (1) 제목
    notion.blocks.children.append(
        PAGE_ID,
        children=[
            {
                "object": "block",
                "type": "heading_1",
                "heading_1": {
                    "rich_text": [{"type": "text", "text": {"content": "2) OS별 ROAS 현황" }}]
                },
            }
        ],
    )



    ########### (2) OS 서식표 업로드

    # 업로드용 경로 변수
    from pathlib import Path
    IMG_PATH = Path("os_roas.png").resolve()  # 절대경로로 변환(권장)
    assert IMG_PATH.exists() and IMG_PATH.stat().st_size > 0

    hdr_json = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }

    # 1) 업로드 오브젝트 생성 (upload_url 받기)
    resp = requests.post(
        "https://api.notion.com/v1/file_uploads",
        json={"filename": IMG_PATH.name, "content_type": "image/png"},
        headers=hdr_json
    )
    resp.raise_for_status()
    fu = resp.json()
    file_upload_id = fu["id"]
    upload_url = fu.get("upload_url")


    # 2) 실제 전송 (multipart/form-data) ⇒ status 가 uploaded 여야 첨부 가능
    with open(IMG_PATH, "rb") as f:
        r2 = requests.post(
            # 권장: 명시적 send 엔드포인트 사용
            f"https://api.notion.com/v1/file_uploads/{file_upload_id}/send",
            # 또는 upload_url 사용 가능 (동일 동작): upload_url,
            headers={"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": NOTION_VERSION},
            files={"file": (IMG_PATH.name, f, "image/png")}
        )
    r2.raise_for_status()




    # 표 첨부
    notion.blocks.children.append(
        PAGE_ID,
        children=[
            {
                "object": "block",
                "type": "image",
                "image": {
                    "type": "file_upload",
                    "file_upload": {"id": file_upload_id},
                    #"caption": [
                    #    {"type": "text", "text": {"content": "ROAS & Cost (auto-upload)"}}
                    #]
                },
            }
        ]
    )




    ########### (3) gemini분석 내용 첨부

    blocks = md_to_notion_blocks(response_os.text + "\n\n\n")

    notion.blocks.children.append(
        block_id=PAGE_ID,
        children=blocks
    )

    from datetime import datetime, timezone, timedelta
    RUN_ID = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")

    LABELS = {"datascience_division_service": "monthly_mkt_framework",
            "run_id": RUN_ID,
            "datascience_division_service_sub" : "mkt_monthly_3_geo_roas"} ## 딕셔너리 형태로 붙일 수 있음.
    print("RUN_ID=", RUN_ID, "LABEL_ID=", LABELS)

    # 2> 국가별
    client = bigquery.Client()
    query = """
    WITH revraw AS(
    select  JoypleGameID, regmonth, geo_user_group
    ,sum(RU) as RU,
    sum(Sales_D1) as sales_D1,
    sum(Sales_D3) as sales_D3,
    sum(Sales_D7) as sales_D7,
    CASE WHEN COUNTIF(sales_D14 IS NULL) >= 1 THEN null ELSE sum(Sales_D14) END as Sales_D14,
    CASE WHEN COUNTIF(Sales_D30 IS NULL) >= 1 THEN null ELSE sum(Sales_D30) END as Sales_D30,
    CASE WHEN COUNTIF(Sales_D60 IS NULL) >= 1 THEN null ELSE sum(Sales_D60) END as Sales_D60,
    CASE WHEN COUNTIF(Sales_D90 IS NULL) >= 1 THEN null ELSE sum(Sales_D90) END as Sales_D90,
    CASE WHEN COUNTIF(Sales_D120 IS NULL) >= 1 THEN null ELSE sum(Sales_D120) END as Sales_D120,
    CASE WHEN COUNTIF(Sales_D150 IS NULL) >= 1 THEN null ELSE sum(Sales_D150) END as Sales_D150,
    CASE WHEN COUNTIF(Sales_D180 IS NULL) >= 1 THEN null ELSE sum(Sales_D180) END as Sales_D180,
    CASE WHEN COUNTIF(Sales_D210 IS NULL) >= 1 THEN null ELSE sum(Sales_D210) END as Sales_D210,
    CASE WHEN COUNTIF(Sales_D240 IS NULL) >= 1 THEN null ELSE sum(Sales_D240) END as Sales_D240,
    CASE WHEN COUNTIF(Sales_D270 IS NULL) >= 1 THEN null ELSE sum(Sales_D270) END as Sales_D270,
    CASE WHEN COUNTIF(Sales_D300 IS NULL) >= 1 THEN null ELSE sum(Sales_D300) END as Sales_D300,
    CASE WHEN COUNTIF(Sales_D330 IS NULL) >= 1 THEN null ELSE sum(Sales_D330) END as Sales_D330,
    CASE WHEN COUNTIF(Sales_D360 IS NULL) >= 1 THEN null ELSE sum(Sales_D360) END as Sales_D360,
    from(
    select JoypleGameID, RegdateAuthAccountDateKST,
    FORMAT_DATE('%Y-%m' ,RegdateAuthAccountDateKST) as regmonth
    , geo_user_group,
    sum(RU) as RU,
    IFNULL(sum(rev_D1),0) as Sales_D1,
    IFNULL(sum(rev_D3),0) as Sales_D3,
    IFNULL(sum(rev_D7),0) as Sales_D7,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -15 DAY) AS Date)  THEN  IFNULL(sum(rev_D14),0) ELSE  null END as Sales_D14,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -31 DAY) AS Date)  THEN  IFNULL(sum(rev_D30),0) ELSE  null END as Sales_D30,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -61 DAY) AS Date)  THEN  IFNULL(sum(rev_D60),0) ELSE  null END as Sales_D60,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -91 DAY) AS Date)  THEN  IFNULL(sum(rev_D90),0) ELSE  null END as Sales_D90,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -121 DAY) AS Date)  THEN  IFNULL(sum(rev_D120),0) ELSE  null END as Sales_D120,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -151 DAY) AS Date)  THEN  IFNULL(sum(rev_D150),0) ELSE  null END as Sales_D150,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -181 DAY) AS Date)  THEN  IFNULL(sum(rev_D180),0) ELSE  null END as Sales_D180,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -211 DAY) AS Date)  THEN  IFNULL(sum(rev_D210),0) ELSE  null END as Sales_D210,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -241 DAY) AS Date)  THEN  IFNULL(sum(rev_D240),0) ELSE  null END as Sales_D240,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -271 DAY) AS Date)  THEN  IFNULL(sum(rev_D270),0) ELSE  null END as Sales_D270,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -301 DAY) AS Date)  THEN  IFNULL(sum(rev_D300),0) ELSE  null END as Sales_D300,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -331 DAY) AS Date)  THEN  IFNULL(sum(rev_D330),0) ELSE  null END as Sales_D330,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -361 DAY) AS Date)  THEN  IFNULL(sum(rev_D360),0) ELSE  null END as Sales_D360
    from(
    select *, case when countrycode = 'KR' then '1.KR'
        when countrycode = 'US' then '2.US'
        when countrycode = 'JP' then '3.JP'
        when countrycode in ('UK','FR','DE','GB') then '4.WEU'
        else '5.ETC' end as geo_user_group
        , case when OS = 'android' then 'And' when OS = 'ios' then 'IOS' else OS end as osuser
    from `dataplatform-reporting.DataService.T_0420_0000_UAPerformanceRaw_V1`
        where JoypleGameID in (131,133,30001,30003)
        and RegdateAuthAccountDateKST between '2025-01-01' and DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 8 DAY)
    )
    group by JoypleGameID, RegdateAuthAccountDateKST, geo_user_group
    ) group by JoypleGameID, regmonth, geo_user_group
    )




    , cost_raw AS(
    select joyplegameid,gameid,  format_date('%Y-%m', cmpgndate) as regmonth   , geo_user_group
    , sum(costcurrency) as cost, sum(costcurrencyuptdt) as cost_exclude_credit,
    from(
    select *, case when countrycode = 'KR' then '1.KR'
        when countrycode = 'US' then '2.US'
        when countrycode = 'JP' then '3.JP'
        when countrycode in ('UK','FR','DE','GB') then '4.WEU'
        else '5.ETC' end as geo_user_group
    from  `dataplatform-reporting.DataService.V_0410_0000_CostCampaignRule_V`
    where joyplegameid in (131,133,30001,30003)
    and cmpgndate between '2025-01-01' and DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 8 DAY)
    )
    group by  joyplegameid,gameid,  format_date('%Y-%m', cmpgndate) , geo_user_group
    )



    select
    ifnull(a.joyplegameid , b.joyplegameid) as joyplegameid
    ,ifnull(a.regmonth , b.regmonth) as regmonth
    , ifnull(a.geo_user_group, b.geo_user_group) as geo_user_group
    , a.ru
    ,a.sales_D1, a.sales_D3, a.sales_D7, a.sales_D14, a.sales_D30, a.sales_D60, a.sales_D90 , a.sales_D120, a.sales_D150, a.sales_D180
    , a.sales_D210, a.sales_D240, a.sales_D270, a.sales_D300, a.sales_D330, a.sales_D360
    , b.cost, b.cost_exclude_credit
    from revraw as a
    full join cost_raw as b
    on a.joyplegameid = b.joyplegameid
    and a.regmonth = b.regmonth
    and a.geo_user_group = b.geo_user_group
    """

    query_result_raw_geo = client.query(query, job_config=bigquery.QueryJobConfig(labels=LABELS)).to_dataframe()

    ########## 국가별 전처리
    # 각 cohort에 대해 ROAS 계산
    for cohort in cohort_columns:
        query_result_raw_geo[f'{cohort.lower()}_roas'] = query_result_raw_geo[f'sales_{cohort}'] / query_result_raw_geo['cost_exclude_credit']


    query_result_raw_geo['cpru'] = query_result_raw_geo['cost_exclude_credit'] / query_result_raw_geo['ru']

    query_result_raw_geo2 = pd.merge(query_result_raw_geo, game_total_cost, on=['joyplegameid', 'regmonth'], how='left')
    query_result_raw_geo2['geo_cost_ratio'] = query_result_raw_geo2['cost_exclude_credit'] / query_result_raw_geo2['game_total_cost']



    query_result_raw_geo2["game_name"] = query_result_raw_geo2["joyplegameid"].map(mapping)  # 매핑 없으면 NaN

    select_cols = ['game_name' ,'regmonth','geo_user_group','cost_exclude_credit','geo_cost_ratio','d1_roas','d3_roas','d7_roas','d14_roas','d30_roas','d60_roas','d90_roas','d120_roas','d150_roas']
    query_result_raw_geo3 = query_result_raw_geo2[select_cols]

    df_geo = df_to_md(query_result_raw_geo3.sort_values(['game_name', 'regmonth']).groupby('game_name').tail(30))
    #### gemini 분석 데이터 필터
    # 최근 6개월 데이터만 추출

    recent_data2 = recent_data[[ "game_name","regmonth", "cost_exclude_credit","cpru" ,"d360roas_growth","month_start"]]


    # 테이블에 대한 설명

    # 데이터 학습 및 정의
    prompt_description = f"""
    SLG 게임 4종의 월별, OS별, 권역별 ROAS 현황을 나타내는 데이터들이야.
    값이 NA인 Cohort변수(dn_roas)는 아직 mature되지 않은 지표야.

    [주요 변수 설명]
    - cost_exclude_credit: 크레딧 제외 월별 마케팅비 (roas 계산 기준, 당월은 일할계산 추정 월 cost)
    - cpru: 단가
    - d360roas_growth: 복귀유저 미포함 신규유저 d360 예측치
    - month_start : 기준이 되는 월

    ### 데이터셋
    1. 전체 유저 기준 데이터:
    {recent_data2}

    2. df_geo (국가별):
    {df_geo}

    3. df_os (OS별):
    {df_os}
    """


    # 3. Gemini에 전달할 전체 프롬프트 구성
    prompt_parts = [
        prompt_description,

    """
    ### 마케팅 분석 리포트 작성 요청

    세 가지 데이터셋을 기반으로 다음 3가지 항목에 대해 게임별 분석 리포트를 작성해줘

    1. **전체 트렌드 분석:**
        -  `month_start` 기준으로 최근 3개월 데이터의 트렌드를 설명해줘.
        - `d360roas_growth` 지표를 사용하여 **최근 3개월간**의 **전월 대비 당월 ROAS 변화** 추이를 요약
        - `recent_data2`의 **cost** 및 **cpru** 지표를 참고하여 최근 3개월간의 **Cost와 단가(CPRU) 트렌드**가 증가하는지 감소하는지 언급해줘

    2. **원인 분석 (국가/OS):**
        - `df_geo`와 `df_os`를 참고하여 **가장 최근 월의 ROAS 변동에 가장 큰 영향을 준 요인** (국가 또는 OS)을 찾고, 해당 요인의 **월별 성과**를 비교해줘
        - **비교는 반드시 동일한 Cohort(dN)를 기준으로 월별**로 진행해

    3. **액션 아이템 제안:**
        - 분석 내용을 바탕으로 명확하고 실행 가능한 **액션 아이템**을 제안해줘
        - 제안은 특정 권역의 특정 기간 성과를 기준으로 **예산 증/감소** 또는 **특정 OS 예산 증/감소** 등.

    작성 시 아래의 형태를 지켜서 마크다운형태로 작성 부탁해

    ## 게임명 (예: ## 1.POTC)

    1. **전체 트렌드:** ...
    2. **원인 분석:**
        - 국가별 분석:...
        - OS별 분석: ...
    3. **제안: ...
    """
    ]

    genai_client = GeminiClient(
        vertexai=True,
        location="global"      # genai 호출용location 변경
    )

    config_3_optimized = GenerateContentConfig(
        temperature=1.0,
        thinking_config=types.ThinkingConfig(thinking_level="high"),
        system_instruction=system_instruction,
        labels=LABELS
    )

    response_total = genai_client.models.generate_content(
        model="gemini-3-pro-preview",   # Vertex AI 모델명
        contents = prompt_parts
        ,config=config_3_optimized
    )
    print(response_total.text)

    ########### (1) 제목
    notion.blocks.children.append(
        PAGE_ID,
        children=[
            {
                "object": "block",
                "type": "heading_1",
                "heading_1": {
                    "rich_text": [{"type": "text", "text": {"content": "3) 종합 결론" }}]
                },
            }
        ],
    )


    ## 종합 해석
    blocks = md_to_notion_blocks(response_total.text + "\n\n\n")
    notion.blocks.children.append(
        block_id=PAGE_ID,
        children=blocks
    )

    print("✅ Append 완료")

    if ref_person_ids:
        props_update = {
            "참조자": {"people": [{"id": pid} for pid in ref_person_ids]}
        }
        updated_page = notion.pages.update(
            page_id=PAGE_ID,
            properties=props_update
        )
        print("✅ Notion 페이지 업데이트 완료 (참조자 추가):", updated_page["url"])


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
    dag_id='Marketing_Monthly_Report',
    default_args=default_args,
    description='월간 마케팅 리포트 생성 (notion + gemini)',
    schedule='10 5 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['marketing', 'report', 'monthly'],
) as dag:
    
    # Task 정의
    task = PythonOperator(
        task_id='mkt_monthly_report_total',
        python_callable=mkt_monthly_report_total,
        dag=dag,
    )