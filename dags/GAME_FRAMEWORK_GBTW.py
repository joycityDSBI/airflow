import time
import pandas as pd
from google.cloud import bigquery
from google import genai
from google.genai import types
from goole.cloud import storage
from vertexai import rag
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
from game_framework_util import *

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

logger = logging.getLogger(__name__)

# 환경 변수 가져오기
def get_var(key: str, default: str = None) -> str:
    """환경 변수 또는 Airflow Variable 조회"""
    return os.environ.get(key) or Variable.get(key, default_var=default)


# 변수 생성
t0 = time.time()
PROJECT_ID = "data-science-division-216308"
LOCATION = "us-central1"
MODEL_NAME = "gemini-2.5-flash"

NOTION_TOKEN=get_var("MS_TEAM_NOTION_TOKEN") # MS팀 API 키
NOTION_VERSION=get_var("NOTION_API_VERSION")
DATABASE_ID=get_var("GAMEFRAMEWORK_GBTW_NOTION_DB_ID")
CREDENTIALS_JSON = get_var('GOOGLE_CREDENTIAL_JSON')

cred_dict = json.loads(CREDENTIALS_JSON)
credentials, project_id = google.auth.default(
    scopes=["https://www.googleapis.com/auth/cloud-platform"]
)
credentials.refresh(Request())


# 클라이언트 모음
genai_client = Client(vertexai=True,project=PROJECT_ID,location=LOCATION)
bigquery_client = bigquery.Client(project=PROJECT_ID, credentials=credentials)# location=LOCATION ## us-central1 로 할 경우 허브 조회불가능
notion = Client(auth=NOTION_TOKEN)
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


## 페이지 생성 함수 //////////// task 함수
def make_gameframework_notion_page(gameidx: str, **context):

    url = "https://api.notion.com/v1/pages"
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }

    # 타임존 지정
    kst = ZoneInfo("Asia/Seoul")
    # 오늘
    today_kst = datetime.now(kst).date()
    # 어제
    yesterday_kst = today_kst - timedelta(days=1)

    # 이번 달 1일 (어제 날짜 기준)
    first_day = yesterday_kst.replace(day=1)

    # 타이틀 문자열 만들기
    title = f"{yesterday_kst.strftime('%y')}년 {yesterday_kst.month}월 매출현황( ~ {yesterday_kst})"
    print(f"{title} : {gameidx}")

    # 페이지 생성 요청 바디
    data = {
        "parent": {"database_id": DATABASE_ID},
        "properties": {
            "이름": {
                "title": [
                    {"text": {"content": title }}
                ]
            },
            "등록 날짜": {
                "date": {"start": today_kst.isoformat() }
            },
            "프로젝트": {
                "multi_select": [
                    {"name": {gameidx}}   # 다중 선택 옵션
                ]
            },
            "리포트 종류": {
                "multi_select": [
                    {"name": "게임분석"}   # 다중 선택 옵션
                ]
            },
            "작성자": {
                "people": [
                    {"id": "ce95f16a-6b6b-447d-a996-a9c5f0cc0113"},  # Notion user_id
                    {"id": "662575bc-731c-481c-afc7-13b2fdf5482a"}  # Notion user_id
                ]
            }
        }
    }

    res = requests.post(url, headers=headers, json=data)

    if res.status_code == 200:
        page_info = res.json() # ✅ 페이지 ID page_info["id"]
        print(f"✅ 페이지 생성 성공 ✅ 페이지 ID : {page_info["id"]}")
    else:
        print(f"⚠️ 에러 발생: {res.status_code} >> {res.text}")

    notion.blocks.children.append(
        block_id=page_info["id"] ,
        children=[
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {"content": " ◾ 목차"},
                            "annotations": {"bold": True}
                        }
                    ]
                }
            }
        ]
    )

    # 목차 블록 추가
    notion.blocks.children.append(
        block_id=page_info["id"] ,
        children=[
            {
                "object": "block",
                "type": "table_of_contents",
                "table_of_contents": {
                    "color": "default"  # "gray", "brown", "orange", "yellow", "green", "blue", "purple", "pink", "red" 가능
                }
            }
        ],
    )
    
    if res.status_code == 200:
        page_info = res.json()
        print(f"✅ 페이지 생성 성공 ✅ 페이지 ID : {page_info['id']}")
    else:
        print(f"⚠️ 에러 발생: {res.status_code} >> {res.text}")

    context['task_instance'].xcom_push(key='page_info', value=page_info)

    return page_info 



################# notion 페이지 생성 함수 실행 ############################

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
        elif line.lstrip().startswith("* "):
            indent = len(line) - len(line.lstrip())  # 들여쓰기 레벨
            content = line.strip()[2:].strip()

            block = {
                "object": "block",
                "type": "bulleted_list_item",
                "bulleted_list_item": {
                    "rich_text": parse_rich_text(content),
                    "children": []
                }
            }

            # indent 기반 계층 처리
            level = indent // indent_unit + 1
            while len(stack) > level:
                stack.pop()
            stack[-1].append(block)
            stack.append(block["bulleted_list_item"]["children"])
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


def df_to_notion_table_under_toggle(
    notion: Client,
    page_id: str,
    df: pd.DataFrame,
    toggle_title: str = "📊 Data Table",
    max_first_batch_rows: int = 90,
    batch_size: int = 100,
    has_column_header: bool = True,
    has_row_header: bool = False,
    ):
    """
    Notion 페이지에 토글을 만들고, 그 아래에 Pandas DataFrame을 표(Table)로 업로드합니다.
    - 최초 생성 시 테이블의 header + 초기 행들을 table.children 안에 포함
    - 이후 남은 행들은 table_row로 배치 append

    Parameters
    ----------
    notion : notion_client.Client
        Notion SDK 클라이언트 (Client(auth=...) 로 생성)
    page_id : str
        테이블을 추가할 페이지(혹은 블록) ID
    df : pandas.DataFrame
        업로드할 데이터프레임
    toggle_title : str
        토글 타이틀
    max_first_batch_rows : int
        테이블 최초 생성 시 포함할 초기 행 개수(Too many children 방지용)
    batch_size : int
        이후 배치 append 시 묶음 크기
    has_column_header : bool
        Notion 테이블 옵션 - 컬럼 헤더 사용 여부
    has_row_header : bool
        Notion 테이블 옵션 - 행 헤더 사용 여부

    Returns
    -------
    dict : {"toggle_id": str, "table_id": str, "rows_created": int}
    """
    # 1) 토글 생성
    toggle_resp = notion.blocks.children.append(
        page_id,
        children=[
            {
                "object": "block",
                "type": "toggle",
                "toggle": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {"content": toggle_title[:2000]},
                            "annotations": {
                                "bold": True,
                                "italic": False,
                                "underline": False,
                                "strikethrough": False,
                                "code": False,
                                "color": "blue"   # ← 색상 지정
                            },
                        }
                    ]
                },
            }
        ],
    )
    toggle_id = toggle_resp["results"][0]["id"]

    # 2) 헤더/초기행 준비
    table_width = len(df.columns)

    # 헤더(컬럼명)
    header_cells = []
    for col in df.columns.astype(str).tolist():
        header_cells.append([{"type": "text", "text": {"content": str(col)[:2000]}}])

    # 초기 데이터 행
    first_rows_blocks = []
    row_count = 0
    for _, row in df.iterrows():
        if row_count >= max_first_batch_rows:
            break
        row_cells = []
        for col in df.columns:
            val = row[col]
            #s = "" if (val is None or (isinstance(val, float) and math.isnan(val))) else str(val)
            s = "" if pd.isna(val) else str(val)
            row_cells.append([{"type": "text", "text": {"content": s[:2000]}}])

        # 열 수 안전장치(패딩/절단)
        if len(row_cells) < table_width:
            row_cells += [[{"type": "text", "text": {"content": ""}}]] * (table_width - len(row_cells))
        elif len(row_cells) > table_width:
            row_cells = row_cells[:table_width]

        first_rows_blocks.append(
            {"object": "block", "type": "table_row", "table_row": {"cells": row_cells}}
        )
        row_count += 1

    # 3) 테이블 블록 생성: table.children 안에 header + 초기행 포함(중요)
    table_block = {
        "object": "block",
        "type": "table",
        "table": {
            "table_width": table_width,
            "has_column_header": has_column_header,
            "has_row_header": has_row_header,
            "children": (
                [
                    {
                        "object": "block",
                        "type": "table_row",
                        "table_row": {"cells": header_cells},
                    }
                ]
                + first_rows_blocks
            ),
        },
    }

    table_create_resp = notion.blocks.children.append(toggle_id, children=[table_block])
    table_id = table_create_resp["results"][0]["id"]

    # 4) 남은 행들 배치 추가
    total = len(df)
    start = row_count
    while start < total:
        end = min(start + batch_size, total)
        batch_children = []

        for _, row in df.iloc[start:end].iterrows():
            row_cells = []
            for col in df.columns:
                val = row[col]
                s = "" if (val is None or (isinstance(val, float) and math.isnan(val))) else str(val)
                row_cells.append([{"type": "text", "text": {"content": s[:2000]}}])

            if len(row_cells) < table_width:
                row_cells += [[{"type": "text", "text": {"content": ""}}]] * (table_width - len(row_cells))
            elif len(row_cells) > table_width:
                row_cells = row_cells[:table_width]

            batch_children.append(
                {"object": "block", "type": "table_row", "table_row": {"cells": row_cells}}
            )

        notion.blocks.children.append(table_id, children=batch_children)
        start = end

    return {"toggle_id": toggle_id, "table_id": table_id, "rows_created": total}


# ─────────────────────────────
# 사용 예시
# ─────────────────────────────
# from notion_client import Client
# notion = Client(auth=NOTION_TOKEN)
# resp = df_to_notion_table_under_toggle(
#     notion=notion,
#     page_id=PAGE_ID,
#     df=query_result1_dailySales,
#     toggle_title="📊 Daily Sales (DataFrame Table)",
#     max_first_batch_rows=90,
#     batch_size=100,
# )
# print(resp)

### 일자별 매출
# 쿼리 & 제미나이 프롬프트

def query_run_method(service_sub: str, query):
    RUN_ID = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    LABELS = {"datascience_division_service": 'gameinsight_framework',
            "run_id": RUN_ID,
            f"datascience_division_service_sub" : {service_sub}} ## 딕셔너리 형태로 붙일 수 있음.
    print("📧 RUN_ID=", RUN_ID, "📧 LABEL_ID=", LABELS)

    query_result = bigquery_client.query(query, job_config=bigquery.QueryJobConfig(labels=LABELS)).to_dataframe()
    return query_result

## 일자별 매출
def Daily_revenue_query(joyplegameid: int, **context):
    query = f"""

    select day
    , cast(sum(if(monthtype = '지난달' , pricekrw, null ))as int64) as `지난달`
    , cast(sum(if(monthtype = '이번달' , pricekrw, null ))as int64) as `이번달`

    from
    (select *
    , format_date('%Y-%m',  logdatekst ) as Month
    , case when logdatekst >= DATE_SUB(DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH), INTERVAL 1 MONTH)
    and logdatekst< DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH) then '지난달'
    when logdatekst >= DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
    and logdatekst <= LAST_DAY(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH) then '이번달'
    else 'etc' end as monthtype
    , format_date('%d',  logdatekst ) as day
    from `dataplatform-reporting.DataService.V_0317_0000_AuthAccountPerformance_V`
    where joyplegameid = {joyplegameid}
    and logdateKst>= DATE_SUB(DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH), INTERVAL 1 MONTH)
    #and logdatekst>= DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
    and logdatekst<=LAST_DAY(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
    )
    group by 1
    order by 1

    """
    query_result = query_run_method('1_daily_sales', query)
    context['task_instance'].xcom_push(key='daily_revenue_df', value=query_result)

    return True
    
    
#### 전년 대비 월 매출 추이
def Daily_revenue_YOY_query(joyplegameid: int, **context):
    query = f"""

    select month
    , cast(sum(if(yeartype = '작년' , pricekrw, null ))as int64) as `작년`
    , cast(sum(if(yeartype = '올해' , pricekrw, null ))as int64) as `올해`

    from
    (select *
    , case
    when logdatekst >= DATE_SUB(DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), YEAR), INTERVAL 1 YEAR)
    and logdatekst< DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), YEAR) then '작년'

    when logdatekst >= DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), YEAR)
    and logdatekst <= LAST_DAY(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), YEAR) then '올해'
    else 'etc' end as yeartype
    , format_date('%m',  logdatekst ) as month
    , format_date('%Y',  logdatekst ) as year

    from `dataplatform-reporting.DataService.V_0317_0000_AuthAccountPerformance_V`
    where joyplegameid = {joyplegameid}
    and logdateKst>= DATE_SUB(DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), YEAR), INTERVAL 1 YEAR)
    and logdatekst<=LAST_DAY(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), YEAR)
    )
    group by 1
    order by 1

    """
    query_result = query_run_method('1_daily_sales', query)
    context['task_instance'].xcom_push(key='Daily_revenue_YOY_df', value=query_result)

    return True


## 현재 매출과 목표 매출
def Daily_revenue_target_revenue_query(joyplegameid: int, gameidx: str, **context):
    query = f"""
    ### 1> 이번달 일자별 매출 실측치
    with thismonthRev as (
    select day ## 일자
    , lastDay ## 이번달 마지막날 (ex - 30)
    , sum(pricekrw) as rev ## 매출액
    from
    (select *
    , cast(format_date('%d',  logdatekst ) as int64) as day ## 일자
    , EXTRACT(DAY FROM LAST_DAY(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)) as lastDay
    from `dataplatform-reporting.DataService.V_0317_0000_AuthAccountPerformance_V`
    where joyplegameid = {joyplegameid}
    and logdatekst>= DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
    and logdatekst<=DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY)
    )
    group by day,lastDay
    order by day
    ),

    ### 2> 목표매출 테이블
    salesGoal as (
    select
        CAST(REPLACE(sales, ',', '') AS INT64) as salesGoalMonthly # 쉼표 포함한 string 형태로 적재되어있어서 int64 형태로 전처리
    , CAST(REPLACE(sales, ',', '') AS INT64)/cast(num_of_days as int64) as salesGoalDaily # 일평균 목표매출
    from `data-science-division-216308.gameInsightFramework.slgMonthlyGoal`
    where idx = {gameidx}
    and month = FORMAT_DATE('%Y-%m', DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY))
    ),

    ### 3> 합치기
    thismonthRev2 as (


    select
        a.day, a.lastDay, a.rev
    , b.salesGoalDaily
    , c.maxDay ## 이번달 며칠까지 기간 찼는지
    #, case when maxDay >5 then rev ## 5일치 이상의 매출이 있으면 그냥 일할계산
    #       when maxDay<=5 and day=1 then salesGoalDaily ## 5일치 이하의 매출만 있다면 , 1일자 매출을 보정치 적용
    #       else rev end as rev2
    , a.rev as rev2
    from
    ## 일자별 매출 실측
    (select * from thismonthRev) as a

    ## 목표매출 (월별, 일별)
    cross join
    (select *
    from salesGoal
    ) as b

    ## 현재 며칠까지 매출 있는지 -> 5일 이전인지 이후인지 확인용도
    cross join
    (select cast(max(day) as int64) as maxDay from thismonthRev) as c

    )

    #select * from thismonthRev_and_revGoal order by day

    ### 4> 전처리
    select cast(current_sales as int64) as current_sales, b.salesGoalMonthly
    from
    (select (rev/maxDay)*lastDay as current_sales
    from
    (select sum(rev2) as rev, max(maxDay) as maxDay, max(lastDay) as lastDay
    from thismonthRev2)
    ) as a
    cross join
    (select * from salesGoal) as b

    """

    query_result = query_run_method('1_daily_sales', query)
    context['task_instance'].xcom_push(key='Daily_revenue_target_revenue_df', value=query_result)

    return True


## 전년 대비 월 매출 추이 수정 - 당월은 일할계산 매출
def merge_daily_revenue(joyplegameid: int, gameidx: str, **context):

    s_total = context['task_instance'].xcom_pull(
        task_ids = 'Daily_revenue_query',
        key='daily_revenue_df'
    )
    val_total = context['task_instance'].xcom_pull(
        task_ids = 'Daily_revenue_YOY_query',
        key='Daily_revenue_YOY_df'
    )

    val = val_total.iat[0, 0]
    s = s_total.iloc[:, 2]
    try:
        idx = s.dropna().index[-1]                 # 마지막 non-null 라벨 인덱스
        s_total.loc[idx, s_total.columns[2]] = val
    except IndexError:
        pass  # 모두 null인 경우

    return s_total


## 프롬프트 
### 4> 일자별 매출에 대한 제미나이 코멘트
def daily_revenue_gemini(joyplegameid: int, service_sub: str, **context):

    query_result1_dailySales = context['task_instance'].xcom_pull(
        task_ids = 'Daily_revenue_query',
        key='daily_revenue_df'
    )

    query_result1_monthlySales = context['task_instance'].xcom_pull(
        task_ids = 'Daily_revenue_YOY_query',
        key='Daily_revenue_YOY_df'
    )

    RUN_ID = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    LABELS = {"datascience_division_service": 'gameinsight_framework',
            "run_id": RUN_ID,
            f"datascience_division_service_sub" : {service_sub}}

    response1_salesComment = genai_client.models.generate_content(
    model=MODEL_NAME,
    contents = f"""
    당월 매출은 일할계산시 {f"{int((query_result1_monthlySales.iat[0,0])):,}"}이고 목표는 {f"{int((query_result1_monthlySales.iat[0,1])):,}"}이야.
    당월 매출은 일할계산시 ~~이고 목표매출은 ~~ 으로, 목표대비 얼마 달성했다의 형식으로 답변해줘.
    그리고 추가로 일자별 매출에 대해 아주 간단히 코멘트를 해줘.
    ~습니다 체로 알려줘

    그리고 전년동월대비어떤지 3줄이내로 간단히 알려줘.

    앞으로 어떻게 해야겠다는 사견은 쓰지마.
    한 문장마다 노션의 마크다운 리스트 문법을 사용해줘. e.g. * 당월 매출은 이렇습니다.
    <일자별 총 매출>
    {query_result1_dailySales}

    < 전년 동월대비 매출>
    {query_result1_monthlySales}
    """,
    config=types.GenerateContentConfig(
            system_instruction=SYSTEM_INSTRUCTION,
            # tools=[RAG],
            temperature=0.5,
            labels=LABELS
        )

    )
    # 코멘트 출력
    return response1_salesComment.text

# 코멘트 정리 ( 향후 요약에 사용하기 용도 )
#gemini_result.loc[len(gemini_result)] = response.text

# ## 한글깨짐 방지를 위해 폰트 지정
# font_path = "/usr/share/fonts/truetype/nanum/NanumGothic.ttf"
# if Path(font_path).exists():
#     fm.fontManager.addfont(font_path)       # 수동 등록
#     mpl.rc('font', family='NanumGothic')    # 기본 폰트 지정
#     mpl.rc('axes', unicode_minus=False)     # 마이너스 깨짐 방지
# else:
#     print("⚠️ NanumGothic 설치 실패. 다른 폰트를 써야 합니다.")

## 그래프 그리기 : arg 값으로 게임 코드
def daily_revenue_graph_draw(joyplegameid: int, gameidx: str, daily_revenue_query_path:str, **context):

    df_daily = load_df_from_gcs(bucket, daily_revenue_query_path.split('/')[-1])
    
    x  = df_daily.iloc[:, 0]
    y1 = pd.to_numeric(df_daily.iloc[:, 1], errors='coerce')
    y2 = pd.to_numeric(df_daily.iloc[:, 2], errors='coerce')

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(x, y2, marker='o',
            markersize=3, linewidth=1, # 마커 크기 작게
            label=df_daily.columns[2])
    ax.plot(x, y1, marker='o',
            markersize=3, linewidth=1, # 마커 크기 작게
            linestyle='--', label=df_daily.columns[1])  # 겹쳐서 표시

    # 옵션
    plt.title("일자별 매출")
    #plt.xlabel(query_result1_dailySales.columns[0])   # 자동으로 컬럼명 표시 가능
    #plt.ylabel(query_result1_dailySales.columns[1])

    # y축 천 단위 구분 기호 넣기
    plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{int(x):,}"))

    # x축 눈금을 7개 단위로만 표시 (예: 1주일 간격)
    plt.xticks(df_daily[df_daily.columns[0]][::2], rotation=45)

    # 범례 표시 - 그래프랑 안겹치게
    plt.legend(
        bbox_to_anchor=(1.05, 1),   # 그래프 오른쪽 바깥 (x=1.05, y=1)
        loc='upper left',           # 앵커 기준 위치
        borderaxespad=0.             # 축과 간격
    )

    # y축 0부터 시작 (안하면 눈금 최소값 자종 조정)
    plt.ylim(0, None)   # None이면 최대값은 자동으로 맞춰짐

    # y축 보조선
    plt.grid(axis='y', linestyle='--', alpha=0.7) # alpha=투명도
    #plt.grid(axis='x', linestyle='--', alpha=0.7) # alpha=투명도

    #plt.xlabel("날짜")
    #plt.ylabel("매출")

    #plt.show()
    # 그래프 안잘리게
    plt.tight_layout()


    # 향후 노션업로드하기 위해 저장
    # #print(os.getcwd()) 이 곳에 저장되고, colab 환경이라 좌측 폴더모양 누르면 png 있음.
    # 세션 종료시 자동으로 삭제됨
    ####################################### 이미지 파일을 저장할 path가 필요함 #####################
    filepath1_dailySales = "graph1_dailySales.png"
    plt.savefig(filepath1_dailySales, dpi=160) # dpi : 해상도
    plt.close()

    blob = bucket.blob(f'{gameidx}/{filepath1_dailySales}')
    blob.upload_from_filename(filepath1_dailySales)

    # 메모리에 올라간 이미지 파일 삭제
    os.remove(filepath1_dailySales)

    return f'{gameidx}/{filepath1_dailySales}'


## 월간 매출 그래프 그리기
def daily_revenue_YOY_graph_draw(joyplegameid: int, gameidx: str, **context):

    query_result1_monthlySales = context['task_instance'].xcom_pull(
        task_ids='Daily_revenue_YOY_query',  # ← 첫 번째 Task의 task_id
        key='Daily_revenue_YOY_df'
    )

    x  = query_result1_monthlySales.iloc[:, 0]
    y1 = pd.to_numeric(query_result1_monthlySales.iloc[:, 1], errors='coerce')
    y2 = pd.to_numeric(query_result1_monthlySales.iloc[:, 2], errors='coerce')

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(x, y2, marker='o',
            markersize=3, linewidth=1, # 마커 크기 작게
            label=query_result1_monthlySales.columns[2])
    ax.plot(x, y1, marker='o',
            markersize=3, linewidth=1, # 마커 크기 작게
            linestyle='--', label=query_result1_monthlySales.columns[1])  # 겹쳐서 표시

    # 옵션
    plt.title("전년 동월대비 매출")
    #plt.xlabel(query_result1_monthlySales.columns[0])   # 자동으로 컬럼명 표시 가능
    #plt.ylabel(query_result1_monthlySales.columns[1])

    # y축 천 단위 구분 기호 넣기
    plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{int(x):,}"))

    # x축 눈금을 7개 단위로만 표시 (예: 1주일 간격)
    plt.xticks(query_result1_monthlySales[query_result1_monthlySales.columns[0]][::1], rotation=45)

    # 범례 표시 - 그래프랑 안겹치게
    plt.legend(
        bbox_to_anchor=(1.05, 1),   # 그래프 오른쪽 바깥 (x=1.05, y=1)
        loc='upper left',           # 앵커 기준 위치
        borderaxespad=0.             # 축과 간격
    )

    # y 축 조정 (20억부터)
    plt.ylim(2000000000, None)   # None이면 최대값은 자동으로 맞춰짐

    # y축 보조선
    plt.grid(axis='y', linestyle='--', alpha=0.7) # alpha=투명도
    #plt.grid(axis='x', linestyle='--', alpha=0.7) # alpha=투명도

    #plt.xlabel("month")
    #plt.ylabel("매출")

    #plt.show()
    # 그래프 안잘리게
    plt.tight_layout()


    # 향후 노션업로드하기 위해 저장
    # #print(os.getcwd()) 이 곳에 저장되고, colab 환경이라 좌측 폴더모양 누르면 png 있음.
    # 세션 종료시 자동으로 삭제됨
    filePath1_monthlySales = "graph1_monthlySales.png"
    plt.savefig(filePath1_monthlySales, dpi=160) # dpi : 해상도
    plt.close()

    blob = bucket.blob(f'{gameidx}/{filePath1_monthlySales}')
    blob.upload_from_filename(filePath1_monthlySales)

    # 메모리에 올라간 이미지 파일 삭제
    os.remove(filePath1_monthlySales)

    return f'{gameidx}/{filePath1_monthlySales}'



# 1) 파일 경로
def merge_daily_graph(joyplegameid: int, gameidx: str):
    p1 = daily_revenue_graph_draw(joyplegameid, gameidx)
    p2 = daily_revenue_YOY_graph_draw(joyplegameid, gameidx)

    # 2) 이미지 열기 (투명 보존 위해 RGBA)
    blob1 = bucket.blob(p1)
    blob2 = bucket.blob(p2)

    im1 = blob1.download_as_bytes()
    im2 = blob2.download_as_bytes()

    im1 = Image.open(BytesIO(im1))
    im2 = Image.open(BytesIO(im2))

    # ---- [옵션 A] 원본 크기 유지 + 세로 패딩으로 높이 맞추기 (권장: 왜곡 없음) ----
    target_h = max(im1.height, im2.height)

    def pad_to_height(img, h, bg=(255, 255, 255, 0)):  # 투명 배경: 알파 0
        if img.height == h:
            return img
        canvas = Image.new("RGBA", (img.width, h), bg)
        # 가운데 정렬로 붙이기 (위에 맞추려면 y=0)
        y = (h - img.height) // 2
        canvas.paste(img, (0, y))
        return canvas

    im1_p = pad_to_height(im1, target_h)
    im2_p = pad_to_height(im2, target_h)

    gap = 0  # 이미지 사이 여백(px). 필요하면 20 등으로 변경
    bg = (255, 255, 255, 0)  # 전체 배경(투명). 흰색 원하면 (255,255,255,255)

    out = Image.new("RGBA", (im1_p.width + gap + im2_p.width, target_h), bg)
    out.paste(im1_p, (0, 0), im1_p)
    out.paste(im2_p, (im1_p.width + gap, 0), im2_p)

    # 3) GCS에 저장
    output_buffer = BytesIO()
    out.save(output_buffer, format='PNG')
    output_buffer.seek(0)

    # GCS 경로
    gcs_path = f'{gameidx}/graph1_dailySales_monthlySales.png'
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(output_buffer.getvalue(), content_type='image/png')

    return gcs_path


def daily_revenue_data_upload_to_notion(joyplegameid: int, gameidx: str, **context):

    PAGE_INFO=context['task_instance'].xcom_pull(
        task_ids = 'make_gameframework_notion_page',
        key='page_info'
    )
    query_result1_dailySales=context['task_instance'].xcom_pull(
        task_ids='daily_revenue_query',  # ← 첫 번째 Task의 task_id
        key='daily_revenue_df'
    )

    query_result1_monthlySales=context['task_instance'].xcom_pull(
        task_ids='Daily_revenue_YOY_query',  # ← 첫 번째 Task의 task_id
        key='Daily_revenue_YOY_df'
    )

    notion.blocks.children.append(
        PAGE_INFO["id"],
        children=[
            {
                "object": "block",
                "type": "heading_2",
                "heading_2": {
                    "rich_text": [{"type": "text", "text": {"content": "\n\n1. 일자별 매출" }}]
                },
            }
        ],
    )

    gcs_path = f'{gameidx}/graph1_dailySales_monthlySales.png'
    blob = bucket.blob(gcs_path)
    image_bytes = blob.download_as_bytes()
    filename = 'graph1_dailySales_monthlySales.png'

    ########### (2) 그래프 업로드
    # 일자별 매출
    # 그래프는 파일 저장후 올리는 구조밖에 되지않아서
    # 1) 업로드 객체 생성 (file_upload 생성)
    create_url = "https://api.notion.com/v1/file_uploads"
    payload = {
        "filename": filename,
        "content_type": "image/png"
    }
    headers_json = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json"
    }
    resp = requests.post(create_url, headers=headers_json, data=json.dumps(payload))
    resp.raise_for_status()
    file_upload = resp.json()
    file_upload_id = file_upload["id"]   # 업로드 ID
    upload_url = file_upload[upload_url]

    # 2) 이미지 업로드
    headers_upload = {
        "Content-Type": "image/png"
    }
    requests.put(upload_url, headers=headers_upload, data=image_bytes)

    # file_upload["upload_url"] 도 응답에 포함됨
    # 2) 파일 바이너리 전송 (multipart/form-data)
    send_url = f"https://api.notion.com/v1/file_uploads/{file_upload_id}/send"
    files = {"file": (filename, BytesIO(image_bytes), "image/png")}
    headers_send = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION
    }
    send_resp = requests.post(send_url, headers=headers_send, files=files)
    send_resp.raise_for_status()

    # 3) 이미지 블록으로 페이지에 첨부
    append_url = f"https://api.notion.com/v1/blocks/{file_upload_id}/children"
    append_payload = {
        "children": [
            {
                "object": "block",
                "type": "image",
                "image": {
                    "type": "file_upload",
                    "file_upload": {"id": file_upload_id},
                    # 캡션을 달고 싶다면 아래 주석 해제
                    # "caption": [{"type": "text", "text": {"content": "자동 업로드된 그래프"}}]
                }
            }
        ]
    }

    headers_json_patch = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json"
    }
    append_resp = requests.patch(append_url, headers=headers_json_patch, data=json.dumps(append_payload))
    append_resp.raise_for_status()

    resp = df_to_notion_table_under_toggle(
        notion=notion,
        page_id=PAGE_INFO["id"],
        df=query_result1_dailySales,
        toggle_title="📊 로데이터 - 일자별 매출",
        max_first_batch_rows=90,
        batch_size=100,
    )

    resp = df_to_notion_table_under_toggle(
        notion=notion,
        page_id=PAGE_INFO["id"],
        df=query_result1_monthlySales,
        toggle_title="📊 로데이터 - 전년 동월대비 매출",
        max_first_batch_rows=90,
        batch_size=100,
    )

    response1_salesComment = daily_revenue_gemini(joyplegameid=joyplegameid)

    ## 제미나이
    blocks = md_to_notion_blocks(response1_salesComment)
    notion.blocks.children.append(
        block_id=PAGE_INFO["id"],
        children=blocks
    )

    return True




# 2. 자체결제 매출
def inhouse_sales_query(joyplegameid: int, **context):
    query = f"""
    select logdatekst, cast(sum(pgpricekrw) as int64) as rev
    from
    (SELECT t1.*,
        t2.PGRole,
        t2.PlatformDeviceTypeName,
        t2.PGName,
        t2.PGBuyCount,
        t2.PGPriceKRW
    FROM `dataplatform-reporting.DataService.T_0317_0000_AuthAccountPerformance_V` AS t1,
    UNNEST(t1.PaymentDetailArrayStruct) AS t2
    where joyplegameid = {joyplegameid}
    and pgrole = '자체결제'
    and logdatekst>= DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
    and logdatekst<=LAST_DAY(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
    )
    group by 1 order by 1

    """
    query_result = query_run_method('2_inhouse_sales', query)

    context['task_instance'].xcom_push(key='inhouse_sales_df', value=query_result)

    return True

### 2> 24년부터 월별 자체결제 매출
def inhouse_sales_before24_query(joyplegameid: int, **context):
    query = f"""
    select a.month
    , cast(a.rev_all as int64) as rev_all
    , cast(b.rev as int64) as rev_self
    , safe_divide(b.rev,a.rev_all) as self_per
    from
    (select month, sum(pricekrw) as rev_all
    from
    (select *,format_date('%Y-%m', logdatekst ) as month
    FROM `dataplatform-reporting.DataService.T_0317_0000_AuthAccountPerformance_V`
    where joyplegameid = {joyplegameid}
    and logdatekst>='2024-01-01'
    and logdatekst<=DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY))
    group by 1
    ) as a

    left join
    (select month, cast(sum(pgpricekrw) as int64) as rev
    from
    (SELECT t1.*,
        t2.PGRole,
        t2.PlatformDeviceTypeName,
        t2.PGName,
        t2.PGBuyCount,
        t2.PGPriceKRW,
        format_date('%Y-%m', logdatekst ) as month
    FROM `dataplatform-reporting.DataService.T_0317_0000_AuthAccountPerformance_V` AS t1,
    UNNEST(t1.PaymentDetailArrayStruct) AS t2
    where joyplegameid = {joyplegameid}
    and pgrole = '자체결제'
    and logdatekst>='2024-01-01'
    and logdatekst<=DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY))
    group by 1) as b
    on a.month = b.month
    order by month
    """

    query_result = query_run_method('2_inhouse_sales', query)
    context['task_instance'].xcom_push(key='inhouse_sales_before24_df', value=query_result)

    return True

## 제미나이 프롬프트 
def inhouses_revenue_gemini(joyplegameid: int, **context):
    
    inhouse_sales = context['task_instance'].xcom_pull(
        task_ids='inhouse_sales_query',
        key='inhouse_sales_df'
    )
    inhouse_sales_before24 = context['task_instance'].xcom_pull(
        task_ids='inhouse_sales_before24_query',
        key='inhouse_sales_before24_df'
    )

    prompt_2 = f"""

    1. 아래는 일자별 자체결제 매출과 과거부터 장기적인 자체결제 매출이야.간단하게 해석해줘.
    2. 자체결제에 대한 정의를 쓸 필요는 없어.
    3. 일자별 자체결제 트렌드에 대해 설명해주고, 장기적인 자체결제 트렌드에 대해 간단히 설명해줘.
    4. 일자별 자체결제 트렌드와 장기적인 자체결제 트렌드 단락을 나눠줘. 예) <일자별 자체결제 트렌드> , <장기적 자체결제 트렌드>
    5. 내 질문을 그대로 쓰지마.
    6. 변수명에 대해선 언급하지마.
    7. 자체결제란, 구글이나 애플의 마켓 수수료를 절감하기 위해 수수료가 낮은 결제 플랫폼에서 결제하는 것을 말해.
    8. 다음은 자체결제에 대한 분석입니다 이런 사전에 말 하지말고 그냥 분석결과를 알려줘
    9. 사견을 쓰지말고 그냥 현재 상황에 대해 팩트만 알려줘.


    < 서식 요구사항 >
    한 문장마다 노션의 마크다운 리스트 문법을 사용해줘. e.g. * 당월 매출은 이렇습니다.

    <일자별 자체결제 매출>
    {inhouse_sales}

    < 장기적 자체결제 매출>
    {inhouse_sales_before24}

    """

    response2_selfPaymentSales = genai_client.models.generate_content(
        model=MODEL_NAME,
        contents=prompt_2,
        config=types.GenerateContentConfig(

            # 영어로 작성하는 것이 잘 이해할 수 있음.
            system_instruction=SYSTEM_INSTRUCTION,
            #tools=[rag_retrieval_tool_test],
            temperature=0.5,
            labels=labels
            # max_output_tokens=2048
        )
    )

    return response2_selfPaymentSales.text

## 한글깨짐 방지를 위해 폰트 지정
# font_path = "/usr/share/fonts/truetype/nanum/NanumGothic.ttf"
# if Path(font_path).exists():
#     fm.fontManager.addfont(font_path)       # 수동 등록
#     mpl.rc('font', family='NanumGothic')    # 기본 폰트 지정
#     mpl.rc('axes', unicode_minus=False)     # 마이너스 깨짐 방지
# else:
#     print("⚠️ NanumGothic 설치 실패. 다른 폰트를 써야 합니다.")

def inhouse_revenue_graph_draw(joyplegameid: int, gameidx: str, **context):

    query_result2_dailySelfPaymentSales = context['task_instance'].xcom_pull(
        task_ids='inhouse_sales_query',
        key='inhouse_sales_df'
    )
    
    # Seaborn 선 그래프
    sns.lineplot(
        x= query_result2_dailySelfPaymentSales.columns[0],
        y=query_result2_dailySelfPaymentSales.columns[1],
        data=query_result2_dailySelfPaymentSales,
        marker="o"
        )

    # 옵션
    plt.title("이번달 일자별 자체결제 매출")
    #plt.xlabel(query_result2_dailySelfPaymentSales.columns[0])   # 자동으로 컬럼명 표시 가능
    #plt.ylabel(query_result2_dailySelfPaymentSales.columns[1])

    # y축 천 단위 구분 기호 넣기
    plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{int(x):,}"))

    # x축 눈금을 7개 단위로만 표시 (예: 1주일 간격)
    plt.xticks(query_result2_dailySelfPaymentSales[query_result2_dailySelfPaymentSales.columns[0]][::1], rotation=45)

    # y축 0부터 시작
    plt.ylim(0, None)   # None이면 최대값은 자동으로 맞춰짐
    # y축 보조선
    plt.grid(axis='y', linestyle='--', alpha=0.7) # alpha=투명도

    # x,y축 제거
    plt.xlabel(None)
    plt.ylabel(None)

    #plt.show()
    # 그래프 안잘리게
    plt.tight_layout()


    # 향후 노션업로드하기 위해 저장
    # #print(os.getcwd()) 이 곳에 저장되고, colab 환경이라 좌측 폴더모양 누르면 png 있음.
    # 세션 종료시 자동으로 삭제됨

    ####################################### 이미지 파일을 저장할 path가 필요함 #####################
    filepath1_inhouseSales = "graph1_dailySelfPaymentSales.png"
    plt.savefig(filepath1_inhouseSales, dpi=160) # dpi : 해상도
    plt.close()

    blob = bucket.blob(f'{gameidx}/{filepath1_inhouseSales}')
    blob.upload_from_filename(filepath1_inhouseSales)

    # 메모리에 올라간 이미지 파일 삭제
    os.remove(filepath1_inhouseSales)

    return f'{gameidx}/{filepath1_inhouseSales}'



## 한글깨짐 방지를 위해 폰트 지정
# font_path = "/usr/share/fonts/truetype/nanum/NanumGothic.ttf"
# if Path(font_path).exists():
#     fm.fontManager.addfont(font_path)       # 수동 등록
#     mpl.rc('font', family='NanumGothic')    # 기본 폰트 지정
#     mpl.rc('axes', unicode_minus=False)     # 마이너스 깨짐 방지
# else:
#     print("⚠️ NanumGothic 설치 실패. 다른 폰트를 써야 합니다.")

def inhouse_revenue_monthly_graph_draw(joyplegameid: int, gameidx: str, **context):
    
    query_result2_monthlySelfPaymentSales = context['task_instance'].xcom_pull(
        task_ids='inhouse_sales_before24_query',
        key='inhouse_sales_before24_df'
    )

    # Figure & Axes 생성
    fig, ax1 = plt.subplots(figsize=(10,5))

    # 옵션
    plt.title("월별 자체결제 매출 & 자체결제 매출 비중 (24년1월~) ")

    # 첫 번째 y축 (왼쪽, 막대그래프)
    ax1.bar(query_result2_monthlySelfPaymentSales["month"],
            query_result2_monthlySelfPaymentSales["rev_self"],
            color="#5B9BD5",
            #label="Sales"
            )
    #ax1.set_ylabel("Sales", color="black")
    ax1.tick_params(axis="y", labelcolor="black")

    # 두 번째 y축 (오른쪽, 선그래프)
    ax2 = ax1.twinx()
    ax2.plot(query_result2_monthlySelfPaymentSales["month"],
            query_result2_monthlySelfPaymentSales["self_per"],
            color="#ED7D31",
            marker="o",
            #label="Users"
            )
    #ax2.set_ylabel("Users", color="black")
    ax2.tick_params(axis="y", labelcolor="black")

    # 👉 선 위에 데이터 레이블 표시
    for x, y in zip(query_result2_monthlySelfPaymentSales["month"],
                    query_result2_monthlySelfPaymentSales["self_per"]):
        ax2.annotate(f"{y:.0%}",  # 0.23 → "23%"
                    xy=(x, y),
                    xytext=(0, 5),  # 살짝 위로
                    textcoords="offset points",
                    ha="center", color="black"
                    )


    # y축 천 단위 구분 기호 넣기
    ax1.yaxis.set_major_formatter(StrMethodFormatter('{x:,.0f}'))

    # 퍼센트 포맷 자동 적용 (self_per가 0~1이면 1.0, 0~100이면 100)
    maxv = float(query_result2_monthlySelfPaymentSales["self_per"].max())
    ax2.yaxis.set_major_formatter(PercentFormatter(1.0 if maxv <= 1.5 else 100))

    # x축 눈금(값) 세로 회전 — 축 객체에 직접 적용
    for tick in ax1.get_xticklabels():
        tick.set_rotation(90)
        tick.set_ha("center")  # 또는 "right"로 바꿔도 됨

    # 제목 & 격자
    ax1.grid(axis="y", linestyle="--", alpha=0.7)

    plt.tight_layout()

    # 향후 노션업로드하기 위해 저장
    # #print(os.getcwd()) 이 곳에 저장되고, colab 환경이라 좌측 폴더모양 누르면 png 있음.
    # 세션 종료시 자동으로 삭제됨
    filepath1_inhouseMonthlySales = "graph1_monthlySelfPaymentSales.png"
    plt.savefig(filepath1_inhouseMonthlySales, dpi=160) # dpi : 해상도
    plt.close()

    blob = bucket.blob(f'{gameidx}/{filepath1_inhouseMonthlySales}')
    blob.upload_from_filename(filepath1_inhouseMonthlySales)

    # 메모리에 올라간 이미지 파일 삭제
    os.remove(filepath1_inhouseMonthlySales)

    return f'{gameidx}/{filepath1_inhouseMonthlySales}'


## 그래프 합치기
### 자체결제 일자별 + 자체결제 월별

### R그룹별 매출그래프와 PU 그래프 합치기

def merge_inhouse_graph(joyplegameid: int, gameidx: str):
    # 1) 파일 경로
    p1 = inhouse_revenue_graph_draw(joyplegameid, gameidx)
    p2 = inhouse_revenue_monthly_graph_draw(joyplegameid, gameidx)

    # 2) 이미지 열기 (투명 보존 위해 RGBA)
    blob1 = bucket.blob(p1)
    blob2 = bucket.blob(p2)

    im1 = blob1.download_as_bytes()
    im2 = blob2.download_as_bytes()

    im1 = Image.open(BytesIO(im1))
    im2 = Image.open(BytesIO(im2))

    # ---- [옵션 A] 원본 크기 유지 + 세로 패딩으로 높이 맞추기 (권장: 왜곡 없음) ----
    target_h = max(im1.height, im2.height)

    def pad_to_height(img, h, bg=(255, 255, 255, 0)):  # 투명 배경: 알파 0
        if img.height == h:
            return img
        canvas = Image.new("RGBA", (img.width, h), bg)
        # 가운데 정렬로 붙이기 (위에 맞추려면 y=0)
        y = (h - img.height) // 2
        canvas.paste(img, (0, y))
        return canvas

    im1_p = pad_to_height(im1, target_h)
    im2_p = pad_to_height(im2, target_h)

    gap = 0  # 이미지 사이 여백(px). 필요하면 20 등으로 변경
    bg = (255, 255, 255, 0)  # 전체 배경(투명). 흰색 원하면 (255,255,255,255)

    out = Image.new("RGBA", (im1_p.width + gap + im2_p.width, target_h), bg)
    out.paste(im1_p, (0, 0), im1_p)
    out.paste(im2_p, (im1_p.width + gap, 0), im2_p)

    # 3) GCS에 저장
    output_buffer = BytesIO()
    out.save(output_buffer, format='PNG')
    output_buffer.seek(0)

    # GCS 경로
    gcs_path = f'{gameidx}/graph2_selfPaymentSales.png'
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(output_buffer.getvalue(), content_type='image/png')

    return gcs_path



def inhouse_revenue_data_upload_to_notion(joyplegameid: int, gameidx: str, **context):

    PAGE_INFO=context['task_instance'].xcom_pull(
        task_ids = 'make_gameframework_notion_page',
        key='page_info'
    )
    query_result1_inhouseSales = context['task_instance'].xcom_pull(
        task_ids = 'inhouse_sales_query',
        key='inhouse_sales_df'
    )
    query_result1_inhouseMonthlySales = context['task_instance'].xcom_pull(
        task_ids='inhouse_sales_before24_query',
        key='inhouse_sales_before24_df'
    )
    
    ########### (1) 제목
    notion.blocks.children.append(
        PAGE_INFO['id'],
        children=[
            {
                "object": "block",
                "type": "heading_2",
                "heading_2": {
                    "rich_text": [{"type": "text", "text": {"content": "2. 자체결제 매출" }}]
                },
            }
        ],
    )

    gcs_path = f'{gameidx}/graph2_selfPaymentSales.png'
    blob = bucket.blob(gcs_path)
    image_bytes = blob.download_as_bytes()
    filename = 'graph2_selfPaymentSales.png'

    ########### (2) 그래프 업로드
    create_url = "https://api.notion.com/v1/file_uploads"
    payload = {
        "filename": filename,
        "content_type": "image/png"
    }
    headers_json = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json"
    }
    resp = requests.post(create_url, headers=headers_json, data=json.dumps(payload))
    resp.raise_for_status()
    file_upload = resp.json()
    file_upload_id = file_upload["id"]   # 업로드 ID
    upload_url = file_upload[upload_url]

    # 2) 이미지 업로드
    headers_upload = {
        "Content-Type": "image/png"
    }
    requests.put(upload_url, headers=headers_upload, data=image_bytes)

    send_url = f"https://api.notion.com/v1/file_uploads/{file_upload_id}/send"
    files = {"file": (filename, BytesIO(image_bytes), "image/png")}
    headers_send = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION
    }
    send_resp = requests.post(send_url, headers=headers_send, files=files)
    send_resp.raise_for_status()

    # 3) 이미지 블록으로 페이지에 첨부
    append_url = f"https://api.notion.com/v1/blocks/{file_upload_id}/children"
    append_payload = {
        "children": [
            {
                "object": "block",
                "type": "image",
                "image": {
                    "type": "file_upload",
                    "file_upload": {"id": file_upload_id},
                    # 캡션을 달고 싶다면 아래 주석 해제
                    # "caption": [{"type": "text", "text": {"content": "자동 업로드된 그래프"}}]
                }
            }
        ]
    }

    headers_json_patch = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json"
    }
    append_resp = requests.patch(append_url, headers=headers_json_patch, data=json.dumps(append_payload))
    append_resp.raise_for_status()

    ## (3) 로데이터
    resp = df_to_notion_table_under_toggle(
        notion=notion,
        page_id=PAGE_INFO['id'],
        df=query_result1_inhouseSales,
        toggle_title="📊 로데이터 - 일자별 자체결제 매출 ",
        max_first_batch_rows=90,
        batch_size=100,
    )

    resp = df_to_notion_table_under_toggle(
        notion=notion,
        page_id=PAGE_INFO['id'],
        df=query_result1_inhouseMonthlySales,
        toggle_title="📊 로데이터 - 장기 자체결제 매출 ",
        max_first_batch_rows=90,
        batch_size=100,
    )

    ## (4) 제미나이 해석
    gemini_text = inhouses_revenue_gemini(joyplegameid)
    blocks = md_to_notion_blocks(gemini_text)

    notion.blocks.children.append(
        block_id=PAGE_INFO['id'],
        children=blocks
    )

    return True




## 이번달 가입 유저의 국가별 매출
def cohort_by_country_revenue(joyplegameid: int, **context):
    query = f"""
    with countryRev as (
    select country2 as country, rev_rank2 as rev_rank, sum(rev) as rev
    from
    (select country, rev
    , case when rev_rank <= 9 then country  when rev_rank > 9 then 'etc' end as country2 # rev 기준 10위부터는 etc 로 표기
    , case when rev_rank <= 9 then rev_rank when rev_rank > 9 then 10 end as rev_rank2
    from
    (select country, rev, row_number() OVER (ORDER BY rev desc ) AS rev_rank # rev 순서대로 랭크
    from
    (select countrycode as country, cast(sum(pricekrw) as int64) as rev
    from `dataplatform-reporting.DataService.V_0317_0000_AuthAccountPerformance_V`
    where joyplegameid = {joyplegameid}
    and logdatekst>= DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
    and logdatekst<=LAST_DAY(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
    and authaccountregdatekst>= DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
    and authaccountregdatekst<=LAST_DAY(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
    group by 1)
    )
    )
    group by 1,2
    order by rev_rank # etc 국가가 맨 뒤로 가야함
    ),

    allRev as  (
    select cast(sum(pricekrw) as int64) as rev
    from `dataplatform-reporting.DataService.V_0317_0000_AuthAccountPerformance_V`
    where joyplegameid = {joyplegameid}
    and logdatekst>= DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
    and logdatekst<=LAST_DAY(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
    and authaccountregdatekst>= DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
    and authaccountregdatekst<=LAST_DAY(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
    )

    select a.*, safe_divide(a.rev,b.rev) as rev_percent
    from
    (select * from countryRev ) as a
    cross join
    (select * from allRev) as b
    order by rev_rank

    """
    query_result=query_run_method('3_global_ua', query)

    context['task_instance'].xcom_push(key='cohort_by_country_revenue_df', value=query_result)
    
    return True

## 이번달 국가별 COST
def cohort_by_country_cost(joyplegameid: int, **context):
    query = f"""
    with countryCost as (
    select country2 as country, cost_rank2 as cost_rank, sum(cost) as cost
    from
    (select country, cost
    , case when cost_rank <= 9 then country when cost_rank > 9 then 'etc' end as country2 # cost 순서대로 10위 부터는 etc 로
    , case when cost_rank <= 9 then cost_rank else 10 end as cost_rank2
    from
    (select country, cost, row_number() OVER (ORDER BY cost desc ) AS cost_rank # cost 순서로 rank
    from
    (select countrycode as country, cast(sum(cost) as int64) cost
    from `dataplatform-reporting.DataService.V_0410_0000_CostCampaignRule_V`
    where joyplegameid = {joyplegameid}
    and cmpgndate >= DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
    and cmpgndate <=LAST_DAY(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
    group by countrycode))
    )
    group by 1,2
    order by cost_rank # etc 국가가 맨 뒤로 가야함
    ),

    allCost as (
    select cast(sum(cost) as int64) cost
    from `dataplatform-reporting.DataService.V_0410_0000_CostCampaignRule_V`
    where joyplegameid = {joyplegameid}
    and cmpgndate >= DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
    and cmpgndate <=LAST_DAY(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
    )

    select a.*, safe_divide(a.cost,b.cost) as cost_percent
    from
    (select * from countryCost) as a
    cross join
    (select * from allCost) as b
    order by cost_rank


    """
    query_result =query_run_method('3_global_ua', query)
    context['task_instance'].xcom_push(key='cohort_by_country_cost_df', value=query_result)
    
    return True


## 국가별 rev, cost 프롬프트
### 4> 일자별 매출에 대한 제미나이 코멘트
def cohort_by_gemini(joyplegameid: int, **context):
    
    cohort_country_revenue = context['task_instance'].xcom_pull(
        task_ids='cohort_by_country_revenue',
        key='cohort_by_country_revenue_df'
    )
    cohort_country_cost = context['task_instance'].xcom_pull(
        task_ids='cohort_by_country_cost',
        key='cohort_by_country_cost_df'
    )

    #client = genai.Client(api_key="AIzaSyAVv2B6DM6w9jd1MxiP3PbzAEMkl97SCGY")
    response3_revAndCostByCountry = genai_client.models.generate_content(
    model=MODEL_NAME,

    contents = f"""
    이번달에 어떤 국가에 마케팅했고, 어떤 국가에서 신규유저의 매출이 나왔는지에 대한 데이터를 줄게.
    간단하게 현황 요약해줘.

    당월 마케팅비용은 얼마이며 당월 신규유저 매출은 얼마입니다를 먼저 한줄로 서두에 언급해줘.
    이번달 COST많이 쓴 국가들 각각 COST 비중이 몇% 인지 한줄에 써주고,
    신규유저 매출 높은 국가들 각각 몇% 매출 비중인지 한줄에 써줘 알려줘.
    그리고 주요 국가들에 대해서 COST 비중과 매출비중을 비교해서 특이한점이 있는것만 알려줘.

    매출과 COST 의 액수 절댓값을 비교하지 말고 비중을 비교해줘.
    etc 는 기타 국가들 총 합 한 값이라서 etc 에 대해서는 언급하지 말아줘.
    마케팅 효율개선이 필요하다는말은 하지말아줘.

    <원하는 서식>
    1. 요약해주겠다 말 하지말고 요약한 내용에 대해서만 적어주면 돼.
    2. 습니다. 체로 써줘
    3. 한 문장마다 노션의 마크다운 리스트 문법을 사용해줘. e.g. * 당월 매출은 이렇습니다.

    <데이터 설명>
    매출이랑 마케팅 비용이랑 가장 많이 사용된 9개 국가와 그 이후 10번째 국가부터는 전부 etc 국가로 처리했어.
    etc 는 국가가 아니라 나머지 국가 총합이야.

    <이번달 가입유저의 국가별 매출>
    {cohort_country_revenue}

    <이번달 국가별 마케팅 비용>
    {cohort_country_cost}

    """,
    config=types.GenerateContentConfig(
            system_instruction=SYSTEM_INSTRUCTION,
            # tools=[RAG],
            temperature=0.5,
            labels=labels
        )

    )
    # 코멘트 출력
    return response3_revAndCostByCountry.text


# 코멘트 정리 ( 향후 요약에 사용하기 용도 )
#gemini_result.loc[len(gemini_result)] = response.text

## OS별 cost
def os_cost(joyplegameid: int, **context):
    query = f"""
    with osCost as (
    select os, cast(sum(cost) as int64) cost
    from `dataplatform-reporting.DataService.V_0410_0000_CostCampaignRule_V`
    where joyplegameid = {joyplegameid}
    and cmpgndate >= DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
    and cmpgndate <=LAST_DAY(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
    group by os
    ),

    allCost as (
    select cast(sum(cost) as int64) cost
    from `dataplatform-reporting.DataService.V_0410_0000_CostCampaignRule_V`
    where joyplegameid = {joyplegameid}
    and cmpgndate >= DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
    and cmpgndate <=LAST_DAY(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
    )

    select a.*, safe_divide(a.cost,b.cost) as cost_percent
    from
    (select * from osCost) as a
    cross join
    (select * from allCost) as b
    """

    query_result =query_run_method('3_global_ua', query)
    context['task_instance'].xcom_push(key='os_cost_df', value=query_result)

    return True

## OS별 매출
def os_rev(joyplegameid: int, **context):
    query = f"""
    with osRev as (

    select os, rev from (
    select os, cast(sum(pricekrw) as int64) as rev
    from `dataplatform-reporting.DataService.V_0317_0000_AuthAccountPerformance_V`
    where joyplegameid = {joyplegameid}
    and logdatekst>= DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
    and logdatekst<=LAST_DAY(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
    and authaccountregdatekst>= DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
    and authaccountregdatekst<=LAST_DAY(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
    group by 1)
    where rev>0
    ),

    allRev as (
    select cast(sum(pricekrw) as int64) as rev
    from `dataplatform-reporting.DataService.V_0317_0000_AuthAccountPerformance_V`
    where joyplegameid = {joyplegameid}
    and logdatekst>= DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
    and logdatekst<=LAST_DAY(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
    and authaccountregdatekst>= DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
    and authaccountregdatekst<=LAST_DAY(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
    )

    select a.*, safe_divide(a.rev,b.rev) as rev_percent
    from
    (select * from osRev) as a
    cross join
    (select * from allRev) as b
    """
    ## 129.93MB
    query_result =query_run_method('3_global_ua', query)
    context['task_instance'].xcom_push(key='os_rev_df', value=query_result)

    return True


### 4> 일자별 매출에 대한 제미나이 코멘트

#client = genai.Client(api_key="AIzaSyAVv2B6DM6w9jd1MxiP3PbzAEMkl97SCGY")
def os_by_gemini(joyplegameid: int, **context):
    
    os_rev_df= context['task_instance'].xcom_pull(
        task_ids='os_cost',
        key='os_cost_df'
    )
    os_cost_df= context['task_instance'].xcom_pull(
        task_ids='os_rev',
        key='os_rev_df'
    )

    response3_revAndCostByOs = genai_client.models.generate_content(
    model=MODEL_NAME,

    contents = f"""

    이번달에 IOS 에 몇 % 마케팅 비용 사용했으며 IOS 의 매출비중은 몇% 입니다.
    의 형식으로 알려줘.


    <원하는 서식>
    1. 요약해주겠다 말 하지말고 요약한 내용에 대해서만 적어주면 돼.
    2. 습니다. 체로 써줘
    3. 한 문장마다 노션의 마크다운 리스트 문법을 사용해줘. e.g. * 당월 매출은 이렇습니다.


    <데이터 설명>


    <이번달 가입유저의 OS별 매출>
    {os_rev_df}

    <이번달 OS별 마케팅 비용>
    {os_cost_df}
    """,
    config=types.GenerateContentConfig(
            system_instruction=SYSTEM_INSTRUCTION,
            # tools=[RAG],
            temperature=0.5,
            labels=labels
        )

    )
    # 코멘트 출력
    return response3_revAndCostByOs.text

# 코멘트 정리 ( 향후 요약에 사용하기 용도 )
#gemini_result.loc[len(gemini_result)] = response.text

### 그래프 그리기
## 국가별 매출

def by_country_revenue_graph_draw(joyplegameid: int, gameidx: str, **context):
    
    query_result3_revByCountry = context['task_instance'].xcom_pull(
        task_ids = 'cohort_by_country_revenue',
        key='cohort_by_country_revenue_df'
    )

    sizes = query_result3_revByCountry["rev"].to_numpy()
    labels = query_result3_revByCountry["country"].to_numpy()
    total  = sizes.sum()

    fig, ax = plt.subplots(figsize=(5,5))
    wedges, _ = ax.pie(sizes, labels=None, startangle=90)

    # 각 웨지의 중앙각(도), 내부/외부 좌표 계산
    angles = [(p.theta1 + p.theta2)/2 for p in wedges]
    inside_r, outside_r = 0.6, 1.28

    # 1) 라벨을 "이름 (x.x%)" 형식으로 우선 내부에 배치
    texts = []
    for ang, size, name in zip(angles, sizes, labels):
        percent = size / total * 100
        txt = f"{name} ({percent:.1f}%)"
        x_in = np.cos(np.deg2rad(ang)) * inside_r
        y_in = np.sin(np.deg2rad(ang)) * inside_r
        t = ax.text(x_in, y_in, txt, ha='center', va='center', fontsize=9, color="black")
        texts.append(t)

    # 2) 겹침 감지 함수 (디스플레이 좌표에서 bbox 겹침 확인)
    def any_overlaps(texts, renderer):
        bboxes = [t.get_window_extent(renderer=renderer).expanded(1.05, 1.2) for t in texts]
        overlaps = set()
        for i in range(len(bboxes)):
            for j in range(i+1, len(bboxes)):
                if bboxes[i].overlaps(bboxes[j]):
                    overlaps.add(i); overlaps.add(j)
        return overlaps

    # 3) 겹치는 것만 외부로 재배치 + 화살표 연결 (작은 파이일수록 우선 이동)
    fig.canvas.draw()  # 렌더러 준비
    over_idx = any_overlaps(texts, fig.canvas.get_renderer())

    # 겹치는 텍스트 중, 웨지 면적(=sizes) 작은 것부터 바깥으로
    idx_sorted = sorted(list(over_idx), key=lambda i: sizes[i])
    for i in idx_sorted:
        ang = angles[i]
        # 원 밖 라벨 위치
        x_out = np.cos(np.deg2rad(ang)) * outside_r
        y_out = np.sin(np.deg2rad(ang)) * outside_r
        # 원 경계 쪽(화살표 기준점)
        x_edge = np.cos(np.deg2rad(ang)) * 1.0
        y_edge = np.sin(np.deg2rad(ang)) * 1.0

        # 기존 내부 텍스트 숨기고(또는 제거) 바깥에 새로 배치
        txt_str = texts[i].get_text()
        texts[i].set_visible(False)

        # 좌우 정렬은 반대쪽으로 맞추면 보기 좋음
        ha = 'left' if x_out >= 0 else 'right'
        ax.annotate(
            txt_str,
            xy=(x_edge, y_edge), xycoords='data',           # 화살표 도착점(파이 경계)
            xytext=(x_out, y_out), textcoords='data',       # 텍스트 위치(원 밖)
            ha=ha, va='center', fontsize=9, color='black',
            arrowprops=dict(arrowstyle='-', color='gray', shrinkA=0, shrinkB=0)
        )
    ax.set_title("국가별 매출 비중", pad=24)
    #plt.title("국가별 매출 비중")
    plt.tight_layout()

    filepath1_dailySales = "graph3_revByCountry.png"
    plt.savefig(filepath1_dailySales, dpi=160) # dpi : 해상도
    plt.close()

    blob = bucket.blob(f'{gameidx}/{filepath1_dailySales}')
    blob.upload_from_filename(filepath1_dailySales)

    # 메모리에 올라간 이미지 파일 삭제
    os.remove(filepath1_dailySales)

    return f'{gameidx}/{filepath1_dailySales}'



def by_country_cost_graph_draw(joyplegameid: int, gameidx: str, **context):
    
    query_result3_costByCountry = context['task_instance'].xcom_pull(
        task_ids = 'cohort_by_country_cost',
        key='cohort_by_country_cost_df'
    )

    ### 국가별 Cost
    sizes = query_result3_costByCountry["cost"].to_numpy()
    labels = query_result3_costByCountry["country"].to_numpy()
    total  = sizes.sum()


    fig, ax = plt.subplots(figsize=(5,5))
    wedges, _ = ax.pie(sizes, labels=None, startangle=90)

    # 각 웨지의 중앙각(도), 내부/외부 좌표 계산
    angles = [(p.theta1 + p.theta2)/2 for p in wedges]
    inside_r, outside_r = 0.6, 1.28

    # 1) 라벨을 "이름 (x.x%)" 형식으로 우선 내부에 배치
    texts = []
    for ang, size, name in zip(angles, sizes, labels):
        percent = size / total * 100
        txt = f"{name} ({percent:.1f}%)"
        x_in = np.cos(np.deg2rad(ang)) * inside_r
        y_in = np.sin(np.deg2rad(ang)) * inside_r
        t = ax.text(x_in, y_in, txt, ha='center', va='center', fontsize=9, color="black")
        texts.append(t)

    # 2) 겹침 감지 함수 (디스플레이 좌표에서 bbox 겹침 확인)
    def any_overlaps(texts, renderer):
        bboxes = [t.get_window_extent(renderer=renderer).expanded(1.05, 1.2) for t in texts]
        overlaps = set()
        for i in range(len(bboxes)):
            for j in range(i+1, len(bboxes)):
                if bboxes[i].overlaps(bboxes[j]):
                    overlaps.add(i); overlaps.add(j)
        return overlaps

    # 3) 겹치는 것만 외부로 재배치 + 화살표 연결 (작은 파이일수록 우선 이동)
    fig.canvas.draw()  # 렌더러 준비
    over_idx = any_overlaps(texts, fig.canvas.get_renderer())

    # 겹치는 텍스트 중, 웨지 면적(=sizes) 작은 것부터 바깥으로
    idx_sorted = sorted(list(over_idx), key=lambda i: sizes[i])
    for i in idx_sorted:
        ang = angles[i]
        # 원 밖 라벨 위치
        x_out = np.cos(np.deg2rad(ang)) * outside_r
        y_out = np.sin(np.deg2rad(ang)) * outside_r
        # 원 경계 쪽(화살표 기준점)
        x_edge = np.cos(np.deg2rad(ang)) * 1.0
        y_edge = np.sin(np.deg2rad(ang)) * 1.0

        # 기존 내부 텍스트 숨기고(또는 제거) 바깥에 새로 배치
        txt_str = texts[i].get_text()
        texts[i].set_visible(False)

        # 좌우 정렬은 반대쪽으로 맞추면 보기 좋음
        ha = 'left' if x_out >= 0 else 'right'
        ax.annotate(
            txt_str,
            xy=(x_edge, y_edge), xycoords='data',           # 화살표 도착점(파이 경계)
            xytext=(x_out, y_out), textcoords='data',       # 텍스트 위치(원 밖)
            ha=ha, va='center', fontsize=9, color='black',
            arrowprops=dict(arrowstyle='-', color='gray', shrinkA=0, shrinkB=0)
        )
    ax.set_title("국가별 COST 비중", pad=24)
    plt.tight_layout()


    filepath1_dailySales = "graph3_costByCountry.png"
    plt.savefig(filepath1_dailySales, dpi=160) # dpi : 해상도
    plt.close()

    blob = bucket.blob(f'{gameidx}/{filepath1_dailySales}')
    blob.upload_from_filename(filepath1_dailySales)

    # 메모리에 올라간 이미지 파일 삭제
    os.remove(filepath1_dailySales)

    return f'{gameidx}/{filepath1_dailySales}'


def merge_contry_graph(joyplegameid: int, gameidx: str):
    p1=by_country_revenue_graph_draw(joyplegameid, gameidx)
    p2=by_country_cost_graph_draw(joyplegameid, gameidx)

    # 2) 이미지 열기 (투명 보존 위해 RGBA)
    blob1 = bucket.blob(p1)
    blob2 = bucket.blob(p2)

    im1 = blob1.download_as_bytes()
    im2 = blob2.download_as_bytes()

    im1 = Image.open(BytesIO(im1))
    im2 = Image.open(BytesIO(im2))

    # ---- [옵션 A] 원본 크기 유지 + 세로 패딩으로 높이 맞추기 (권장: 왜곡 없음) ----
    target_h = max(im1.height, im2.height)

    def pad_to_height(img, h, bg=(255, 255, 255, 0)):  # 투명 배경: 알파 0
        if img.height == h:
            return img
        canvas = Image.new("RGBA", (img.width, h), bg)
        # 가운데 정렬로 붙이기 (위에 맞추려면 y=0)
        y = (h - img.height) // 2
        canvas.paste(img, (0, y))
        return canvas

    im1_p = pad_to_height(im1, target_h)
    im2_p = pad_to_height(im2, target_h)

    gap = 0  # 이미지 사이 여백(px). 필요하면 20 등으로 변경
    bg = (255, 255, 255, 0)  # 전체 배경(투명). 흰색 원하면 (255,255,255,255)

    out = Image.new("RGBA", (im1_p.width + gap + im2_p.width, target_h), bg)
    out.paste(im1_p, (0, 0), im1_p)
    out.paste(im2_p, (im1_p.width + gap, 0), im2_p)

    # 3) GCS에 저장
    output_buffer = BytesIO()
    out.save(output_buffer, format='PNG')
    output_buffer.seek(0)

    # GCS 경로
    gcs_path = f'{gameidx}/graph3_revAndCostByCountry.png'
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(output_buffer.getvalue(), content_type='image/png')

    return gcs_path

### OS 별 매출
def os_rev_graph_draw(joyplegameid: int, gameidx: str, **context):

    query_result3_revByOs = context['task_instance'].xcom_pull(
        task_ids='os_rev',
        key='os_rev_df'
    )

    sizes = query_result3_revByOs["rev"].to_numpy()
    labels = query_result3_revByOs["os"].to_numpy()
    total  = sizes.sum()


    fig, ax = plt.subplots(figsize=(5,5))
    wedges, _ = ax.pie(sizes, labels=None, startangle=90)

    # 각 웨지의 중앙각(도), 내부/외부 좌표 계산
    angles = [(p.theta1 + p.theta2)/2 for p in wedges]
    inside_r, outside_r = 0.6, 1.28

    # 1) 라벨을 "이름 (x.x%)" 형식으로 우선 내부에 배치
    texts = []
    for ang, size, name in zip(angles, sizes, labels):
        percent = size / total * 100
        txt = f"{name} ({percent:.1f}%)"
        x_in = np.cos(np.deg2rad(ang)) * inside_r
        y_in = np.sin(np.deg2rad(ang)) * inside_r
        t = ax.text(x_in, y_in, txt, ha='center', va='center', fontsize=9, color="black")
        texts.append(t)

    # 2) 겹침 감지 함수 (디스플레이 좌표에서 bbox 겹침 확인)
    def any_overlaps(texts, renderer):
        bboxes = [t.get_window_extent(renderer=renderer).expanded(1.05, 1.2) for t in texts]
        overlaps = set()
        for i in range(len(bboxes)):
            for j in range(i+1, len(bboxes)):
                if bboxes[i].overlaps(bboxes[j]):
                    overlaps.add(i); overlaps.add(j)
        return overlaps

    # 3) 겹치는 것만 외부로 재배치 + 화살표 연결 (작은 파이일수록 우선 이동)
    fig.canvas.draw()  # 렌더러 준비
    over_idx = any_overlaps(texts, fig.canvas.get_renderer())

    # 겹치는 텍스트 중, 웨지 면적(=sizes) 작은 것부터 바깥으로
    idx_sorted = sorted(list(over_idx), key=lambda i: sizes[i])
    for i in idx_sorted:
        ang = angles[i]
        # 원 밖 라벨 위치
        x_out = np.cos(np.deg2rad(ang)) * outside_r
        y_out = np.sin(np.deg2rad(ang)) * outside_r
        # 원 경계 쪽(화살표 기준점)
        x_edge = np.cos(np.deg2rad(ang)) * 1.0
        y_edge = np.sin(np.deg2rad(ang)) * 1.0

        # 기존 내부 텍스트 숨기고(또는 제거) 바깥에 새로 배치
        txt_str = texts[i].get_text()
        texts[i].set_visible(False)

        # 좌우 정렬은 반대쪽으로 맞추면 보기 좋음
        ha = 'left' if x_out >= 0 else 'right'
        ax.annotate(
            txt_str,
            xy=(x_edge, y_edge), xycoords='data',           # 화살표 도착점(파이 경계)
            xytext=(x_out, y_out), textcoords='data',       # 텍스트 위치(원 밖)
            ha=ha, va='center', fontsize=9, color='black',
            arrowprops=dict(arrowstyle='-', color='gray', shrinkA=0, shrinkB=0)
        )

    ax.set_title("OS별 매출 비중", pad=24)
    plt.tight_layout()

    filepath1_dailySales = "graph3_revByOs.png"
    plt.savefig(filepath1_dailySales, dpi=160) # dpi : 해상도
    plt.close()

    blob = bucket.blob(f'{gameidx}/{filepath1_dailySales}')
    blob.upload_from_filename(filepath1_dailySales)

    # 메모리에 올라간 이미지 파일 삭제
    os.remove(filepath1_dailySales)

    return f'{gameidx}/{filepath1_dailySales}'
    

### os 별 Cost
def os_cost_graph_draw(joyplegameid: int, gameidx: str, **context):

    query_result3_costByOs = context['task_instance'].xcom_pull(
        task_ids='os_cost',
        key='os_cost_df'
    )

    sizes = query_result3_costByOs["cost"].to_numpy()
    labels = query_result3_costByOs["os"].to_numpy()
    total  = sizes.sum()
    
    fig, ax = plt.subplots(figsize=(5,5))
    wedges, _ = ax.pie(sizes, labels=None, startangle=90)

    # 각 웨지의 중앙각(도), 내부/외부 좌표 계산
    angles = [(p.theta1 + p.theta2)/2 for p in wedges]
    inside_r, outside_r = 0.6, 1.28

    # 1) 라벨을 "이름 (x.x%)" 형식으로 우선 내부에 배치
    texts = []
    for ang, size, name in zip(angles, sizes, labels):
        percent = size / total * 100
        txt = f"{name} ({percent:.1f}%)"
        x_in = np.cos(np.deg2rad(ang)) * inside_r
        y_in = np.sin(np.deg2rad(ang)) * inside_r
        t = ax.text(x_in, y_in, txt, ha='center', va='center', fontsize=9, color="black")
        texts.append(t)

    # 2) 겹침 감지 함수 (디스플레이 좌표에서 bbox 겹침 확인)
    def any_overlaps(texts, renderer):
        bboxes = [t.get_window_extent(renderer=renderer).expanded(1.05, 1.2) for t in texts]
        overlaps = set()
        for i in range(len(bboxes)):
            for j in range(i+1, len(bboxes)):
                if bboxes[i].overlaps(bboxes[j]):
                    overlaps.add(i); overlaps.add(j)
        return overlaps

    # 3) 겹치는 것만 외부로 재배치 + 화살표 연결 (작은 파이일수록 우선 이동)
    fig.canvas.draw()  # 렌더러 준비
    over_idx = any_overlaps(texts, fig.canvas.get_renderer())

    # 겹치는 텍스트 중, 웨지 면적(=sizes) 작은 것부터 바깥으로
    idx_sorted = sorted(list(over_idx), key=lambda i: sizes[i])
    for i in idx_sorted:
        ang = angles[i]
        # 원 밖 라벨 위치
        x_out = np.cos(np.deg2rad(ang)) * outside_r
        y_out = np.sin(np.deg2rad(ang)) * outside_r
        # 원 경계 쪽(화살표 기준점)
        x_edge = np.cos(np.deg2rad(ang)) * 1.0
        y_edge = np.sin(np.deg2rad(ang)) * 1.0

        # 기존 내부 텍스트 숨기고(또는 제거) 바깥에 새로 배치
        txt_str = texts[i].get_text()
        texts[i].set_visible(False)

        # 좌우 정렬은 반대쪽으로 맞추면 보기 좋음
        ha = 'left' if x_out >= 0 else 'right'
        ax.annotate(
            txt_str,
            xy=(x_edge, y_edge), xycoords='data',           # 화살표 도착점(파이 경계)
            xytext=(x_out, y_out), textcoords='data',       # 텍스트 위치(원 밖)
            ha=ha, va='center', fontsize=9, color='black',
            arrowprops=dict(arrowstyle='-', color='gray', shrinkA=0, shrinkB=0)
        )

    ax.set_title("OS별 COST 비중", pad=24)
    plt.tight_layout()

    filepath1_dailySales = "graph3_costByOs.png"
    plt.savefig(filepath1_dailySales, dpi=160) # dpi : 해상도
    plt.close()

    blob = bucket.blob(f'{gameidx}/{filepath1_dailySales}')
    blob.upload_from_filename(filepath1_dailySales)

    # 메모리에 올라간 이미지 파일 삭제
    os.remove(filepath1_dailySales)

    return f'{gameidx}/{filepath1_dailySales}'


def merge_os_graph(joyplegameid: int, gameidx: str):
    p1 = os_rev_graph_draw(joyplegameid, gameidx)
    p2 = os_cost_graph_draw(joyplegameid, gameidx)

    # 2) 이미지 열기 (투명 보존 위해 RGBA)
    blob1 = bucket.blob(p1)
    blob2 = bucket.blob(p2)

    im1 = blob1.download_as_bytes()
    im2 = blob2.download_as_bytes()

    im1 = Image.open(BytesIO(im1))
    im2 = Image.open(BytesIO(im2))

    # ---- [옵션 A] 원본 크기 유지 + 세로 패딩으로 높이 맞추기 (권장: 왜곡 없음) ----
    target_h = max(im1.height, im2.height)

    def pad_to_height(img, h, bg=(255, 255, 255, 0)):  # 투명 배경: 알파 0
        if img.height == h:
            return img
        canvas = Image.new("RGBA", (img.width, h), bg)
        # 가운데 정렬로 붙이기 (위에 맞추려면 y=0)
        y = (h - img.height) // 2
        canvas.paste(img, (0, y))
        return canvas

    im1_p = pad_to_height(im1, target_h)
    im2_p = pad_to_height(im2, target_h)

    gap = 0  # 이미지 사이 여백(px). 필요하면 20 등으로 변경
    bg = (255, 255, 255, 0)  # 전체 배경(투명). 흰색 원하면 (255,255,255,255)

    out = Image.new("RGBA", (im1_p.width + gap + im2_p.width, target_h), bg)
    out.paste(im1_p, (0, 0), im1_p)
    out.paste(im2_p, (im1_p.width + gap, 0), im2_p)

    # 3) GCS에 저장
    output_buffer = BytesIO()
    out.save(output_buffer, format='PNG')
    output_buffer.seek(0)

    # GCS 경로
    gcs_path = f'{gameidx}/graph3_revAndCostByOs.png'
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(output_buffer.getvalue(), content_type='image/png')

    return gcs_path


#### 노션에 업로드

def country_data_upload_to_notion(joyplegameid: int, gameidx: str, **context):

    PAGE_INFO=context['task_instance'].xcom_pull(
        task_ids = 'make_gameframework_notion_page',
        key='page_info'
    )

    ########### (1) 제목
    notion.blocks.children.append(
        PAGE_INFO['id'],
        children=[
            {
                "object": "block",
                "type": "heading_2",
                "heading_2": {
                    "rich_text": [{"type": "text", "text": {"content": "3. 글로벌 모객 지표 " }}]
                },
            }
        ],
    )

    notion.blocks.children.append(
        PAGE_INFO['id'],
        children=[
            {
                "object": "block",
                "type": "heading_3",
                "heading_3": {
                    "rich_text": [{"type": "text", "text": {"content": "(1) 국가별 마케팅비용과 매출비중" }}]
                },
            }
        ],
    )

    query_result3_revByCountry=context['task_instance'].xcom_pull(
        task_ids='cohort_by_country_revenue',  # ← 첫 번째 Task의 task_id
        key='cohort_by_country_revenue_df'
    )
    query_result3_costByCountry=context['task_instance'].xcom_pull(
        task_ids='cohort_by_country_cost',  # ← 첫 번째 Task의 task_id
        key='cohort_by_country_cost_df'
    )

    filePath3_revAndCostByCountry = merge_contry_graph(joyplegameid, gameidx)
    ########### (2) 그래프 업로드
    ## IAP+유가젬
    # 1) 업로드 객체 생성 (file_upload 생성)
    create_url = "https://api.notion.com/v1/file_uploads"
    payload = {
        "filename": os.path.basename(filePath3_revAndCostByCountry),
        "content_type": "image/png"
    }
    headers_json = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json"
    }
    resp = requests.post(create_url, headers=headers_json, data=json.dumps(payload))
    resp.raise_for_status()
    file_upload = resp.json()
    file_upload_id = file_upload["id"]   # 업로드 ID
    # file_upload["upload_url"] 도 응답에 포함됨

    # 2) 파일 바이너리 전송 (multipart/form-data)
    send_url = f"https://api.notion.com/v1/file_uploads/{file_upload_id}/send"
    with open(filePath3_revAndCostByCountry, "rb") as f:
        files = {"file": (os.path.basename(filePath3_revAndCostByCountry), f, "image/png")}
        headers_send = {
            "Authorization": f"Bearer {NOTION_TOKEN}",
            "Notion-Version": NOTION_VERSION
            # Content-Type은 files로 자동 설정됨
        }
        send_resp = requests.post(send_url, headers=headers_send, files=files)
        send_resp.raise_for_status()


    # 3) 이미지 블록으로 페이지에 첨부
    append_url = f"https://api.notion.com/v1/blocks/{PAGE_INFO['id']}/children"
    append_payload = {
        "children": [
            {
                "object": "block",
                "type": "image",
                "image": {
                    "type": "file_upload",
                    "file_upload": {"id": file_upload_id},
                    # 캡션을 달고 싶다면 아래 주석 해제
                    # "caption": [{"type": "text", "text": {"content": "자동 업로드된 그래프"}}]
                }
            }
        ]
    }

    headers_json_patch = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json"
    }
    append_resp = requests.patch(append_url, headers=headers_json_patch, data=json.dumps(append_payload))
    append_resp.raise_for_status()

    ### 로데이터 제공
    resp = df_to_notion_table_under_toggle(
        notion=notion,
        page_id=PAGE_INFO['id'],
        df=query_result3_revByCountry,
        toggle_title="📊 로데이터 - 국가별 매출 ",
        max_first_batch_rows=90,
        batch_size=100,
    )
    resp = df_to_notion_table_under_toggle(
        notion=notion,
        page_id=PAGE_INFO['id'],
        df=query_result3_costByCountry,
        toggle_title="📊 로데이터 - 국가별 COST  ",
        max_first_batch_rows=90,
        batch_size=100,
    )


    ### 국가별 cost rev 코멘트
    ########### (3) 제미나이 해석

    text = cohort_by_gemini(joyplegameid)
    blocks = md_to_notion_blocks(text)
    notion.blocks.children.append(
        block_id=PAGE_INFO['id'],
        children=blocks
    )


    ## 부제목
    notion.blocks.children.append(
        PAGE_INFO['id'],
        children=[
            {
                "object": "block",
                "type": "heading_3",
                "heading_3": {
                    "rich_text": [{"type": "text", "text": {"content": "(2) OS별 마케팅비용과 매출비중" }}]
                },
            }
        ],
    )




## os별 cost, rev 그래프
########### (2) 그래프 업로드
## IAP+유가젬
def country_data_upload_to_notion(joyplegameid: int, gameidx: str, **context):

    PAGE_INFO=context['task_instance'].xcom_pull(
        task_ids = 'make_gameframework_notion_page',
        key='page_info'
    )

    query_result3_costByOs= context['task_instance'].xcom_pull(
        task_ids='os_cost',
        key='os_cost_df'
    )
    query_result3_revByOs= context['task_instance'].xcom_pull(
        task_ids='os_rev',
        key='os_rev_df'
    )

    filePath3_revAndCostByOs = merge_os_graph(joyplegameid, gameidx)

    # 1) 업로드 객체 생성 (file_upload 생성)
    create_url = "https://api.notion.com/v1/file_uploads"
    payload = {
        "filename": os.path.basename(filePath3_revAndCostByOs),
        "content_type": "image/png"
    }
    headers_json = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json"
    }
    resp = requests.post(create_url, headers=headers_json, data=json.dumps(payload))
    resp.raise_for_status()
    file_upload = resp.json()
    file_upload_id = file_upload["id"]   # 업로드 ID
    # file_upload["upload_url"] 도 응답에 포함됨

    # 2) 파일 바이너리 전송 (multipart/form-data)
    send_url = f"https://api.notion.com/v1/file_uploads/{file_upload_id}/send"
    with open(filePath3_revAndCostByOs, "rb") as f:
        files = {"file": (os.path.basename(filePath3_revAndCostByOs), f, "image/png")}
        headers_send = {
            "Authorization": f"Bearer {NOTION_TOKEN}",
            "Notion-Version": NOTION_VERSION
            # Content-Type은 files로 자동 설정됨
        }
        send_resp = requests.post(send_url, headers=headers_send, files=files)
        send_resp.raise_for_status()


    # 3) 이미지 블록으로 페이지에 첨부
    append_url = f"https://api.notion.com/v1/blocks/{PAGE_INFO['id']}/children"
    append_payload = {
        "children": [
            {
                "object": "block",
                "type": "image",
                "image": {
                    "type": "file_upload",
                    "file_upload": {"id": file_upload_id},
                    # 캡션을 달고 싶다면 아래 주석 해제
                    # "caption": [{"type": "text", "text": {"content": "자동 업로드된 그래프"}}]
                }
            }
        ]
    }

    headers_json_patch = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json"
    }
    append_resp = requests.patch(append_url, headers=headers_json_patch, data=json.dumps(append_payload))
    append_resp.raise_for_status()

    ### 로데이터 제공
    resp = df_to_notion_table_under_toggle(
        notion=notion,
        page_id=PAGE_INFO['id'],
        df=query_result3_revByOs,
        toggle_title="📊 로데이터 - 장기 자체결제 매출 ",
        max_first_batch_rows=90,
        batch_size=100,
    )

    resp = df_to_notion_table_under_toggle(
        notion=notion,
        page_id=PAGE_INFO['id'],
        df=query_result3_costByOs,
        toggle_title="📊 로데이터 - OS별 COST ",
        max_first_batch_rows=90,
        batch_size=100,
    )

    ## os별 cost, rev 코멘트
    ########### (3) 제미나이 해석
    blocks = md_to_notion_blocks(os_by_gemini(joyplegameid))
    notion.blocks.children.append(
        block_id=PAGE_INFO['id'],
        children=blocks
    )


# 최근 30일 기준 국가그룹별 X 결제처별 매출 쿼리
# 지리적 주요 국가별로 그룹화
# 한국
# 미국
# 일본
# 동아시아 & 오세아니아: 중국, 대만, 홍콩, 싱가포르, 태국, 베트남, 말레이시아, 필리핀, 인도네시아, 인도, 호주, 뉴질랜드
# 중동: 아랍에미리트, 사우디아라비아, 터키, 이란, 이스라엘, 카타르, 쿠웨이트, 오만, 바레인, 요르단
# 서유럽: 영국, 프랑스, 독일, 이탈리아, 스페인, 네덜란드, 벨기에, 스위스, 오스트리아, 아일랜드, 포르투갈
# 동유럽: 폴란드, 체코, 헝가리, 루마니아, 슬로바키아, 러시아, 우크라이나, 불가리아, 슬로베니아, 크로아티아
# 아메리카: 캐나다, 멕시코, 브라질, 아르헨티나, 칠레, 콜롬비아, 페루
# 기타: 그 외 국가

def country_group_rev(joyplegameid: int, gameidx: str, **context):
    query = f"""
    with chk as (
    SELECT
    perf.LogDateKST,
    perf,AuthAccountName,
    perf.CountryGroup,
        pg.PGRole,
        pg.PlatformDeviceTypeName,
        pg.PGName,
        pg.PGBuyCount,
        pg.PGPriceKRW
    FROM
    (
        select * except(CountryGroup)
            , CASE
    WHEN CountryCode = 'KR' THEN '한국'
    WHEN CountryCode = 'JP' THEN '일본'
    WHEN CountryCode = 'US' THEN '미국'
    WHEN CountryCode IN ('CN', 'TW', 'HK', 'SG', 'TH', 'VN', 'MY', 'PH', 'ID', 'IN', 'AU', 'NZ') THEN '동아시아 & 오세아니아'
    WHEN CountryCode IN ('AE', 'SA', 'TR', 'IR', 'IL', 'QA', 'KW', 'OM', 'BH', 'JO') THEN '중동'
    WHEN CountryCode IN ('GB', 'FR', 'DE', 'IT', 'ES', 'NL', 'BE', 'CH', 'AT', 'IE', 'PT') THEN '서유럽'
    WHEN CountryCode IN ('PL', 'CZ', 'HU', 'RO', 'SK', 'RU', 'UA', 'BG', 'SI', 'HR') THEN '동유럽'
    WHEN CountryCode IN ('CA', 'MX', 'BR', 'AR', 'CL', 'CO', 'PE') THEN '아메리카'
    ELSE '기타'
    END AS CountryGroup
        from `dataplatform-reporting.DataService.V_0317_0000_AuthAccountPerformance_V`
        where joyplegameid = {joyplegameid}
                and logdateKst >= DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 14 DAY)
                and logdatekst <= DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY)
    ) AS perf,
    UNNEST(perf.PaymentDetailArrayStruct) AS pg
    )

    select CountryGroup, LogDateKST, PGName, sum(PGPriceKRW) as Sales
    from chk
    group by CountryGroup, LogDateKST, PGName
    order by case when CountryGroup = '한국' then 1
                when CountryGroup = '미국' then 2
                when CountryGroup = '일본' then 3
                when CountryGroup = '동아시아 & 오세아니아' then 4
                when CountryGroup = '서유럽' then 5
                when CountryGroup = '동유럽' then 6
                when CountryGroup = '아메리카' then 7
                when CountryGroup = '중동' then 8
                when CountryGroup = '기타' then 9
            end
            , LogDateKST, PGName
    """


    query_result = query_run_method('3_global_ua', query)
    context['task_instance'].xcom_push(key='country_group_rev', value=query_result)

    return True

def country_group_to_df(**context):

    query_result = context['task_instance'].xcom_pull(
        task_ids='country_group_rev',
        key='country_group_rev'
    )

    grouped_dfs = {
        country: group_df.pivot_table(
            index="LogDateKST",
            columns="PGName",
            values="Sales",
            aggfunc="sum",
            fill_value=0
        )
        for country, group_df in query_result.groupby("CountryGroup")
    }


    # 로데이터 제공용 데이터프레임
    grouped_dfs_union = query_result.pivot_table(
        index=["CountryGroup", "LogDateKST"],  # 두 컬럼 기준으로 인덱스 구성
        columns="PGName",
        values="Sales",
        aggfunc="sum",
        fill_value=0
    ).reset_index()

    num_cols = grouped_dfs_union.select_dtypes(include="number").columns
    grouped_dfs_union[num_cols] = grouped_dfs_union[num_cols].astype(int)

    return grouped_dfs, grouped_dfs_union



def country_group_to_df_gemini(joyplegameid: int, service_sub: str, **context):

    query_result = context['task_instance'].xcom_pull(
        task_ids='country_group_rev',
        key='country_group_rev'
    )

    RUN_ID = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    LABELS = {"datascience_division_service": 'gameinsight_framework',
            "run_id": RUN_ID,
            f"datascience_division_service_sub" : {service_sub}}

    response_GeoPGSales = genai_client.models.generate_content(
        model=MODEL_NAME,
        contents=f"""
    지난 2주간 국가그룹별로 일간 결제처별 매출 데이터야.\n{query_result.to_csv(index=False)}
    각 국가그룹에서 결제처별로 일간 매출흐름이 어떻게 되는지, 어떤 결제처의 매출이 언제 급증했는지를 요약해서 6줄 이내로 알려줘.
    매출 급증의 원인을 파악하지는 말아줘.
    #, ##, ###, ####은 사용하지 말아줘.
    """,
        config=types.GenerateContentConfig(
            system_instruction=SYSTEM_INSTRUCTION,
            # tools=[RAG],
            temperature=0.3
            ,labels=LABELS
            # max_output_tokens=2048
        )
    )

    return response_GeoPGSales.text



def country_group_df_draw(joyplegameid: int, gameidx: str, **context):
    
    gcs_paths = []
    grouped_dfs, _ = country_group_to_df(**context)

    # ✅ 모든 그룹별로 그래프 생성
    for country, df in grouped_dfs.items():
        # index(LogDateKST)가 문자열이면 datetime으로 변환
        if not pd.api.types.is_datetime64_any_dtype(df.index):
            df.index = pd.to_datetime(df.index, errors="coerce")

        # 숫자형 변환 (Sales값)
        df = df.map(
            lambda x: pd.to_numeric(str(x).replace(",", "").replace("-", "0"), errors="coerce")
        )

        # ✅ 인덱스를 x축으로 사용
        x = df.index

        fig, ax = plt.subplots(figsize=(10, 5))

        # 결제수단별 선그래프
        for col in df.columns:
            ax.plot(
                x, df[col],
                marker='o', markersize=3, linewidth=1,
                label=col
            )

        plt.title(f"{country} - 일자별 결제수단 매출 추이")
        plt.xlabel("날짜")
        plt.ylabel("매출액")

        # y축 천 단위 포맷
        ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{int(x):,}"))

        # x축 라벨 회전
        plt.xticks(x, rotation=45)

        # 범례, 보조선, 저장
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0)
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        plt.tight_layout()

        filepath1_dailySales = f"graph_{country}.png"
        plt.savefig(filepath1_dailySales, dpi=160) # dpi : 해상도
        plt.close()

        blob = bucket.blob(f'{gameidx}/{filepath1_dailySales}')
        blob.upload_from_filename(filepath1_dailySales)

        # 메모리에 올라간 이미지 파일 삭제
        os.remove(filepath1_dailySales)

        # gcs 경로 추가
        gcs_paths.append(f'{gameidx}/{filepath1_dailySales}')

    return gcs_paths
    

def merge_images_by_three_gcs(
    bucket,
    gcs_image_paths: List[str],
    output_dir: str,
    gameidx: str,
    gap: int = 0,
    bg_color: Tuple[int, int, int, int] = (255, 255, 255, 0),
    cleanup_temp: bool = True
) -> List[str]:

    def pad_to_height(img: Image.Image, h: int, bg: Tuple = bg_color) -> Image.Image:
        """이미지의 높이를 맞춰줌 (세로 패딩 추가)"""
        if img.height == h:
            return img
        canvas = Image.new("RGBA", (img.width, h), bg)
        y = (h - img.height) // 2
        canvas.paste(img, (0, y))
        return canvas
    
    # 이미지 경로 리스트 출력
    print("처리할 GCS 이미지 목록:")
    for i, path in enumerate(gcs_image_paths, 1):
        print(f"  {i}. {path}")
    print()
    
    uploaded_paths = []
    temp_files = []
    
    # 3개씩 묶어서 처리
    for batch_num, i in enumerate(range(0, len(gcs_image_paths), 3), 1):
        imgs = []
        names = []
        
        # 최대 3개의 이미지 로드
        for j in range(3):
            if i + j < len(gcs_image_paths):
                gcs_path = gcs_image_paths[i + j]
                
                try:
                    # GCS에서 이미지 다운로드
                    blob = bucket.blob(gcs_path)
                    image_bytes = blob.download_as_bytes()
                    img = Image.open(BytesIO(image_bytes)).convert("RGBA")
                    imgs.append(img)
                    
                    # 파일명에서 확장자 제거 및 "graph_" 제거
                    name = os.path.splitext(os.path.basename(gcs_path))[0]
                    if name.startswith("graph_"):
                        name = name.replace("graph_", "", 1)
                    names.append(name)
                    
                except Exception as e:
                    print(f"경고: GCS에서 이미지를 로드할 수 없습니다 - {gcs_path}")
                    print(f"  에러: {str(e)}")
                    continue
        
        if not imgs:
            print(f"배치 {batch_num}: 유효한 이미지가 없습니다.")
            continue
        
        # 세로 맞추기
        target_h = max(img.height for img in imgs)
        imgs_padded = [pad_to_height(img, target_h) for img in imgs]
        
        # 가로로 합치기
        total_width = sum(img.width for img in imgs_padded) + gap * (len(imgs_padded) - 1)
        out = Image.new("RGBA", (total_width, target_h), bg_color)
        
        x_offset = 0
        for img in imgs_padded:
            out.paste(img, (x_offset, 0), img)
            x_offset += img.width + gap
        
        # 파일명 구성
        merged_filename = "graph_" + " 및 ".join(names) + ".png"
        temp_filepath = f"/tmp/{merged_filename}"
        
        # 로컬에 임시 저장
        out.save(temp_filepath)
        temp_files.append(temp_filepath)
        
        # GCS 경로 구성
        gcs_upload_path = f"{gameidx}/{output_dir}/{merged_filename}"
        
        # GCS에 업로드
        upload_blob = bucket.blob(gcs_upload_path)
        upload_blob.upload_from_filename(temp_filepath)
        
        print(f"✓ 배치 {batch_num} 저장됨: gs://{bucket.name}/{gcs_upload_path}")
        uploaded_paths.append(gcs_upload_path)
    
    # 로컬 임시 파일 정리
    if cleanup_temp:
        for temp_file in temp_files:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        print(f"\n✓ {len(temp_files)}개의 임시 파일 정리 완료")
    
    print(f"\n총 {len(uploaded_paths)}개의 합쳐진 이미지가 GCS에 업로드되었습니다.")

    return uploaded_paths


def merge_country_group_df_draw(joyplegameid: int, gameidx: str, **context):
    """
    Airflow DAG에서 사용할 wrapper 함수
    """
    from google.cloud import storage
    
    # GCS 클라이언트 및 버킷 초기화
    client = storage.Client()
    bucket = client.bucket("game-framework1")  # 버킷명 수정 필요
    
    # 이미지 저장 경로 가져오기 (리스트)
    img_gcs_list = country_group_df_draw(joyplegameid, gameidx, **context)
    
    # 합치기 처리
    merged_paths = merge_images_by_three_gcs(
        bucket=bucket,
        gcs_image_paths=img_gcs_list,
        output_dir="merged",  # GCS 내 출력 디렉토리
        gameidx=gameidx,
        gap=0,
        bg_color=(255, 255, 255, 0),
        cleanup_temp=True
    )
    
    return merged_paths


def country_group_data_upload_to_notion(joyplegameid: int, gameidx: str, bucket_name: str = "game-framework1", merged_image_dir: str= "merged", **context):

    PAGE_INFO=context['task_instance'].xcom_pull(
        task_ids = 'make_gameframework_notion_page',
        key='page_info'
    )

    notion.blocks.children.append(
        PAGE_INFO['id'],
        children=[
            {
                "object": "block",
                "type": "heading_2",
                "heading_2": {
                    "rich_text": [{"type": "text", "text": {"content": "국가별 X 결제처별 지표" }}]
                },
            }
        ],
    )

    notion.blocks.children.append(
        PAGE_INFO['id'],
        children=[
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [
                        {'annotations': {'bold': True,
                                                'code': False,
                                                'color': 'default',
                                                'italic': False,
                                                'strikethrough': False,
                                                'underline': False},
                        "type": "text", "text": {"content": "🌐 국가그룹 분류 기준\n1. 한국\n2. 미국\n3. 일본\n"}},
                        {'annotations': {'bold': True,
                                                'code': False,
                                                'color': 'default',
                                                'italic': False,
                                                'strikethrough': False,
                                                'underline': False},
                        "type": "text", "text": {"content": "4. 서유럽: "}},
                        {'annotations': {'bold': False,
                                                'code': False,
                                                'color': 'default',
                                                'italic': False,
                                                'strikethrough': False,
                                                'underline': False},
                        "type": "text", "text": {"content": "영국, 프랑스, 독일, 이탈리아, 스페인, 네덜란드, 벨기에, 스위스, 오스트리아, 아일랜드, 포르투갈\n"}},
                        {'annotations': {'bold': True,
                                                'code': False,
                                                'color': 'default',
                                                'italic': False,
                                                'strikethrough': False,
                                                'underline': False},
                        "type": "text", "text": {"content": "5. 동유럽: "}},
                        {'annotations': {'bold': False,
                                                'code': False,
                                                'color': 'default',
                                                'italic': False,
                                                'strikethrough': False,
                                                'underline': False},
                        "type": "text", "text": {"content": "폴란드, 체코, 헝가리, 루마니아, 슬로바키아, 러시아, 우크라이나, 불가리아, 슬로베니아, 크로아티아\n"}},
                        {'annotations': {'bold': True,
                                                'code': False,
                                                'color': 'default',
                                                'italic': False,
                                                'strikethrough': False,
                                                'underline': False},
                        "type": "text", "text": {"content": "6. 동아시아 & 오세아니아: "}},
                        {'annotations': {'bold': False,
                                                'code': False,
                                                'color': 'default',
                                                'italic': False,
                                                'strikethrough': False,
                                                'underline': False},
                        "type": "text", "text": {"content": "중국, 대만, 홍콩, 싱가포르, 태국, 베트남, 말레이시아, 필리핀, 인도네시아, 인도, 호주, 뉴질랜드\n"}},
                        {'annotations': {'bold': True,
                                                'code': False,
                                                'color': 'default',
                                                'italic': False,
                                                'strikethrough': False,
                                                'underline': False},
                        "type": "text", "text": {"content": "7. 아메리카: "}},
                        {'annotations': {'bold': False,
                                                'code': False,
                                                'color': 'default',
                                                'italic': False,
                                                'strikethrough': False,
                                                'underline': False},
                        "type": "text", "text": {"content": "캐나다, 멕시코, 브라질, 아르헨티나, 칠레, 콜롬비아, 페루\n"}},
                                            {'annotations': {'bold': True,
                                                'code': False,
                                                'color': 'default',
                                                'italic': False,
                                                'strikethrough': False,
                                                'underline': False},
                        "type": "text", "text": {"content": "8. 중동: "}},
                        {'annotations': {'bold': False,
                                                'code': False,
                                                'color': 'default',
                                                'italic': False,
                                                'strikethrough': False,
                                                'underline': False},
                        "type": "text", "text": {"content": "아랍에미리트, 사우디아라비아, 터키, 이란, 이스라엘, 카타르, 쿠웨이트, 오만, 바레인, 요르단\n"}},
                        {'annotations': {'bold': True,
                                                'code': False,
                                                'color': 'default',
                                                'italic': False,
                                                'strikethrough': False,
                                                'underline': False},
                        "type": "text", "text": {"content": "9. 기타: "}},
                        {'annotations': {'bold': False,
                                                'code': False,
                                                'color': 'default',
                                                'italic': False,
                                                'strikethrough': False,
                                                'underline': False},
                        "type": "text", "text": {"content": "그 외 국가들"}},
                        ]
                },
            }
        ],
    )

    # GCS 클라이언트 및 버킷 초기화
    gcs_client = storage.Client()
    bucket = gcs_client.bucket(bucket_name)
    
    # GCS에서 합쳐진 이미지 목록 조회
    gcs_image_paths = []
    blobs = gcs_client.list_blobs(
        bucket_name,
        prefix=f"{gameidx}/{merged_image_dir}/"
    )

    for blob in blobs:
        # "및"이 포함된 PNG 파일만 필터링
        if blob.name.lower().endswith(".png") and "및" in blob.name:
            gcs_image_paths.append(blob.name)
    
    # 파일명 역순 정렬
    gcs_image_paths.sort(reverse=True)
    
    print(f"업로드할 이미지 개수: {len(gcs_image_paths)}개")
    print("이미지 목록:")
    for path in gcs_image_paths:
        print(f"  - {path}")
    

    # 공통 헤더
    headers_json = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json"
    }

    # GCS에서 이미지 다운로드 및 Notion 업로드
    for gcs_path in gcs_image_paths:
        filename = gcs_path.split('/')[-1]
        print(f"\n업로드 중: {filename}")
        
        try:
            # GCS에서 이미지 바이너리 다운로드
            blob = bucket.blob(gcs_path)
            image_bytes = blob.download_as_bytes()
            
            # 파일 업로드 객체 생성
            create_url = "https://api.notion.com/v1/file_uploads"
            payload = {
                "filename": filename,
                "content_type": "image/png"
            }
            resp = requests.post(create_url, headers=headers_json, data=json.dumps(payload))
            resp.raise_for_status()
            file_upload = resp.json()
            file_upload_id = file_upload["id"]
            
            # 파일 바이너리 전송
            send_url = f"https://api.notion.com/v1/file_uploads/{file_upload_id}/send"
            files = {"file": (filename, BytesIO(image_bytes), "image/png")}
            headers_send = {
                "Authorization": f"Bearer {NOTION_TOKEN}",
                "Notion-Version": NOTION_VERSION
            }
            send_resp = requests.post(send_url, headers=headers_send, files=files)
            send_resp.raise_for_status()
            
            # Notion 페이지에 이미지 블록으로 첨부
            append_url = f"https://api.notion.com/v1/blocks/{PAGE_INFO['id']}/children"
            append_payload = {
                "children": [
                    {
                        "object": "block",
                        "type": "image",
                        "image": {
                            "type": "file_upload",
                            "file_upload": {"id": file_upload_id},
                        }
                    }
                ]
            }
            
            append_resp = requests.patch(
                append_url, headers=headers_json, data=json.dumps(append_payload)
            )
            append_resp.raise_for_status()
            
            print(f"✅ 업로드 완료: {filename}")
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Notion API 에러: {filename}")
            print(f"  에러: {str(e)}")
            continue
        except Exception as e:
            print(f"❌ GCS 다운로드 또는 업로드 에러: {filename}")
            print(f"  에러: {str(e)}")
            continue
    
    print("\n🎉 모든 이미지 업로드 완료!")

    _, grouped_dfs_union =country_group_to_df(**context)

    resp = df_to_notion_table_under_toggle(
        notion=notion,
        page_id=PAGE_INFO['id'],
        df=grouped_dfs_union,
        toggle_title="📊 로데이터 - 국가별 X 결제처별 지표 ",
        max_first_batch_rows=90,
        batch_size=100,
    )

    blocks = md_to_notion_blocks(country_group_to_df_gemini(joyplegameid, "3_global_ua"))
    notion.blocks.children.append(
        block_id=PAGE_INFO['id'],
        children=blocks
    )

    return True


def rev_group_rev_pu(joyplegameid: int, **context):
    query = f"""
    select logdatekst,Week
    , cast(sum(if(rgroup_final = 'R0', pricekrw, null)) as int64)as R0_Sales
    , cast(sum(if(rgroup_final = 'R1', pricekrw, null)) as int64) as R1_Sales
    , cast(sum(if(rgroup_final = 'R2', pricekrw, null)) as int64) as R2_Sales
    , cast(sum(if(rgroup_final = 'R3', pricekrw, null)) as int64) as R3_Sales
    , cast(sum(if(rgroup_final = 'R4', pricekrw, null)) as int64) as R4_Sales
    , cast(sum(if(rgroup_final = '전월 무과금', pricekrw, null)) as int64) as `전월 무과금_Sales`
    , cast(sum(if(rgroup_final = '당월가입자', pricekrw, null)) as int64) as `당월가입자_Sales`
    , cast(sum(pricekrw) as int64) as `전체유저_Sales`

    , count(distinct if(rgroup_final = 'R0' and pricekrw>0 , authaccountname, null)) as R0_PU
    , count(distinct if(rgroup_final = 'R1' and pricekrw>0 , authaccountname, null)) as R1_PU
    , count(distinct if(rgroup_final = 'R2' and pricekrw>0 , authaccountname, null)) as R2_PU
    , count(distinct if(rgroup_final = 'R3' and pricekrw>0 , authaccountname, null)) as R3_PU
    , count(distinct if(rgroup_final = 'R4' and pricekrw>0 , authaccountname, null)) as R4_PU
    , count(distinct if(rgroup_final = '전월 무과금' and pricekrw>0 , authaccountname, null)) as `전월 무과금_PU`
    , count(distinct if(rgroup_final = '당월가입자' and pricekrw>0 , authaccountname, null)) as `당월가입자_PU`
    , count(distinct if(pricekrw>0, authaccountname, null)) as `전체유저_PU`
    from
    (select *, concat(cast(cast(DATE_TRUNC(logdatekst ,week(Wednesday)) as date) as string),' ~ ',
                    cast(date_add(cast(DATE_TRUNC(logdatekst,week(Wednesday)) as date), interval 6 day) as string)) as Week
    from `data-science-division-216308.gameInsightFramework.paymentGroup`
    where logdatekst>= DATE_SUB(DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH), INTERVAL 1 MONTH)
    and logdatekst<=LAST_DAY(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH))
    and joypleGameID = {joyplegameid}
    group by 1,2
    order by 1
    """

    query_result =query_run_method('4_detail_sales', query)
    context['task_instance'].xcom_push(key='rev_group_rev_pu', value=query_result)

    return True


def rev_group_rev_pu_gemini(joyplegameid: int, service_sub: str, **context):
    rev_group_rev_pu_data = context['task_instance'].xcom_pull(
        task_ids = 'rev_group_rev_pu',
        key='rev_group_rev_pu'
    )

    RUN_ID = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    LABELS = {"datascience_division_service": 'gameinsight_framework',
            "run_id": RUN_ID,
            f"datascience_division_service_sub" : {service_sub}}

    response4_RgroupSales = genai_client.models.generate_content(
    model=MODEL_NAME,
    contents = f"""

    과금그룹 정의는 다음과 같아.
    R0 : 전월 과금액 1천만원 이상
    R1 : 전월 과금액 1천만원 미만 ~ 1백만원 이상
    R2 : 전월 과금액 1백만원 미만 ~ 10만원 이상
    R3 : 전월 과금액 10만원 미만 ~ 1만원 이상
    R4 : 전월 과금액 1만원 미만 ~ 0원 초과
    전월 무과금 : 전월 무과금 유저
    당월가입자 : 이번달에 가입한 유저

    1. 이번주 매출과 PU 트렌드에 대해, 지난주와 지지난주와 비교해서 특별한 점이 있다면 알려줘.
    총합이나 평균적인것 말고도 트렌드 변화에 대해서도 알려줘
    2. 존댓말로 써줘. 10줄 내로 써줘
    3. 분석한 결과는 다음과 같습니다 혹은 분석해보았습니다 등의 말을 쓰지말고 바로 분석한 내용에 대해 알려줘.
    제공해주신 데이터는 이런 말 쓰지말아줘 바로 분석결과를 알려줘
    4. 비교할때, 이번주가 다 지나지 않았으면 다른 주차도 동일한 일수를 가지고 비교해주고, 어떻게 동일기간 비교되었는지도 명시해줘.
    5. 매출은 일자별 총합으로 비교해도 되지만, PU 는 그날의 PU 이기 때문에 총합보다는 트렌드로 비교해줘.
    6. 비교할 때는 R0, R1,R2 그룹을 위주로 말해줘
    7. 한문장당 줄바꿈 한번 해줘.
    8. 매출 1원단위까지 다 쓰지 말고 대략 말해줘.e.g : 5100만원 , 1억 2천만원
    9. 한 문장마다 노션의 마크다운 리스트 문법을 사용해줘. e.g. * 당월 매출은 이렇습니다.
    10. 향후 어떻게 해야된다는 말은 하지 말아줘.
    11. 핵심적인 내용 한줄을 서두에 써주고(Bold 처리), 마지막 문단에 결론이나 요약은 작성하지 말아줘.
    핵심내용은 R0,R1,R2 의 매출이 어떻게 되었는지가 필요해

    <일자별 과금그룹별 매출액>
    {rev_group_rev_pu_data}


    """
    ,
    config=types.GenerateContentConfig(
            system_instruction=SYSTEM_INSTRUCTION,
            # tools=[RAG],
            temperature=0.5
            ,labels=LABELS
        )
    )

    # 코멘트 출력
    return response4_RgroupSales.text


def iap_gem_ruby(joyplegameid:int, databaseschema: str='GW', **context):
    query = f"""
    select logdate_kst,week
    , cast(sum(if(cat_package2='전투기', sales_buygem, 0)) as int64) as `전투기`
    , cast(sum(if(cat_package2='종합', sales_buygem, 0)) as int64) as `종합`
    , cast(sum(if(cat_package2='자원', sales_buygem, 0)) as int64) as `자원`
    , cast(sum(if(cat_package2='항공모함', sales_buygem, 0)) as int64) as `항공모함`
    , cast(sum(if(cat_package2='영웅', sales_buygem, 0)) as int64) as `영웅`
    , cast(sum(if(cat_package2='군함', sales_buygem, 0)) as int64) as `군함`
    , cast(sum(if(cat_package2='배틀패스', sales_buygem, 0)) as int64) as `배틀패스`
    , cast(sum(if(cat_package2='연구', sales_buygem, 0)) as int64) as `연구`
    , cast(sum(if(cat_package2='장비', sales_buygem, 0)) as int64) as `장비`
    , cast(sum(if(cat_package2 not in ('전투기','종합','자원','항공모함','영웅','군함','루비','배틀패스','연구','장비'), sales_buygem, null)) as int64) as `기타`
    from
    (
    select *
    , format_date('%Y-%m',  logdate_kst ) as month
    , concat(cast(cast(DATE_TRUNC(logdate_kst ,week(Wednesday)) as date) as string),' ~ ',
                cast(date_add(cast(DATE_TRUNC(logdate_kst,week(Wednesday)) as date), interval 6 day) as string)) as week
    , case when cat_shop = '배틀패스' then '배틀패스' else cat_package end as cat_package2
    from
    (
    ### IAP
        (
        select 'IAP' As idx, logdate_kst, datetime(logtime_kst) as logtime_kst, authaccountname
        , package_name, cat_shop, cat_package, cast(package_kind as string) as package_kind
        , pricekrw as sales_usegem, pricekrw as sales_buygem
        from `data-science-division-216308.{databaseschema}.Sales_iap_hub`
        where (cat_package not in ('젬','루비') or cat_package is null)
        )
    union all
    ### GEM
        (
        select 'GEM' as idx, logdate_kst, datetime(logtime_kst) as logtime_kst, auth_account_name
        , case when action_category_name = 'payment' then package_name else action_name end as package_name
        , case when action_category_name = 'payment' then cat_shop else 'contents' end as cat_shop
        , case when action_category_name = 'payment' then cat_package else 'contents' end as cat_package
        , package_kind
        , (usegem*(1500/40)) as sales_usegem, (buygem*(1500/40)) as sales_buygem
        from `data-science-division-216308.gameInsightFramework.sales_goods`  where is_tester=0
        and joyple_game_code = {joyplegameid}
        and goods_name='gem' and add_or_spend = 'spend'
        )
    union all
    ### RUBY
        (
        select 'RUBY' as idx, logdate_kst, datetime(logtime_kst) as logtime_kst, auth_account_name
        , case when action_category_name = 'payment' then package_name else action_name end as package_name
        , case when action_category_name = 'payment' then cat_shop else 'contents' end as cat_shop
        , case when action_category_name = 'payment' then cat_package else 'contents' end as cat_package
        , package_kind
        , (usegem*(15000/999)) as sales_usegem, (buygem*(15000/999)) as sales_buygem
        from `data-science-division-216308.gameInsightFramework.sales_goods`  where is_tester=0
        and joyple_game_code = {joyplegameid}
        and goods_name='ruby' and add_or_spend = 'spend')
        )
    )
    where logdate_kst>= DATE_SUB(DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH), INTERVAL 1 MONTH)
    and logdate_kst<=LAST_DAY(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
    group by 1,2 order by 1
    """

    query_result =query_run_method('4_detail_sales', query)
    context['task_instance'].xcom_push(key='iap_gem_ruby', value=query_result)

    return True


def iap_gem_ruby_history(gameidx: str, **context):
    query = f"""
    select *
    from (
        select distinct updateDate `업데이트일`
                        , case when category is null then '기타'
                                when category = '이벤트 (운영툴)' then '이벤트'
                                else category end as `업데이트 항목 분류`
                        , title as `업데이트 내용`
        from `data-science-division-216308.gameInsightFramework.{gameidx}_history`
        where date(updateDate)>= DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 32 DAY)
        and date(updateDate)<=LAST_DAY(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
        and (title is not null or title != '' or title != ' ')
        )
    where `업데이트 항목 분류` not in ('기타', '점검 기본 정보', 'LQA', '버그 수정 및 사용성 개선', 'BM_상점')
    order by `업데이트일`desc
    """

    query_result =query_run_method('4_detail_sales', query)
    # 1주전 수요일부터 어제일자까지의 데이터만으로 전처리 (쿼리에서 전처리하는 것으로 추후 수정 필요)

    # 문자열 컬럼 -> datetime으로 파싱 (실패한 값은 NaT)
    s = pd.to_datetime(query_result['업데이트일'], errors='coerce')

    # 한국시간 기준 오늘/전일
    today = pd.Timestamp.now(tz='Asia/Seoul').normalize().date()
    yesterday = today - pd.Timedelta(days=1)

    # 오늘 기준 '직전 수요일'(오늘이 수요일이면 오늘 제외) 계산
    # 월=0, 화=1, 수=2, ... 일=6
    w = today.weekday()
    delta_to_last_wed = (w - 2) % 7
    if delta_to_last_wed == 0:  # 오늘이 수요일이면 7일 전을 '직전 수요일'로
        delta_to_last_wed = 7
    last_wed = today - pd.Timedelta(days=delta_to_last_wed)

    # 👉 "1주 전 수요일"을 시작일로
    start_date = last_wed

    # 구간: [1주 전 수요일, 전일] (양끝 포함)
    mask = (s.dt.date >= start_date) & (s.dt.date <= yesterday)
    query_result4_ingameHistory = query_result.loc[mask].copy()

    context['task_instance'].xcom_push(key='iap_gem_ruby_history', value=query_result4_ingameHistory)

    return True


def iap_gem_ruby_gemini(service_sub: str, **context):
    
    query_result4_salesByPackage = context['task_instance'].xcom_pull(
        task_ids = 'iap_gem_ruby',
        key='iap_gem_ruby'
    )

    query_result4_ingameHistory = context['task_instance'].xcom_pull(
        task_ids = 'iap_gem_ruby_history',
        key='iap_gem_ruby_history'
    )

    RUN_ID = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    LABELS = {"datascience_division_service": 'gameinsight_framework',
            "run_id": RUN_ID,
            f"datascience_division_service_sub" : {service_sub}}

    response4_salesByPackage = genai_client.models.generate_content(
    model=MODEL_NAME,
    contents = f"""
    다음은 이번주 상품 카테고리별 매출액이야.
    \n{query_result4_salesByPackage.to_csv(index=False)}

    이번주 상품 카테고리 매출에 대해서 특별한 점을 아주 간단히 요약해서 말해줘. (15줄이내)
    이번주는 데이터 "week" 컬럼에서 가장 최근을 말해.
    다음의 게임 업데이트일과 업데이트 내용 데이터를 참고하고, 연관이 없다면 업데이트 내용에 대해서는 언급하지마.
    전주, 전전주와 비교하되, 동일기간으로 비교해줘. (이번주 데이터가 3일치만 있으면 전주, 전전주도 3일치만 비교)

    < 서두에 쓰일 내용>
    1. 핵심적인 내용 한줄을 서두에 써주고(Bold 처리), 마지막 문단에 결론이나 요약은 작성하지 말아줘.
    2. 서두에 핵심내용 쓸때는 먼저 이번주 기간과 지난주, 지지난주 기간을 써주고 동일기간 비교 했다고도 같이 써줘.
    3. 어떤 상품 카테고리에서 차이가 어떻게 났다고 서두에 써줘.



    <업데이트 히스토리>
    {query_result4_ingameHistory.to_csv(index=False)}


    < 서식 요구사항 >
    1. 한문장당 줄바꿈 한번 해줘.
    2. 매출 1원단위까지 다 쓰지 말고 대략 말해줘.
    3. 한 문장마다 노션의 마크다운 리스트 문법을 사용해줘. e.g. * 당월 매출은 이렇습니다.
    4. 매출액 잘 확인해줘. 1억인데 10억이라고 쓰지마

    """,

    ### 이전버전 프롬프트 ###
    # contents - f"""
    # 이번주 상품군별 매출액에서 특별한 점을 알려줘.
    # 히스토리가 있으면 참고해주고 없으면 아예 히스토리에 대해 아무 언급하지말아줘

    # 1. 한문장당 줄바꿈 한번 해줘.
    # 2. 매출 1원단위까지 다 쓰지 말고 대략 말해줘.
    # 3. 한 문장마다 노션의 마크다운 리스트 문법을 사용해줘. e.g. * 당월 매출은 이렇습니다.


    # <일자별 상품군별 매출액>
    # {query_result4_salesByPackage}
    # <히스토리>
    # {query_result4_ingameHistory}
    # """

    config=types.GenerateContentConfig(
            system_instruction=SYSTEM_INSTRUCTION,
            # tools=[RAG],
            temperature=0.5
            ,labels=LABELS
        )
    )

    # 코멘트 출력
    return response4_salesByPackage.text


def iap_df(joyplegameid: int, databaseschema: str='GW', **context):
    # IAP
    query = f"""
    WITH base AS (
    SELECT *
    FROM `data-science-division-216308.{databaseschema}.Sales_iap_hub`
    WHERE logdate_kst >= DATE_SUB(DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH), INTERVAL 1 MONTH)
        AND logdate_kst <= LAST_DAY(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
    ),

    daily AS (  -- 일자 x 상품군 매출
    SELECT logdate_kst, cat_package, SUM(pricekrw) AS rev
    FROM base
    GROUP BY 1,2
    ),

    top_cat AS (  -- 매출 top15 (동률 시 이름 오름차순으로 결정)
    SELECT cat_package
    FROM
        (
            SELECT cat_package, sum(rev) AS peak_rev
            FROM daily
            GROUP BY 1
        )
    ORDER BY peak_rev DESC, cat_package ASC
    LIMIT 15
    )

    SELECT format_date('%Y-%m', d.logdate_kst) as month,
        concat(cast(cast(DATE_TRUNC(d.logdate_kst ,week(Wednesday)) as date) as string),' ~ ',
                    cast(date_add(cast(DATE_TRUNC(d.logdate_kst,week(Wednesday)) as date), interval 6 day) as string)) as week,
        d.logdate_kst,
        IF(d.cat_package IN (SELECT cat_package FROM top_cat), d.cat_package, '기타') AS cat_package_grouped,
        SUM(d.rev) AS rev
    FROM daily d
    GROUP BY 1,2,3,4
    ORDER BY 1,2,3,4

    """

    query_result =query_run_method('4_detail_sales', query)
    # 카테고리별로 Pivot

    query_result4_salesByPackage_IAP = query_result.pivot_table(
        index=["month", "week", "logdate_kst"],  # 두 컬럼 기준으로 인덱스 구성
        columns="cat_package_grouped",
        values="rev",
        aggfunc="sum",
        fill_value=0
    ).reset_index()

    context['task_instance'].xcom_push(key='iap_df', value=query_result4_salesByPackage_IAP)

    return True


def gem_df(joyplegameid: int, **context):
    query = f"""
    WITH base AS (
    SELECT * EXCEPT(package_name, cat_shop, cat_package)
    , CASE WHEN action_category_name = 'payment' THEN package_name ELSE action_name END AS package_name
    , CASE WHEN action_category_name = 'payment' THEN cat_shop ELSE 'contents' END AS cat_shop
    , CASE WHEN action_category_name = 'payment' THEN cat_package ELSE 'contents' END AS cat_package
    FROM `data-science-division-216308.gameInsightFramework.sales_goods`
    WHERE is_tester=0
    AND joyple_game_code = {joyplegameid}
    AND goods_name='gem' and add_or_spend = 'spend'
    AND logdate_kst >= DATE_SUB(DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH), INTERVAL 1 MONTH)
    AND logdate_kst <= LAST_DAY(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
    ),

    daily AS (  -- 일자 x 상품군 매출
    SELECT logdate_kst, cat_package, SUM(usegem) AS usegem
    FROM base
    GROUP BY 1,2
    ),

    top_cat AS (  -- 매출 top15 (동률 시 이름 오름차순으로 결정)
    SELECT cat_package
    FROM
        (SELECT cat_package, sum(usegem) AS peak_usegem
        FROM daily
        GROUP BY 1)
    ORDER BY peak_usegem DESC, cat_package ASC
    LIMIT 15
    )

    SELECT format_date('%Y-%m', d.logdate_kst) as month,
        concat(cast(cast(DATE_TRUNC(d.logdate_kst ,week(Wednesday)) as date) as string),' ~ ',
                    cast(date_add(cast(DATE_TRUNC(d.logdate_kst,week(Wednesday)) as date), interval 6 day) as string)) as week,
    d.logdate_kst,
    IF(d.cat_package IN (SELECT cat_package FROM top_cat), d.cat_package, '기타') AS cat_package_grouped,
    SUM(d.usegem) AS usegem
    FROM daily d
    GROUP BY 1,2,3,4
    ORDER BY 1,2,3,4
    ;

    """

    query_result =query_run_method('4_detail_sales', query)

    query_result4_salesByPackage_GEM = query_result.pivot_table(
        index=["month", "week", "logdate_kst"],  # 두 컬럼 기준으로 인덱스 구성
        columns="cat_package_grouped",
        values="usegem",
        aggfunc="sum",
        fill_value=0
    ).reset_index()

    context['task_instance'].xcom_push(key='gem_df', value=query_result4_salesByPackage_GEM)

    return True


def ruby_df(joyplegameid: int, **context):
    
    query = f"""
    WITH base AS (
    SELECT * EXCEPT(package_name, cat_shop, cat_package)
    , CASE WHEN action_category_name = 'payment' THEN package_name ELSE action_name END AS package_name
    , CASE WHEN action_category_name = 'payment' THEN cat_shop ELSE 'contents' END AS cat_shop
    , CASE WHEN action_category_name = 'payment' THEN cat_package ELSE 'contents' END AS cat_package
    FROM `data-science-division-216308.gameInsightFramework.sales_goods`
    WHERE is_tester=0
    AND joyple_game_code = {joyplegameid}
    AND goods_name='ruby' and add_or_spend = 'spend'
    AND logdate_kst >= DATE_SUB(DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH), INTERVAL 1 MONTH)
    AND logdate_kst <= LAST_DAY(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
    ),

    daily AS (  -- 일자 x 상품군 매출
    SELECT logdate_kst, cat_package, SUM(usegem) AS useruby
    FROM base
    GROUP BY 1,2
    ),

    top_cat AS (  -- 매출 top15 (동률 시 이름 오름차순으로 결정)
    SELECT cat_package
    FROM
        (SELECT cat_package, sum(useruby) AS peak_useruby
        FROM daily
        GROUP BY 1)
    ORDER BY peak_useruby DESC, cat_package ASC
    LIMIT 15
    )

    SELECT format_date('%Y-%m', d.logdate_kst) as month,
        concat(cast(cast(DATE_TRUNC(d.logdate_kst ,week(Wednesday)) as date) as string),' ~ ',
                    cast(date_add(cast(DATE_TRUNC(d.logdate_kst,week(Wednesday)) as date), interval 6 day) as string)) as week,
    d.logdate_kst,
    IF(d.cat_package IN (SELECT cat_package FROM top_cat), d.cat_package, '기타') AS cat_package_grouped,
    SUM(d.useruby) AS useruby
    FROM daily d
    GROUP BY 1,2,3,4
    ORDER BY 1,2,3,4
    ;

    """

    query_result =query_run_method('4_detail_sales', query)

    query_result4_salesByPackage_RUBY = query_result.pivot_table(
        index=["month", "week", "logdate_kst"],  # 두 컬럼 기준으로 인덱스 구성
        columns="cat_package_grouped",
        values="useruby",
        aggfunc="sum",
        fill_value=0
    ).reset_index()

    context['task_instance'].xcom_push(key='ruby_df', value=query_result4_salesByPackage_RUBY)

    return True


def iap_df_gemini(service_sub: str, **context):

    iap_df = context['task_instance'].xcom_pull(
        task_ids = 'iap_df',
        key='iap_df'
    )

    iap_gem_ruby_history = context['task_instance'].xcom_pull(
        task_ids = 'iap_gem_ruby_history',
        key='iap_gem_ruby_history'
    )

    RUN_ID = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    LABELS = {"datascience_division_service": 'gameinsight_framework',
            "run_id": RUN_ID,
            f"datascience_division_service_sub" : {service_sub}}
    
    response4_salesByPackage_IAP = genai_client.models.generate_content(
    model=MODEL_NAME,

    contents = f"""
    다음은 이번주 상품 카테고리별 매출액이야.
    \n{iap_df.to_csv(index=False)}

    이번주 IAP 상품 카테고리 매출에 대해서 특별한 점을 아주 간단히 요약해서 말해줘. (15줄이내)
    이번주는 데이터 "week" 컬럼에서 가장 최근을 말해.
    다음의 게임 업데이트일과 업데이트 내용 데이터를 참고하고, 연관이 없다면 업데이트 내용에 대해서는 언급하지마.
    전주, 전전주와 비교하되, 동일기간으로 비교해줘. (이번주 데이터가 3일치만 있으면 전주, 전전주도 3일치만 비교)
    6줄 이내로 작성해줘.

    < 서두에 쓰일 내용>
    1. 핵심적인 내용 한줄을 서두에 써주고(Bold 처리), 마지막 문단에 결론이나 요약은 작성하지 말아줘.
    2. 서두에 핵심내용 쓸때는 먼저 이번주 기간과 지난주, 지지난주 기간을 써주고 동일기간 비교 했다고도 같이 써줘.
    3. 어떤 상품 카테고리에서 차이가 어떻게 났다고 서두에 써줘.



    <업데이트 히스토리>
    {iap_gem_ruby_history.to_csv(index=False)}


    < 서식 요구사항 >
    1. 한문장당 줄바꿈 한번 해줘.
    2. 매출 1원단위까지 다 쓰지 말고 대략 말해줘.
    3. 한 문장마다 노션의 마크다운 리스트 문법을 사용해줘. e.g. * 당월 매출은 이렇습니다.
    4. 매출액 잘 확인해줘. 1억인데 10억이라고 쓰지마

    """,

    ### 이전버전 프롬프트 ###
    # contents - f"""
    # 이번주 상품군별 매출액에서 특별한 점을 알려줘.
    # 히스토리가 있으면 참고해주고 없으면 아예 히스토리에 대해 아무 언급하지말아줘

    # 1. 한문장당 줄바꿈 한번 해줘.
    # 2. 매출 1원단위까지 다 쓰지 말고 대략 말해줘.
    # 3. 한 문장마다 노션의 마크다운 리스트 문법을 사용해줘. e.g. * 당월 매출은 이렇습니다.


    # <일자별 상품군별 매출액>
    # {query_result4_salesByPackage}
    # <히스토리>
    # {query_result4_ingameHistory}
    # """

    config=types.GenerateContentConfig(
            system_instruction=SYSTEM_INSTRUCTION,
            # tools=[RAG],
            temperature=0.5
            ,labels=LABELS
        )
    )

    # 코멘트 출력
    return response4_salesByPackage_IAP.text


def gem_df_gemini(service_sub: str, **context):
    gem_df = context['task_instance'].xcom_pull(
        task_ids = 'gem_df',
        key='gem_df'
    )

    iap_gem_ruby_history = context['task_instance'].xcom_pull(
        task_ids = 'iap_gem_ruby_history',
        key='iap_gem_ruby_history'
    )

    RUN_ID = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    LABELS = {"datascience_division_service": 'gameinsight_framework',
            "run_id": RUN_ID,
            f"datascience_division_service_sub" : {service_sub}}
    
    response4_salesByPackage_GEM = genai_client.models.generate_content(
    model=MODEL_NAME,
    contents = f"""
    다음은 이번주 젬으로 구매한 상품들의 카테고리별 젬소모량이야.
    \n{gem_df.to_csv(index=False)}

    이번주 젬으로 구매한 상품들의 카테고리 젬 소모량에 대해서 특별한 점을 아주 간단히 요약해서 말해줘. (15줄이내)
    이번주는 데이터 "week" 컬럼에서 가장 최근을 말해.
    다음의 게임 업데이트일과 업데이트 내용 데이터를 참고하고, 연관이 없다면 업데이트 내용에 대해서는 언급하지마.
    전주, 전전주와 비교하되, 동일기간으로 비교해줘. (이번주 데이터가 3일치만 있으면 전주, 전전주도 3일치만 비교)
    6줄 이내로 작성해줘.

    < 서두에 쓰일 내용>
    1. 핵심적인 내용 한줄을 서두에 써주고(Bold 처리), 마지막 문단에 결론이나 요약은 작성하지 말아줘.
    2. 서두에 핵심내용 쓸때는 먼저 이번주 기간과 지난주, 지지난주 기간을 써주고 동일기간 비교 했다고도 같이 써줘.
    3. 어떤 상품 카테고리에서 차이가 어떻게 났다고 서두에 써줘.



    <업데이트 히스토리>
    {iap_gem_ruby_history.to_csv(index=False)}


    < 서식 요구사항 >
    1. 한문장당 줄바꿈 한번 해줘.
    2. 젬소비량을 첫번째 자리까지 다 쓰지 말고 대략 말해줘.
    3. 한 문장마다 노션의 마크다운 리스트 문법을 사용해줘. e.g. * 당월 젬소비량은 이렇습니다.
    4. 젬소비량을 잘 확인해줘. 1억인데 10억이라고 쓰지마

    """,

    ### 이전버전 프롬프트 ###
    # contents - f"""
    # 이번주 상품군별 매출액에서 특별한 점을 알려줘.
    # 히스토리가 있으면 참고해주고 없으면 아예 히스토리에 대해 아무 언급하지말아줘

    # 1. 한문장당 줄바꿈 한번 해줘.
    # 2. 매출 1원단위까지 다 쓰지 말고 대략 말해줘.
    # 3. 한 문장마다 노션의 마크다운 리스트 문법을 사용해줘. e.g. * 당월 매출은 이렇습니다.


    # <일자별 상품군별 매출액>
    # {query_result4_salesByPackage}
    # <히스토리>
    # {query_result4_ingameHistory}
    # """

    config=types.GenerateContentConfig(
            system_instruction=SYSTEM_INSTRUCTION,
            # tools=[RAG],
            temperature=0.5
            ,labels=LABELS
        )
    )

    # 코멘트 출력
    return response4_salesByPackage_GEM.text


def ruby_df_gemini(service_sub: str, **context):
    ruby_df = context['task_instance'].xcom_pull(
        task_ids = 'ruby_df',
        key='ruby_df'
    )

    iap_gem_ruby_history = context['task_instance'].xcom_pull(
        task_ids = 'iap_gem_ruby_history',
        key='iap_gem_ruby_history'
    )

    RUN_ID = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    LABELS = {"datascience_division_service": 'gameinsight_framework',
            "run_id": RUN_ID,
            f"datascience_division_service_sub" : {service_sub}}
    
    response4_salesByPackage_RUBY = genai_client.models.generate_content(
    model=MODEL_NAME,
    contents = f"""
    다음은 이번주 루비로 구매한 상품들의 카테고리별 루비 소모량이야.
    \n{ruby_df.to_csv(index=False)}

    이번주 루비로 구매한 상품들의 카테고리 루비 소모량 대해서 특별한 점을 아주 간단히 요약해서 말해줘. (15줄이내)
    이번주는 데이터 "week" 컬럼에서 가장 최근을 말해.
    다음의 게임 업데이트일과 업데이트 내용 데이터를 참고하고, 연관이 없다면 업데이트 내용에 대해서는 언급하지마.
    전주, 전전주와 비교하되, 동일기간으로 비교해줘. (이번주 데이터가 3일치만 있으면 전주, 전전주도 3일치만 비교)
    6줄 이내로 작성해줘.
    단위는 원이 아니라 루비로 표기해줘.

    < 서두에 쓰일 내용>
    1. 핵심적인 내용 한줄을 서두에 써주고(Bold 처리), 마지막 문단에 결론이나 요약은 작성하지 말아줘.
    2. 서두에 핵심내용 쓸때는 먼저 이번주 기간과 지난주, 지지난주 기간을 써주고 동일기간 비교 했다고도 같이 써줘.
    3. 어떤 상품 카테고리에서 차이가 어떻게 났다고 서두에 써줘.



    <업데이트 히스토리>
    {iap_gem_ruby_history.to_csv(index=False)}


    < 서식 요구사항 >
    1. 한문장당 줄바꿈 한번 해줘.
    2. 루비 소비량을 한자리 수 단위까지 다 쓰지 말고 대략 말해줘.
    3. 한 문장마다 노션의 마크다운 리스트 문법을 사용해줘. e.g. * 당월 루비 소비량은 이렇습니다.
    4. 루비 소비량을 잘 확인해줘. 1억인데 10억이라고 쓰지마

    """,

    ### 이전버전 프롬프트 ###
    # contents - f"""
    # 이번주 상품군별 매출액에서 특별한 점을 알려줘.
    # 히스토리가 있으면 참고해주고 없으면 아예 히스토리에 대해 아무 언급하지말아줘

    # 1. 한문장당 줄바꿈 한번 해줘.
    # 2. 매출 1원단위까지 다 쓰지 말고 대략 말해줘.
    # 3. 한 문장마다 노션의 마크다운 리스트 문법을 사용해줘. e.g. * 당월 매출은 이렇습니다.


    # <일자별 상품군별 매출액>
    # {query_result4_salesByPackage}
    # <히스토리>
    # {query_result4_ingameHistory}
    # """

    config=types.GenerateContentConfig(
            system_instruction=SYSTEM_INSTRUCTION,
            # tools=[RAG],
            temperature=0.5
            ,labels=LABELS
        )
    )

    # 코멘트 출력
    return response4_salesByPackage_RUBY.text


def weekly_iapcategory_rev(joyplegameid: int, gameidx: str, databaseschema: str, **context):
    
    query = f"""
    with base as (
        select *
        , format_date('%Y-%m',  logdate_kst ) as month
    , concat(cast(cast(DATE_TRUNC(logdate_kst ,week(Wednesday)) as date) as string),' ~ ',
                    cast(date_add(cast(DATE_TRUNC(logdate_kst,week(Wednesday)) as date), interval 6 day) as string)) as logweek
    , case when cat_shop = '배틀패스' then '배틀패스' else cat_package end as cat_package2
    from
    (

        ## IAP
        (
        select 'IAP' As idx, logdate_kst, datetime(logtime_kst) as logtime_kst, authaccountname
        , package_name, cat_shop, cat_package, cast(package_kind as string) as package_kind
        , pricekrw as sales_usegem, pricekrw as sales_buygem
        from `data-science-division-216308.{databaseschema}.Sales_iap_hub`
        where (cat_package not in ('젬','루비') or cat_package is null)
        and logdate_kst >= CASE
                    WHEN EXTRACT(DAYOFWEEK FROM CURRENT_DATE("Asia/Seoul")) = 4
                    THEN DATE_SUB(CURRENT_DATE("Asia/Seoul"), INTERVAL 7 DAY)
                    ELSE DATE_SUB(
                            CURRENT_DATE("Asia/Seoul"),
                            INTERVAL MOD(EXTRACT(DAYOFWEEK FROM CURRENT_DATE("Asia/Seoul")) - 4 + 7, 7) DAY
                            )
                    END
        AND logdate_kst <= DATE_SUB(CURRENT_DATE("Asia/Seoul"), INTERVAL 1 DAY))

        union all

        ## GEM
        (select 'GEM' as idx, logdate_kst, datetime(logtime_kst) as logtime_kst, auth_account_name
        , case when action_category_name = 'payment' then package_name else action_name end as package_name
        , case when action_category_name = 'payment' then cat_shop else 'contents' end as cat_shop
        , case when action_category_name = 'payment' then cat_package else 'contents' end as cat_package
        , package_kind
        , (usegem*(1500/40)) as sales_usegem, (buygem*(1500/40)) as sales_buygem
        from `data-science-division-216308.gameInsightFramework.sales_goods`  where is_tester=0
        and joyple_game_code = {joyplegameid}
        and goods_name='gem' and add_or_spend = 'spend'
        and logdate_kst >= CASE
                    WHEN EXTRACT(DAYOFWEEK FROM CURRENT_DATE("Asia/Seoul")) = 4
                    THEN DATE_SUB(CURRENT_DATE("Asia/Seoul"), INTERVAL 7 DAY)
                    ELSE DATE_SUB(
                            CURRENT_DATE("Asia/Seoul"),
                            INTERVAL MOD(EXTRACT(DAYOFWEEK FROM CURRENT_DATE("Asia/Seoul")) - 4 + 7, 7) DAY
                            )
                    END
        AND logdate_kst <= DATE_SUB(CURRENT_DATE("Asia/Seoul"), INTERVAL 1 DAY))

        union all

        ## RUBY
        (select 'RUBY' as idx, logdate_kst, datetime(logtime_kst) as logtime_kst, auth_account_name
        , case when action_category_name = 'payment' then package_name else action_name end as package_name
        , case when action_category_name = 'payment' then cat_shop else 'contents' end as cat_shop
        , case when action_category_name = 'payment' then cat_package else 'contents' end as cat_package
        , package_kind
        , (usegem*(15000/999)) as sales_usegem, (buygem*(15000/999)) as sales_buygem
        from `data-science-division-216308.gameInsightFramework.sales_goods`  where is_tester=0
        and joyple_game_code = {joyplegameid}
        and goods_name='ruby' and add_or_spend = 'spend'
        and logdate_kst >= CASE
                    WHEN EXTRACT(DAYOFWEEK FROM CURRENT_DATE("Asia/Seoul")) = 4
                    THEN DATE_SUB(CURRENT_DATE("Asia/Seoul"), INTERVAL 7 DAY)
                    ELSE DATE_SUB(
                            CURRENT_DATE("Asia/Seoul"),
                            INTERVAL MOD(EXTRACT(DAYOFWEEK FROM CURRENT_DATE("Asia/Seoul")) - 4 + 7, 7) DAY
                            )
                    END
        AND logdate_kst <= DATE_SUB(CURRENT_DATE("Asia/Seoul"), INTERVAL 1 DAY))
        )
    )

    , daily AS (  -- 일자 x 상품군 매출
    SELECT logdate_kst, cat_package2, SUM(sales_buygem) AS rev
    FROM base
    GROUP BY 1,2
    ),

    top_cat AS (  -- 매출 top15 (동률 시 이름 오름차순으로 결정)
    SELECT cat_package2
    FROM
        (SELECT cat_package2, sum(rev) AS peak_rev
        FROM daily
        GROUP BY 1)
    ORDER BY peak_rev DESC, cat_package2 ASC
    LIMIT 15
    )

    SELECT d.logdate_kst,
        IF(d.cat_package2 IN (SELECT cat_package2 FROM top_cat), d.cat_package2, '기타') AS cat_package_grouped,
        SUM(d.rev) AS rev
    FROM daily d
    GROUP BY 1,2
    ORDER BY 1,2


    """

    query_result =query_run_method('4_detail_sales', query)

    query_result4_salesByCategory = query_result.pivot_table(
        index=["logdate_kst"],  # 두 컬럼 기준으로 인덱스 구성
        columns="cat_package_grouped",
        values="rev",
        aggfunc="sum",
        fill_value=0
    ).reset_index()

    exclude = {'logdate_kst', '기타'}
    cols = [c for c in query_result4_salesByCategory.columns if c not in exclude]

    # 작은따옴표 이스케이프 안전 처리
    def sq(c: str) -> str:
        return "'" + c.replace("'", "''") + "'"

    query_result4_salesByCategory_Cols = ", ".join(sq(c) for c in cols)

    context['task_instance'].xcom_push(key='weekly_iapcategory_rev', value=query_result4_salesByCategory)
    context['task_instance'].xcom_push(key='weekly_iapcategory_rev_cols', value=query_result4_salesByCategory_Cols)

    return True


def ruby_df_gemini(service_sub: str, **context):
    weekly_iapcategory_rev = context['task_instance'].xcom_pull(
        task_ids = 'weekly_iapcategory_rev',
        key='weekly_iapcategory_rev'
    )

    RUN_ID = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    LABELS = {"datascience_division_service": 'gameinsight_framework',
            "run_id": RUN_ID,
            f"datascience_division_service_sub" : {service_sub}}

    response4_salesByCategory = genai_client.models.generate_content(
    model=MODEL_NAME,
    contents=f"""
    지난 업데이트일부터 전일자까지의 상품 카테고리별 매출 정보가 들어있는 다음의 데이터를 참조해서 어떤 상품 카테고리에서 매출이 높게 나왔고, 어떤 상품 카테고리에서 매출이 크게 변화했는지를 확인해줘.\n{weekly_iapcategory_rev.to_csv(index=False)}
    어떤 상품 카테고리에서 매출이 높게 나왔는지는 상위 3개만 알려주고, 매출이 크게 변화한 카테고리에서는 매출 상위 3개 카테고리는 제외하고 알려줘.
    제언은 하지 말아줘.
    """,
    config=types.GenerateContentConfig(
        system_instruction=SYSTEM_INSTRUCTION,
        # tools=[RAG],
        temperature=0.1
        ,labels=LABELS
        # max_output_tokens=2048
        )
    )

    response4_CategoryListUp = genai_client.models.generate_content(
    model=MODEL_NAME,
    contents=f"""
    {response4_salesByCategory.text}\n상품 카테고리별 정보에서 상품 카테고리만 추출해서 다음과 같은 형식으로 대답해줘.
    ('골드', '은화', '청사진', '전술 교본', '자원')
    """,
        config=types.GenerateContentConfig(
            temperature=0
            ,labels=LABELS
            # max_output_tokens=2048
        )
    )

    CategoryListUp = re.search(r"\(.*\)", response4_CategoryListUp.text)
    if CategoryListUp:
        # eval로 문자열을 실제 tuple로 변환
        CategoryListUp_2 = eval(CategoryListUp.group(0))
        # SQL용 문자열로 변환
        CategoryListUp_SQL = ", ".join([f"'{c}'" for c in CategoryListUp_2])
    # SQL order by 시, 상품 카테고리 내림차순 정렬 그대로 반영하기 위한 코드
    case_when_str = "\n".join(
        [f"WHEN '{c}' THEN {i+1}" for i, c in enumerate(CategoryListUp_2)]
    )

    return CategoryListUp_SQL, case_when_str, response4_salesByCategory, response4_CategoryListUp



def top3_items_by_category(joyplegameid: int, gameidx: str, databaseschema:str,  service_sub: str, **context):

    weekly_iapcategory_rev_cols = context['task_instance'].xcom_pull(
        task_ids = 'weekly_iapcategory_rev',
        key='weekly_iapcategory_rev_cols'
    )

    CategoryListUp_SQL, case_when_str, _, _ = ruby_df_gemini(service_sub)
    
    RUN_ID = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    LABELS = {"datascience_division_service": 'gameinsight_framework',
        "run_id": RUN_ID,
        f"datascience_division_service_sub" : {service_sub}}


    query = f"""
    with sales_data as (
    select `일자`
        , case when rnum <= 3 then `상품결제 재화` else null end as `상품결제 재화`
        ,`상품 카테고리`
        , case when rnum <= 3 then `상품` else '그 외 상품들' end as `상품 이름`
        , `매출`
    from (
        select logdate_kst as `일자`
        , idx as `상품결제 재화`
        , cat_package2 as `상품 카테고리`
        , package_name as `상품`
        , sum(sales_buygem) as `매출`
        , row_number() over(partition by cat_package2, logdate_kst order by sum(sales_buygem) desc) as rnum
        from(
            select *
            , format_date('%Y-%m',  logdate_kst ) as month

            , concat(cast(cast(DATE_TRUNC(logdate_kst ,week(Wednesday)) as date) as string),' ~ ',
                        cast(date_add(cast(DATE_TRUNC(logdate_kst,week(Wednesday)) as date), interval 6 day) as string)) as logweek
            , case when cat_shop = '배틀패스' then '배틀패스'
                when cat_package not in ({weekly_iapcategory_rev_cols}) then '기타'
                        else cat_package end as cat_package2
            from (
    ### IAP
    (select 'IAP' As idx, logdate_kst, datetime(logtime_kst) as logtime_kst, authaccountname
    , package_name, cat_shop, cat_package, cast(package_kind as string) as package_kind
    , pricekrw as sales_usegem, pricekrw as sales_buygem
    from `data-science-division-216308.{databaseschema}.Sales_iap_hub`
    where (cat_package not in ('젬','루비') or cat_package is null)
                and logdate_kst >= CASE
                            WHEN EXTRACT(DAYOFWEEK FROM CURRENT_DATE("Asia/Seoul")) = 4
                            THEN DATE_SUB(CURRENT_DATE("Asia/Seoul"), INTERVAL 7 DAY)
                            ELSE DATE_SUB(
                                    CURRENT_DATE("Asia/Seoul"),
                                    INTERVAL MOD(EXTRACT(DAYOFWEEK FROM CURRENT_DATE("Asia/Seoul")) - 4 + 7, 7) DAY
                                    )
                            END
                AND logdate_kst <= DATE_SUB(CURRENT_DATE("Asia/Seoul"), INTERVAL 1 DAY))

                union all

    (select 'GEM' as idx, logdate_kst, datetime(logtime_kst) as logtime_kst, auth_account_name
    , case when action_category_name = 'payment' then package_name else action_name end as package_name
    , case when action_category_name = 'payment' then cat_shop else 'contents' end as cat_shop
    , case when action_category_name = 'payment' then cat_package else 'contents' end as cat_package
    , package_kind
    , (usegem*(1500/40)) as sales_usegem, (buygem*(1500/40)) as sales_buygem
    from `data-science-division-216308.gameInsightFramework.sales_goods`  where is_tester=0
    and joyple_game_code = {joyplegameid}
    and goods_name='gem' and add_or_spend = 'spend'
                and logdate_kst >= CASE
                            WHEN EXTRACT(DAYOFWEEK FROM CURRENT_DATE("Asia/Seoul")) = 4
                            THEN DATE_SUB(CURRENT_DATE("Asia/Seoul"), INTERVAL 7 DAY)
                            ELSE DATE_SUB(
                                    CURRENT_DATE("Asia/Seoul"),
                                    INTERVAL MOD(EXTRACT(DAYOFWEEK FROM CURRENT_DATE("Asia/Seoul")) - 4 + 7, 7) DAY
                                    )
                            END
                AND logdate_kst <= DATE_SUB(CURRENT_DATE("Asia/Seoul"), INTERVAL 1 DAY))

                union all

    ### RUBY
    (select 'RUBY' as idx, logdate_kst, datetime(logtime_kst) as logtime_kst, auth_account_name
    , case when action_category_name = 'payment' then package_name else action_name end as package_name
    , case when action_category_name = 'payment' then cat_shop else 'contents' end as cat_shop
    , case when action_category_name = 'payment' then cat_package else 'contents' end as cat_package
    , package_kind
    , (usegem*(15000/999)) as sales_usegem, (buygem*(15000/999)) as sales_buygem
    from `data-science-division-216308.gameInsightFramework.sales_goods`  where is_tester=0
    and joyple_game_code = {joyplegameid}
    and goods_name='ruby' and add_or_spend = 'spend'
                and logdate_kst >= CASE
                            WHEN EXTRACT(DAYOFWEEK FROM CURRENT_DATE("Asia/Seoul")) = 4
                            THEN DATE_SUB(CURRENT_DATE("Asia/Seoul"), INTERVAL 7 DAY)
                            ELSE DATE_SUB(
                                    CURRENT_DATE("Asia/Seoul"),
                                    INTERVAL MOD(EXTRACT(DAYOFWEEK FROM CURRENT_DATE("Asia/Seoul")) - 4 + 7, 7) DAY
                                    )
                            END
                AND logdate_kst <= DATE_SUB(CURRENT_DATE("Asia/Seoul"), INTERVAL 1 DAY))
            )
            )
    where cat_package2 in ({CategoryListUp_SQL})
        group by 1,2,3,4
        )
    )

    select `일자`, `상품결제 재화`, `상품 카테고리`, `상품 이름`, sum(`매출`) as `매출`
    from sales_data
    where `상품 이름` != '그 외 상품들'
    group by `일자`, `상품결제 재화`, `상품 카테고리`, `상품 이름`
    order by `일자`,
            CASE `상품 카테고리`
            {case_when_str}
            ELSE 99
            END,
            case when `상품 이름` = '그 외 상품들' then 1 else 0 end,
            `매출` desc

    """

    query_result=query_run_method('4_detail_sales', query)
    query_result['매출'] = query_result['매출'].map(lambda x: f"{int(x)}")

    context['task_instance'].xcom_push(key='top3_items_by_category', value=query_result)

    return True



def top3_items_by_category_gemini(service_sub: str, **context):

    query_result4_salesByPackage_ListedCategory = context['task_instance'].xcom_pull(
        task_ids = 'top3_items_by_category',
        key='top3_items_by_category'
    )

    RUN_ID = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    LABELS = {"datascience_division_service": 'gameinsight_framework',
            "run_id": RUN_ID,
            f"datascience_division_service_sub" : {service_sub}}

    _, _, response4_salesByCategory = ruby_df_gemini(service_sub)

    response4_salesByPackage_ListedCategory = genai_client.models.generate_content(
    model=MODEL_NAME,
    contents=f"""
    {query_result4_salesByPackage_ListedCategory.to_csv(index=False)}
    위의 데이터는 매출 상위 3위 카테고리 및 매출 변동이 높았던 카테고리들의 상품별 매출 데이터야.
    \n{response4_salesByCategory.text}
    그리고 위의 데이터는 상품 카테고리별 매출 변화 요약한 내용이야.
    두 내용을 참조해서,
    매출 상위 3개 카테고리는 각 일자별로 어떤 상품들 때문인지(모든 날짜를 참조해줘),
    그 외 카테고리들은 매출 변동이 큰 날짜에만 어떤 상품들 때문인지 분석해줘.
    제시된 데이터만으로 알 수 없는 상품 카테고리에 대해서는 언급하지 말아줘.
    주어진 데이터가 하루만 있다는 유의사항은 말하지마.
    """,
        config=types.GenerateContentConfig(
            system_instruction=SYSTEM_INSTRUCTION,
            # tools=[RAG],
            temperature=0.1
            ,labels=LABELS
            # max_output_tokens=9000
        )
    )

    response4_WeeklySales_Draft1 = genai_client.models.generate_content(
    model=MODEL_NAME,
    contents=f"""
        다음은 상품 카테고리별 매출 지표에 대한 요약글이야.\n{response4_salesByCategory.text}
        그리고 다음은 상품 카테고리별로 매출에 기여한 상품에 대한 정보글이야.\n{response4_salesByPackage_ListedCategory.text}
        두 글을 종합해서, 기간동안 게임의 매출 변화에 대한 리포트를 작성해줘.
        리포트 작성시 반드시 아래의 형식으로 작성해줘.

        서두에는 '금주 카테고리별 매출 상세 리포트 (데이터 기간)'으로 작성해줘. Bold 처리해줘.
        각 상품 카테고리(매출 상위 카테고리 인 경우, 순위 언급. e.g. 전투기 (매출 1위). Bold 처리해줘.)
        * 상품 카테고리 주요 변화. 일자와 수치를 언급해줘. 문단 제목은 작성하지 말아줘. 큰 변화가 있거나 매출이 높았던 날짜에 대해서만 언급해줘.
        * 해당 상품 카테고리에서의 주요 상품 매출, 일자와 수치를 언급해줘. 문단 제목은 작성하지 말아줘. 주요 날짜에 대해서만 언급해줘.
        '상품 카테고리 주요 변화'와 '해당 상품 카테고리에서의 주요 상품 매출'은 마크다운 리스트 형식으로 작성해줘.
        각 상품 카테고리별로 6줄 이내로 작성해줘.

        '젬'과 '루비'는 서로 다른 카테고리야. 동일한 카테고리로 취급하지마.
        상품명에 있는 날짜로 상품명의 출시된 날짜를 언급하지마.
        총 매출은 언급하지마.
        주어진 정보를 제외한 이벤트 및 프로모션은 언급하지마.
        """,
    config=types.GenerateContentConfig(
        system_instruction=SYSTEM_INSTRUCTION,
        # tools=[RAG],
        temperature=0.5
        ,labels=LABELS
        # max_output_tokens=2048
        )
    )

    iap_gem_ruby_history = context['task_instance'].xcom_pull(
        task_ids = 'iap_gem_ruby_history',
        key='iap_gem_ruby_history'
    )

    response4_WeeklySales_Report = genai_client.models.generate_content(
    model=MODEL_NAME,
    contents=f"""
    다음은 게임 업데이트일과 업데이트 내용 데이터야.\n{iap_gem_ruby_history.to_csv(index=False)}
    다음의 지난 업데이트 이후 매출 분석 리포트에서, 업데이트와 연관이 있는 상품 카테고리나 상품이 있다면 해당 업데이트 내용과 연관이 있을 수 있음을 언급해줘.\n{response4_WeeklySales_Draft1.text}
    주어진 분석 리포트의 형식에 각 상품 카테고리별로 업데이트 관련 내용만 언급을 추가하는 식으로 구성해줘. e.g.*   **업데이트 연관성:**
    상품명의 날짜가 들어가 있다는 사실만으로 업데이트와 연관성이 있다고 추론하지 마.
    정보가 제공되지 않았다는 말은 하지마.
    """,
    config=types.GenerateContentConfig(
        system_instruction=SYSTEM_INSTRUCTION,
        # tools=[RAG],
        temperature=0.1
        ,labels=LABELS
        # max_output_tokens=2048
        )
    )

    return response4_WeeklySales_Report.text


def rgroup_top3_pu(joyplegameid:int, gameidx:str, databaseschema:str, **context):
    query = f"""
        with raw as (
        select *
        , format_date('%Y-%m',  logdate_kst ) as month
        , format_date('%Y-%m',  authaccountregdatekst ) as regmonth

        , concat(cast(cast(DATE_TRUNC(logdate_kst ,week(Wednesday)) as date) as string),' ~ ',
                        cast(date_add(cast(DATE_TRUNC(logdate_kst,week(Wednesday)) as date), interval 6 day) as string)) as week
        , case when cat_shop = '배틀패스' then '배틀패스' else cat_package end as cat_package2
        , DATE_SUB(
                    date_add(current_date('Asia/Seoul'),interval -1 day),
                    INTERVAL MOD(EXTRACT(DAYOFWEEK FROM date_add(current_date('Asia/Seoul'),interval -1 day)) - 4 + 7, 7) DAY
                    ) AS week_start ## 가장 최근 직전 수요일
        from
        (
        ### IAP
        (select 'IAP' As idx, logdate_kst, datetime(logtime_kst) as logtime_kst, authaccountname, authaccountregdatekst
        , package_name, cat_shop, cat_package, cast(package_kind as string) as package_kind
        , cast(price_sheet as int64) as price_sheet
        , pricekrw as sales_usegem, pricekrw as sales_buygem
        from `data-science-division-216308.{databaseschema}.Sales_iap_hub`
        where (cat_package not in ('젬','루비') or cat_package is null))
        union all
        ### GEM
        (select 'GEM' as idx, logdate_kst, datetime(logtime_kst) as logtime_kst, auth_account_name, authaccountregdatekst
        , case when action_category_name = 'payment' then package_name else action_name end as package_name
        , case when action_category_name = 'payment' then cat_shop else 'contents' end as cat_shop
        , case when action_category_name = 'payment' then cat_package else 'contents' end as cat_package
        , package_kind
        , cast(price_sheet as int64)*(1500/40) as price_sheet
        , (usegem*(1500/40)) as sales_usegem, (buygem*(1500/40)) as sales_buygem
        from `data-science-division-216308.gameInsightFramework.sales_goods`  where is_tester=0
        and joyple_game_code = {joyplegameid}
        and goods_name='gem' and add_or_spend = 'spend')
        union all
        ### RUBY
        (select 'RUBY' as idx, logdate_kst, datetime(logtime_kst) as logtime_kst, auth_account_name, authaccountregdatekst
        , case when action_category_name = 'payment' then package_name else action_name end as package_name
        , case when action_category_name = 'payment' then cat_shop else 'contents' end as cat_shop
        , case when action_category_name = 'payment' then cat_package else 'contents' end as cat_package
        , package_kind, cast(price_sheet as int64)*(15000/999) as price_sheet
        , (usegem*(15000/999)) as sales_usegem, (buygem*(15000/999)) as sales_buygem
        from `data-science-division-216308.gameInsightFramework.sales_goods`  where is_tester=0
        and joyple_game_code = {joyplegameid}
        and goods_name='ruby' and add_or_spend = 'spend')

        )
        where logdate_kst>= DATE_SUB(DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH), INTERVAL 1 MONTH)
        and logdate_kst<=LAST_DAY(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
        ),

        sales_raw as ( ## 5331039
        select *  , format_date('%Y-%m',  logdatekst ) as month
        from `dataplatform-reporting.DataService.V_0317_0000_AuthAccountPerformance_V`
        where joyplegameid = {joyplegameid}
        and logdatekst>='2025-01-01'
        ),


        monthly_rev as (
        select authaccountname, logmonth, regmonth, ifnull(sum(pricekrw),0) as rev
        from
        (select *
        , format_date('%Y-%m-01',  logdatekst ) as logmonth
        , format_date('%Y-%m',  AuthAccountRegDateKST ) as regmonth
        from sales_raw
        where logdatekst>='2025-01-01')
        group by 1,2,3
        ),

        r_group as (
        select *
        , case
        when rev>=10000000 then 'R0'
        when rev>=1000000  then 'R1'
        when rev>=100000   then 'R2'
        when rev>=10000    then 'R3'
        when rev>=1        then 'R4'
        # when rev=0         then 'nonPU'
        else 'ETC' end as rgroup
        from monthly_rev
        where rev>0
        ),

        raw2 as (
        select a.*,month_key
        , case
        when a.month2 <= a.regmonth then '당월가입자'
        when b.rgroup is null then '전월 무과금'
        else b.rgroup end as rgroup_final

        from
        (select * , format_date('%Y-%m',  week_start ) as month2
        from raw) as a

        left join
        (select * , format_date('%Y-%m', date_add(date(logmonth), interval 1 month )) as month_key
        from r_group
        ) as b

        on a.authaccountname = b.authaccountname
        and a.month2 = b.month_key  ## 주차 시작일과 조인
        ),

        raw3 as (

        select *
        , row_number() OVER (partition by week, rgroup_final ORDER BY PU desc, sales desc  ) AS pu_rank
        , row_number() OVER (partition by week, rgroup_final ORDER BY sales desc, PU desc ) AS sales_rank
        from
        (select week, rgroup_final, package_name, cat_shop as shop_category, cat_package2 as package_category, price_sheet
        , count(distinct authaccountname) as PU
        , cast(sum(sales_buygem) as int64) as sales
        from raw2
        where logdate_kst between week_start and DATE_ADD(week_start, INTERVAL 6 DAY) ## 이번주 필터(수요일부터 화요일)
        and sales_buygem>0 ## 유가젬 사용만
        group by 1,2,3,4,5,6)
        )

        select *
        from raw3
        where rgroup_final is not null
        and pu_rank in (1,2,3)
        order by rgroup_final, pu_rank
    """

    query_result = query_run_method('4_detail_sales', query)

    context['task_instance'].xcom_push(key='rgroup_top3_pu', value=query_result)

    return True


def rgroup_top3_rev(joyplegameid:int, gameidx:str, databaseschema:str, **context):
    query = f"""

    with raw as (
    select *
    , format_date('%Y-%m',  logdate_kst ) as month
    , format_date('%Y-%m',  authaccountregdatekst ) as regmonth

    , concat(cast(cast(DATE_TRUNC(logdate_kst ,week(Wednesday)) as date) as string),' ~ ',
                    cast(date_add(cast(DATE_TRUNC(logdate_kst,week(Wednesday)) as date), interval 6 day) as string)) as week
    , case when cat_shop = '배틀패스' then '배틀패스' else cat_package end as cat_package2
    , DATE_SUB(
                date_add(current_date('Asia/Seoul'),interval -1 day),
                INTERVAL MOD(EXTRACT(DAYOFWEEK FROM date_add(current_date('Asia/Seoul'),interval -1 day)) - 4 + 7, 7) DAY
                ) AS week_start ## 가장 최근 직전 수요일
    from
    (
    ### IAP
    (select 'IAP' As idx, logdate_kst, datetime(logtime_kst) as logtime_kst, authaccountname, authaccountregdatekst
    , package_name, cat_shop, cat_package, cast(package_kind as string) as package_kind
    , cast(price_sheet as int64) as price_sheet
    , pricekrw as sales_usegem, pricekrw as sales_buygem
    from `data-science-division-216308.{databaseschema}.Sales_iap_hub`
    where (cat_package not in ('젬','루비') or cat_package is null))
    union all
    ### GEM
    (select 'GEM' as idx, logdate_kst, datetime(logtime_kst) as logtime_kst, auth_account_name, authaccountregdatekst
    , case when action_category_name = 'payment' then package_name else action_name end as package_name
    , case when action_category_name = 'payment' then cat_shop else 'contents' end as cat_shop
    , case when action_category_name = 'payment' then cat_package else 'contents' end as cat_package
    , package_kind
    , cast(price_sheet as int64)*(1500/40) as price_sheet
    , (usegem*(1500/40)) as sales_usegem, (buygem*(1500/40)) as sales_buygem
    from `data-science-division-216308.gameInsightFramework.sales_goods`  where is_tester=0
    and joyple_game_code = {joyplegameid}
    and goods_name='gem' and add_or_spend = 'spend')
    union all
    ### RUBY
    (select 'RUBY' as idx, logdate_kst, datetime(logtime_kst) as logtime_kst, auth_account_name, authaccountregdatekst
    , case when action_category_name = 'payment' then package_name else action_name end as package_name
    , case when action_category_name = 'payment' then cat_shop else 'contents' end as cat_shop
    , case when action_category_name = 'payment' then cat_package else 'contents' end as cat_package
    , package_kind, cast(price_sheet as int64)*(15000/999) as price_sheet
    , (usegem*(15000/999)) as sales_usegem, (buygem*(15000/999)) as sales_buygem
    from `data-science-division-216308.gameInsightFramework.sales_goods`  where is_tester=0
    and joyple_game_code = {joyplegameid}
    and goods_name='ruby' and add_or_spend = 'spend')

    )
    where logdate_kst>= DATE_SUB(DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH), INTERVAL 1 MONTH)
    and logdate_kst<=LAST_DAY(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
    ),

    sales_raw as ( ## 5331039
    select *  , format_date('%Y-%m',  logdatekst ) as month
    from `dataplatform-reporting.DataService.V_0317_0000_AuthAccountPerformance_V`
    where joyplegameid = {joyplegameid}
    and logdatekst>='2025-01-01'
    ),


    monthly_rev as (
    select authaccountname, logmonth, regmonth, ifnull(sum(pricekrw),0) as rev
    from
    (select *
    , format_date('%Y-%m-01',  logdatekst ) as logmonth
    , format_date('%Y-%m',  AuthAccountRegDateKST ) as regmonth
    from sales_raw
    where logdatekst>='2025-01-01')
    group by 1,2,3
    ),

    r_group as (
    select *
    , case
    when rev>=10000000 then 'R0'
    when rev>=1000000  then 'R1'
    when rev>=100000   then 'R2'
    when rev>=10000    then 'R3'
    when rev>=1        then 'R4'
    # when rev=0         then 'nonPU'
    else 'ETC' end as rgroup
    from monthly_rev
    where rev>0
    ),

    raw2 as (
    select a.*,month_key
    , case
    when a.month2 <= a.regmonth then '당월가입자'
    when b.rgroup is null then '전월 무과금'
    else b.rgroup end as rgroup_final

    from
    (select * , format_date('%Y-%m',  week_start ) as month2
    from raw) as a

    left join
    (select * , format_date('%Y-%m', date_add(date(logmonth), interval 1 month )) as month_key
    from r_group
    ) as b

    on a.authaccountname = b.authaccountname
    and a.month2 = b.month_key  ## 주차 시작일과 조인
    ),

    raw3 as (

    select *
    , row_number() OVER (partition by week, rgroup_final ORDER BY PU desc, sales desc  ) AS pu_rank
    , row_number() OVER (partition by week, rgroup_final ORDER BY sales desc, PU desc ) AS sales_rank
    from
    (select week, rgroup_final, package_name, cat_shop as shop_category, cat_package2 as package_category, price_sheet
    , count(distinct authaccountname) as PU
    , cast(sum(sales_buygem) as int64) as sales
    from raw2
    where logdate_kst between week_start and DATE_ADD(week_start, INTERVAL 6 DAY) ## 이번주 필터(수요일부터 화요일)
    and sales_buygem>0 ## 유가젬 사용만
    group by 1,2,3,4,5,6)
    )

    select *
    from raw3
    where rgroup_final is not null
    and sales_rank in (1,2,3)
    order by rgroup_final, sales_rank
    """

    query_result = query_run_method('4_detail_sales', query)

    context['task_instance'].xcom_push(key='rgroup_top3_rev', value=query_result)

    return True


def rgroup_top3_gemini(service_sub: str, **context):
    query_result4_thisWeekSalesTop3 = context['task_instance'].xcom_pull(
        task_ids = 'rgroup_top3_rev',
        key='rgroup_top3_rev'
    )

    query_result4_thisWeekPUTop3 = context['task_instance'].xcom_pull(
        task_ids = 'rgroup_top3_pu',
        key='rgroup_top3_pu'
    )

    RUN_ID = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    LABELS = {"datascience_division_service": 'gameinsight_framework',
            "run_id": RUN_ID,
            f"datascience_division_service_sub" : {service_sub}}


    response4_thisWeekRgroup = genai_client.models.generate_content(
    model=MODEL_NAME,
    contents = f"""

    과금그룹 정의는 다음과 같아.
    R0 : 전월 과금액 1천만원 이상
    R1 : 전월 과금액 1천만원 미만 ~ 1백만원 이상
    R2 : 전월 과금액 1백만원 미만 ~ 10만원 이상
    R3 : 전월 과금액 10만원 미만 ~ 1만원 이상
    R4 : 전월 과금액 1만원 미만 ~ 0원 초과
    전월 무과금 : 전월 무과금 유저
    당월가입자 : 이번달에 가입한 유저

    이번주 R그룹별 PU top3 , 매출 top3 상품들 정보를 줄게
    상위 과금그룹과 하위과금그룹 간의 차이에대해서만 간단히 요약해줘

    < 서식 요구사항 >
    1. 한문장당 줄바꿈 한번 해줘.
    2. 매출 1원단위까지 다 쓰지 말고 대략 말해줘.
    3. 한 문장마다 노션의 마크다운 리스트 문법을 사용해줘. e.g. * 당월 매출은 이렇습니다.
    4. 매출액 잘 확인해줘. 1억인데 10억이라고 쓰지마


    < R그룹별 PU top3 상품>
    {query_result4_thisWeekPUTop3}


    < R그룹별 매출 top3 상품>
    {query_result4_thisWeekSalesTop3}



    """,

    ### 이전버전 프롬프트 ###
    # contents - f"""
    # 이번주 상품군별 매출액에서 특별한 점을 알려줘.
    # 히스토리가 있으면 참고해주고 없으면 아예 히스토리에 대해 아무 언급하지말아줘

    # 1. 한문장당 줄바꿈 한번 해줘.
    # 2. 매출 1원단위까지 다 쓰지 말고 대략 말해줘.
    # 3. 한 문장마다 노션의 마크다운 리스트 문법을 사용해줘. e.g. * 당월 매출은 이렇습니다.


    # <일자별 상품군별 매출액>
    # {query_result4_salesByPackage}
    # <히스토리>
    # {query_result4_ingameHistory}
    # """

    config=types.GenerateContentConfig(
            system_instruction=SYSTEM_INSTRUCTION,
            # tools=[RAG],
            temperature=0.5
            ,labels=LABELS
        )
    )

    # 코멘트 출력
    return response4_thisWeekRgroup.text



def category_for_bigquery_sql(service_sub:str, **context):

    _, _, _, response4_CategoryListUp = ruby_df_gemini(service_sub, **context)

    CategoryListUp = re.search(r"\(.*\)", response4_CategoryListUp.text)
    if CategoryListUp:
        # 문자열을 실제 tuple/list로 변환 (안전)
        CategoryListUp_2 = eval(CategoryListUp.group(0))

        # 앞 3개만 추출
        CategoryListUp_Top3 = list(CategoryListUp_2)[:3]

        # SQL용 문자열로 변환: '배틀패스', '군함', '전투기'
        CategoryListUp_SQL = ", ".join([f"'{c}'" for c in CategoryListUp_Top3])

        # SQL ORDER BY용 CASE WHEN ... THEN ...
        case_when_str = "\n".join(
            [f"WHEN '{c}' THEN {i+1}" for i, c in enumerate(CategoryListUp_Top3)]
        )
    return CategoryListUp_SQL, case_when_str, CategoryListUp_Top3


def top3_items_rev(joyplegameid:int, gameidx:str, databaseschema:str, service_sub:str, **context):
    
    CategoryListUp_SQL, case_when_str, _ = category_for_bigquery_sql(service_sub=service_sub)

    query = f"""
    with sales_data as (
    select `일자`
        , case when rnum <= 5 then `상품결제 재화` else null end as `상품결제 재화`
        ,`상품 카테고리`
        , case when rnum <= 5 then `상품` else '그 외 상품들' end as `상품 이름`
        , `매출`
    from (
        select logdate_kst as `일자`
        , idx as `상품결제 재화`
        , cat_package2 as `상품 카테고리`
        , package_name as `상품`
        , sum(sales_buygem) as `매출`
        , row_number() over(partition by cat_package2, logdate_kst order by sum(sales_buygem) desc) as rnum
        from(
            select *
            , format_date('%Y-%m',  logdate_kst ) as month

            , case when CountryCode = 'KR' then '1.KR' when CountryCode = 'US' then '2.US' else '3.ETC' end as CountryCat
            , concat(cast(cast(DATE_TRUNC(logdate_kst ,week(Wednesday)) as date) as string),' ~ ',
                        cast(date_add(cast(DATE_TRUNC(logdate_kst,week(Wednesday)) as date), interval 6 day) as string)) as logweek
            , case when cat_shop = '배틀패스' then '배틀패스'
                when cat_package not in ('전투기','종합','자원','항공모함','영웅','군함','루비','배틀패스','연구','장비') then '기타'
                        else cat_package end as cat_package2
            from (
                (
                    select 'IAP' As idx, logdate_kst, datetime(logtime_kst) as logtime_kst, authaccountname, authaccountregdatekst, CountryCode
                        , package_name, cat_shop, cat_package, cast(package_kind as string) as package_kind
                        , cast(price_sheet as int64) as price_sheet
                        , pricekrw as sales_usegem, pricekrw as sales_buygem
                from `data-science-division-216308.{databaseschema}.Sales_iap_hub`
                where (cat_package not in ('젬','루비') or cat_package is null)
                        and logdate_kst >= DATE_SUB(
                                        date_add(current_date('Asia/Seoul'),interval -1 day),
                                        INTERVAL MOD(EXTRACT(DAYOFWEEK FROM date_add(current_date('Asia/Seoul'),interval -1 day)) - 4 + 7, 7) DAY
                                        )
                AND logdate_kst <= DATE_SUB(CURRENT_DATE("Asia/Seoul"), INTERVAL 1 DAY)
                )

                union all

                (
                select 'GEM' as idx, logdate_kst, datetime(logtime_kst) as logtime_kst, auth_account_name, authaccountregdatekst, CountryCode
                        , case when action_category_name = 'payment' then package_name else action_name end as package_name
                        , case when action_category_name = 'payment' then cat_shop else 'contents' end as cat_shop
                        , case when action_category_name = 'payment' then cat_package else 'contents' end as cat_package
                        , package_kind
                        , cast(price_sheet as int64)*(1500/40) as price_sheet
                        , (usegem*(1500/40)) as sales_usegem, (buygem*(1500/40)) as sales_buygem
                from `data-science-division-216308.gameInsightFramework.sales_goods`  where is_tester=0
                and joyple_game_code = {joyplegameid}
                and goods_name='gem' and add_or_spend = 'spend'
                        and logdate_kst >= DATE_SUB(
                                        date_add(current_date('Asia/Seoul'),interval -1 day),
                                        INTERVAL MOD(EXTRACT(DAYOFWEEK FROM date_add(current_date('Asia/Seoul'),interval -1 day)) - 4 + 7, 7) DAY
                                        )
                AND logdate_kst <= DATE_SUB(CURRENT_DATE("Asia/Seoul"), INTERVAL 1 DAY)
                )

                union all

                (
                select 'RUBY' as idx, logdate_kst, datetime(logtime_kst) as logtime_kst, auth_account_name, authaccountregdatekst, CountryCode
                        , case when action_category_name = 'payment' then package_name else action_name end as package_name
                        , case when action_category_name = 'payment' then cat_shop else 'contents' end as cat_shop
                        , case when action_category_name = 'payment' then cat_package else 'contents' end as cat_package
                        , package_kind, cast(price_sheet as int64)*(15000/999) as price_sheet
                        , (usegem*(15000/999)) as sales_usegem, (buygem*(15000/999)) as sales_buygem
                from `data-science-division-216308.gameInsightFramework.sales_goods`  where is_tester=0
                and joyple_game_code = {joyplegameid}
                and goods_name='ruby' and add_or_spend = 'spend'
                        and logdate_kst >= DATE_SUB(
                                        date_add(current_date('Asia/Seoul'),interval -1 day),
                                        INTERVAL MOD(EXTRACT(DAYOFWEEK FROM date_add(current_date('Asia/Seoul'),interval -1 day)) - 4 + 7, 7) DAY
                                        )
                AND logdate_kst <= DATE_SUB(CURRENT_DATE("Asia/Seoul"), INTERVAL 1 DAY)
                )
            )
            )
    where cat_package2 in  ({CategoryListUp_SQL})
        group by 1,2,3,4
        )
    )

    select `일자`, `상품 카테고리`, `상품 이름`, sum(`매출`) as `매출`
    from sales_data
    group by `일자`, `상품 카테고리`, `상품 이름`
    order by `일자`,
            CASE `상품 카테고리`
            {case_when_str}
            ELSE 99
            END,
            case when `상품 이름` = '그 외 상품들' then 1 else 0 end,
            `매출` desc
    """
    query_result = query_run_method('4_detail_sales', query)
    query_result['매출'] = query_result['매출'].map(lambda x: f"{int(x)}")
    
    context['task_instance'].xcom_push(key='top3_items_rev', value=query_result)


    cats = [re.sub(r"^[\"'’‘`]+|[\"'’‘`]+$", "", t.strip())
        for t in CategoryListUp_SQL.split(",") if t.strip()]

    # 2) 순서대로 필터링해서 새 DF 생성
    category_col = "상품 카테고리"

    dfs = {}  # 사전으로 보관: {"query_result4_salesByPackage_forGraph_1": df1, ...}
    for i, c in enumerate(cats, start=1):
        key = f"query_result4_salesByPackage_forCategoryGraph_{i}"
        dfs[key] = query_result[
            query_result[category_col] == c
        ].copy()

    return dfs


def rgroup_rev_draw(gameidx: str, **context):
    ## 해당 데이터프레임에는 매출, PU 둘다 있어서, 매출까지만 필터링
    query_result4_RgroupSales = context['task_instance'].xcom_pull(
        task_ids = 'rev_group_rev_pu',
        key='rev_group_rev_pu'
    )
    query_result4_RgroupSales2_salesGraph = query_result4_RgroupSales.iloc[:, [0,2,3,4,5,6,7,8]]

    ##
    query_result4_RgroupSales2_salesGraph = query_result4_RgroupSales2_salesGraph.rename(
        columns = {"R0_Sales" : "R0",
                "R1_Sales" : "R1",
                "R2_Sales" : "R2",
                "R3_Sales" : "R3",
                "R4_Sales" : "R4",
                "전월 무과금_Sales" : "전월 무과금",
                "당월가입자_Sales" : "당월가입자"}
    )


    # ⬇️ 가로폭 넓히기: width=20인치(원하는 만큼 키우세요), height=6인치
    fig, ax = plt.subplots(figsize=(12, 6))

    x = query_result4_RgroupSales2_salesGraph["logdatekst"]
    y = query_result4_RgroupSales2_salesGraph.iloc[:, 1:]

    # 누적 막대 bottom은 넘파이로 (리스트 + 시리즈 더하기 오류 방지)
    bottom = np.zeros(len(query_result4_RgroupSales2_salesGraph), dtype=float)

    for col in y.columns:
        ax.bar(x, y[col], bottom=bottom, label=col)
        bottom += y[col].to_numpy()

    # y축 천단위
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{int(v):,}"))

    # 여백 제거
    ax.margins(x=0)

    # x축 매일
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

    # x축 라벨/눈금
    ax.set_title(" R그룹별 매출 ")
    ax.tick_params(axis='x', labelsize=9, pad=2)
    plt.xticks(rotation=90)

    # 범례를 밖으로, 잘림 방지
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)

    ax.grid(axis="y", linestyle="--", alpha=0.7)

    fig.tight_layout()
    file_path4_RgroupSales_salesGraph = "graph4_RgroupSales_salesGraph.png"
    
    # ⬇️ 잘림 방지용 bbox_inches
    plt.savefig(file_path4_RgroupSales_salesGraph, dpi=160, bbox_inches='tight') # dpi : 해상도
    plt.close()

    blob = bucket.blob(f'{gameidx}/{file_path4_RgroupSales_salesGraph}')
    blob.upload_from_filename(file_path4_RgroupSales_salesGraph)

    # 메모리에 올라간 이미지 파일 삭제
    os.remove(file_path4_RgroupSales_salesGraph)

    return f'{gameidx}/{file_path4_RgroupSales_salesGraph}'


def rgroup_pu_draw(gameidx: str, **context):
    
    query_result4_RgroupSales = context['task_instance'].xcom_pull(
        task_ids = 'rev_group_rev_pu',
        key='rev_group_rev_pu'
    )

    ## 해당 데이터프레임에는 매출, PU 둘다 있어서, 매출까지만 필터링
    query_result4_RgroupSales2_puGraph = query_result4_RgroupSales.iloc[:, [0,10,11,12,13,14,15,16]]

    ##
    query_result4_RgroupSales2_puGraph = query_result4_RgroupSales2_puGraph.rename(
        columns = {"R0_PU" : "R0",
                "R1_PU" : "R1",
                "R2_PU" : "R2",
                "R3_PU" : "R3",
                "R4_PU" : "R4",
                "전월 무과금_PU" : "전월 무과금",
                "당월가입자_PU" : "당월가입자"}
    )

    # ⬇️ 가로폭 넓히기: width=20인치(원하는 만큼 키우세요), height=6인치
    fig, ax = plt.subplots(figsize=(12, 6))

    x = query_result4_RgroupSales2_puGraph["logdatekst"]
    y = query_result4_RgroupSales2_puGraph.iloc[:, 1:]

    # 누적 막대 bottom은 넘파이로 (리스트 + 시리즈 더하기 오류 방지)
    bottom = np.zeros(len(query_result4_RgroupSales2_puGraph), dtype=float)

    for col in y.columns:
        ax.bar(x, y[col], bottom=bottom, label=col)
        bottom += y[col].to_numpy()

    # y축 천단위
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{int(v):,}"))

    # 여백 제거
    ax.margins(x=0)

    # x축 매일
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

    # x축 라벨/눈금
    ax.set_title(" (R그룹별 PU 수 ")
    ax.tick_params(axis='x', labelsize=9, pad=2)
    plt.xticks(rotation=90)

    # 범례를 밖으로, 잘림 방지
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)

    ax.grid(axis="y", linestyle="--", alpha=0.7)

    fig.tight_layout()
    file_path4_RgroupSales_puGraph = "graph4_RgroupSales_puGraph.png"

    # ⬇️ 잘림 방지용 bbox_inches
    fig.savefig(file_path4_RgroupSales_puGraph, dpi=160, bbox_inches='tight')
    plt.close(fig)

    blob = bucket.blob(f'{gameidx}/{file_path4_RgroupSales_puGraph}')
    blob.upload_from_filename(file_path4_RgroupSales_puGraph)

    # 메모리에 올라간 이미지 파일 삭제
    os.remove(file_path4_RgroupSales_puGraph)

    return f'{gameidx}/{file_path4_RgroupSales_puGraph}'


def merge_rgroup_graph(gameidx: str):
    p1 = rgroup_rev_draw(gameidx)
    p2 = rgroup_pu_draw(gameidx)

    # 2) 이미지 열기 (투명 보존 위해 RGBA)
    blob1 = bucket.blob(p1)
    blob2 = bucket.blob(p2)

    im1 = blob1.download_as_bytes()
    im2 = blob2.download_as_bytes()

    im1 = Image.open(BytesIO(im1)).convert("RGBA")
    im2 = Image.open(BytesIO(im2)).convert("RGBA")


    # ---- [옵션 A] 원본 크기 유지 + 세로 패딩으로 높이 맞추기 (권장: 왜곡 없음) ----
    target_h = max(im1.height, im2.height)

    def pad_to_height(img, h, bg=(255, 255, 255, 0)):  # 투명 배경: 알파 0
        if img.height == h:
            return img
        canvas = Image.new("RGBA", (img.width, h), bg)
        # 가운데 정렬로 붙이기 (위에 맞추려면 y=0)
        y = (h - img.height) // 2
        canvas.paste(img, (0, y))
        return canvas

    im1_p = pad_to_height(im1, target_h)
    im2_p = pad_to_height(im2, target_h)

    gap = 0  # 이미지 사이 여백(px). 필요하면 20 등으로 변경
    bg = (255, 255, 255, 0)  # 전체 배경(투명). 흰색 원하면 (255,255,255,255)

    out = Image.new("RGBA", (im1_p.width + gap + im2_p.width, target_h), bg)
    out.paste(im1_p, (0, 0), im1_p)
    out.paste(im2_p, (im1_p.width + gap, 0), im2_p)

    # 3) GCS에 저장
    output_buffer = BytesIO()
    out.save(output_buffer, format='PNG')
    output_buffer.seek(0)

    # GCS 경로
    gcs_path = f'{gameidx}/graph4_RgroupSales.png'
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(output_buffer.getvalue(), content_type='image/png')

    return gcs_path



def iap_gem_ruby_graph_draw(gameidx:str, **context):

    query_result4_salesByPackage = context['task_instance'].xcom_pull(
        task_ids = 'iap_gem_ruby',
        key='iap_gem_ruby'
        )

    query_result4_salesByPackage_salesGraph = query_result4_salesByPackage.iloc[:, [0,2,3,4,5,6,7,8,9,10,11]]

    # ⬇️ 가로폭 넓히기: width=20인치(원하는 만큼 키우세요), height=6인치
    fig, ax = plt.subplots(figsize=(20, 6))

    x = query_result4_salesByPackage_salesGraph["logdate_kst"]
    y = query_result4_salesByPackage_salesGraph.iloc[:, 1:]

    # 누적 막대 bottom은 넘파이로 (리스트 + 시리즈 더하기 오류 방지)
    bottom = np.zeros(len(query_result4_salesByPackage_salesGraph), dtype=float)

    for col in y.columns:
        ax.bar(x, y[col], bottom=bottom, label=col)
        bottom += y[col].to_numpy()

    # y축 천단위
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{int(v):,}"))

    # 여백 제거
    ax.margins(x=0)

    # x축 매일
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

    # x축 라벨/눈금
    ax.set_title("(IAP+유가젬+유가루비) 일자별 상품별 매출")
    ax.tick_params(axis='x', labelsize=9, pad=2)
    plt.xticks(rotation=90)

    # 범례를 밖으로, 잘림 방지
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)

    ax.grid(axis="y", linestyle="--", alpha=0.7)

    fig.tight_layout()
    file_path4_salesByPackage = "graph4_salesByPackage.png"

    # ⬇️ 잘림 방지용 bbox_inches
    fig.savefig(file_path4_salesByPackage, dpi=160, bbox_inches='tight')
    plt.close(fig)

    blob = bucket.blob(f'{gameidx}/{file_path4_salesByPackage}')
    blob.upload_from_filename(file_path4_salesByPackage)

    # 메모리에 올라간 이미지 파일 삭제
    os.remove(file_path4_salesByPackage)

    return f'{gameidx}/{file_path4_salesByPackage}'



def iap_gem_ruby_IAP_graph_draw(gameidx:str, **context):
    
    query_result4_salesByPackage_IAP = context['task_instance'].xcom_pull(
    task_ids = 'iap_df',
    key='iap_df'
    )

    # ... (위 데이터 준비·폰트 부분 동일)
    query_result4_salesByPackage_IAP_salesGraph = query_result4_salesByPackage_IAP.iloc[:, (query_result4_salesByPackage_IAP.columns != 'month') & (query_result4_salesByPackage_IAP.columns != 'week')]


    # ⬇️ 가로폭 넓히기: width=20인치(원하는 만큼 키우세요), height=6인치
    fig, ax = plt.subplots(figsize=(20, 6))

    x = query_result4_salesByPackage_IAP_salesGraph["logdate_kst"]
    y = query_result4_salesByPackage_IAP_salesGraph.iloc[:, 1:]

    # 누적 막대 bottom은 넘파이로 (리스트 + 시리즈 더하기 오류 방지)
    bottom = np.zeros(len(query_result4_salesByPackage_IAP_salesGraph), dtype=float)

    for col in y.columns:
        ax.bar(x, y[col], bottom=bottom, label=col)
        bottom += y[col].to_numpy()

    # y축 천단위
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{int(v):,}"))

    # 여백 제거
    ax.margins(x=0)

    # x축 매일
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

    # x축 라벨/눈금
    ax.set_title("(IAP) 일자별 상품별 매출")
    ax.tick_params(axis='x', labelsize=9, pad=2)
    plt.xticks(rotation=90)

    # 범례를 밖으로, 잘림 방지
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)

    ax.grid(axis="y", linestyle="--", alpha=0.7)

    fig.tight_layout()
    file_path4_salesByPackage_IAP = "graph4_salesByPackage_IAP.png"

    # ⬇️ 잘림 방지용 bbox_inches
    fig.savefig(file_path4_salesByPackage_IAP, dpi=160, bbox_inches='tight')
    plt.close(fig)

    blob = bucket.blob(f'{gameidx}/{file_path4_salesByPackage_IAP}')
    blob.upload_from_filename(file_path4_salesByPackage_IAP)

    # 메모리에 올라간 이미지 파일 삭제
    os.remove(file_path4_salesByPackage_IAP)

    return f'{gameidx}/{file_path4_salesByPackage_IAP}'


def iap_gem_ruby_GEM_graph_draw(gameidx:str, **context):

    query_result4_salesByPackage_GEM = context['task_instance'].xcom_pull(
    task_ids = 'gem_df',
    key='gem_df'
    )
    
    # ... (위 데이터 준비·폰트 부분 동일)
    query_result4_salesByPackage_GEM_salesGraph = query_result4_salesByPackage_GEM.iloc[:, (query_result4_salesByPackage_GEM.columns != 'month') & (query_result4_salesByPackage_GEM.columns != 'week')]


    # ⬇️ 가로폭 넓히기: width=20인치(원하는 만큼 키우세요), height=6인치
    fig, ax = plt.subplots(figsize=(20, 6))

    x = query_result4_salesByPackage_GEM_salesGraph["logdate_kst"]
    y = query_result4_salesByPackage_GEM_salesGraph.iloc[:, 1:]

    # 누적 막대 bottom은 넘파이로 (리스트 + 시리즈 더하기 오류 방지)
    bottom = np.zeros(len(query_result4_salesByPackage_GEM_salesGraph), dtype=float)

    for col in y.columns:
        ax.bar(x, y[col], bottom=bottom, label=col)
        bottom += y[col].to_numpy()

    # y축 천단위
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{int(v):,}"))

    # 여백 제거
    ax.margins(x=0)

    # x축 매일
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

    # x축 라벨/눈금
    ax.set_title("(젬) 일자별 상품별 매출")
    ax.tick_params(axis='x', labelsize=9, pad=2)
    plt.xticks(rotation=90)

    # 범례를 밖으로, 잘림 방지
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)

    ax.grid(axis="y", linestyle="--", alpha=0.7)

    fig.tight_layout()
    file_path4_salesByPackage_GEM = "graph4_salesByPackage_GEM.png"

    # ⬇️ 잘림 방지용 bbox_inches
    fig.savefig(file_path4_salesByPackage_GEM, dpi=160, bbox_inches='tight')
    plt.close(fig)

    blob = bucket.blob(f'{gameidx}/{file_path4_salesByPackage_GEM}')
    blob.upload_from_filename(file_path4_salesByPackage_GEM)

    # 메모리에 올라간 이미지 파일 삭제
    os.remove(file_path4_salesByPackage_GEM)

    return f'{gameidx}/{file_path4_salesByPackage_GEM}'
    


def iap_gem_ruby_RUBY_graph_draw(gameidx:str, **context):

    query_result4_salesByPackage_RUBY = context['task_instance'].xcom_pull(
    task_ids = 'ruby_df',
    key='ruby_df'
    )

    query_result4_salesByPackage_RUBY_salesGraph = query_result4_salesByPackage_RUBY.iloc[:, (query_result4_salesByPackage_RUBY.columns != 'month') & (query_result4_salesByPackage_RUBY.columns != 'week')]


    # ⬇️ 가로폭 넓히기: width=20인치(원하는 만큼 키우세요), height=6인치
    fig, ax = plt.subplots(figsize=(20, 6))

    x = query_result4_salesByPackage_RUBY_salesGraph["logdate_kst"]
    y = query_result4_salesByPackage_RUBY_salesGraph.iloc[:, 1:]

    # 누적 막대 bottom은 넘파이로 (리스트 + 시리즈 더하기 오류 방지)
    bottom = np.zeros(len(query_result4_salesByPackage_RUBY_salesGraph), dtype=float)

    for col in y.columns:
        ax.bar(x, y[col], bottom=bottom, label=col)
        bottom += y[col].to_numpy()

    # y축 천단위
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{int(v):,}"))

    # 여백 제거
    ax.margins(x=0)

    # x축 매일
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

    # x축 라벨/눈금
    ax.set_title("(루비) 일자별 상품별 매출")
    ax.tick_params(axis='x', labelsize=9, pad=2)
    plt.xticks(rotation=90)

    # 범례를 밖으로, 잘림 방지
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)

    ax.grid(axis="y", linestyle="--", alpha=0.7)

    fig.tight_layout()
    file_path4_salesByPackage_RUBY = "graph4_salesByPackage_RUBY.png"

    # ⬇️ 잘림 방지용 bbox_inches
    fig.savefig(file_path4_salesByPackage_RUBY, dpi=160, bbox_inches='tight')
    plt.close(fig)

    blob = bucket.blob(f'{gameidx}/{file_path4_salesByPackage_RUBY}')
    blob.upload_from_filename(file_path4_salesByPackage_RUBY)

    # 메모리에 올라간 이미지 파일 삭제
    os.remove(file_path4_salesByPackage_RUBY)

    return f'{gameidx}/{file_path4_salesByPackage_RUBY}'


### 1위
def top1_graph_draw(joyplegameid: int, gameidx: str, databaseschema: str, service_sub: str, **context):

    dfs = top3_items_rev(joyplegameid, gameidx, databaseschema, service_sub, **context)

    df = dfs.get("query_result4_salesByPackage_forCategoryGraph_1")
    df["일자"] = pd.to_datetime(df["일자"])
    df["매출"] = pd.to_numeric(df["매출"], errors="coerce").fillna(0).astype("int64")

    # 3) 집계 → 피벗
    g = df.groupby(["일자", "상품 이름"], as_index=False)["매출"].sum()
    wide = g.pivot(index="일자", columns="상품 이름", values="매출").fillna(0)

    # 4) 상위 N개(원문대로 None이면 전체)
    top_n = None
    if top_n is not None:
        top_items = wide.sum(axis=0).sort_values(ascending=False).head(top_n).index
        wide = wide[top_items]

    # --- 방법 1) 적용: 컬럼명 정규화 + 색상 매핑 고정 -----------------------

    def norm_label(s: str) -> str:
        # 양끝 공백/여러 형태의 따옴표 제거
        return re.sub(r'^[\'"\s`’‘]+|[\'"\s`’‘]+$', '', str(s).strip())

    # 컬럼 정규화
    cols_norm = [norm_label(c) for c in wide.columns]
    wide.columns = cols_norm

    # 색상 팔레트 구성
    n = len(cols_norm)
    cmap = plt.get_cmap('tab20', n) if n > 0 else None
    color_map = {col: cmap(i) for i, col in enumerate(cols_norm)} if n > 0 else {}

    # ‘그 외 상품들’을 밝은 회색으로 강제
    DARK_GRAY = "#525252"
    color_map["그 외 상품들"] = DARK_GRAY

    # ----------------------------------------------------------------------

    # 5) 누적 막대
    fig, ax = plt.subplots(figsize=(20, 6))
    x = wide.index
    bottom = np.zeros(len(x), dtype=float)

    for col in wide.columns:
        vals = wide[col].to_numpy()
        ax.bar(x, vals, bottom=bottom, color=color_map.get(col), label=col)
        bottom += vals

    # 6) 포맷팅
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{int(v):,}"))
    ax.margins(x=0)
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.xticks(rotation=90)
    ax.grid(axis="y", linestyle="--", alpha=0.7)

    # 제목 (카테고리명 반영)
    _, _, CategoryListUp_Top3 = category_for_bigquery_sql(service_sub=service_sub)

    title_cat = str(CategoryListUp_Top3[0]).strip().strip("'\"`’‘") if CategoryListUp_Top3 else "" # CategoryListUp_Top3[] 부분 수정
    ax.set_title(f"{title_cat} 일자별 {'상위'+str(top_n)+'개 ' if top_n else ''}상품 매출")

    # 7) 범례 중복 제거 후 표시
    handles, labels = ax.get_legend_handles_labels()
    seen = set()
    uniq_h, uniq_l = [], []
    for h, l in zip(handles, labels):
        if l and l not in seen and not l.startswith("_"):
            uniq_h.append(h); uniq_l.append(l); seen.add(l)

    if uniq_l:
        ax.legend(uniq_h, uniq_l, bbox_to_anchor=(1.02, 1), loc='upper left', borderaxespad=0.)

    fig.tight_layout()
    salesByPackage_Category1 = "graph4_salesByPackage_Category1.png"

    # ⬇️ 잘림 방지용 bbox_inches
    fig.savefig(salesByPackage_Category1, dpi=160, bbox_inches='tight')
    plt.close(fig)

    blob = bucket.blob(f'{gameidx}/{salesByPackage_Category1}')
    blob.upload_from_filename(salesByPackage_Category1)

    # 메모리에 올라간 이미지 파일 삭제
    os.remove(salesByPackage_Category1)

    return f'{gameidx}/{salesByPackage_Category1}'


### 2위
def top2_graph_draw(joyplegameid: int, gameidx: str, databaseschema: str, service_sub: str, **context):

    dfs = top3_items_rev(joyplegameid, gameidx, databaseschema, service_sub, **context)

    df = dfs.get("query_result4_salesByPackage_forCategoryGraph_1")
    df["일자"] = pd.to_datetime(df["일자"])
    df["매출"] = pd.to_numeric(df["매출"], errors="coerce").fillna(0).astype("int64")

    # 3) 집계 → 피벗
    g = df.groupby(["일자", "상품 이름"], as_index=False)["매출"].sum()
    wide = g.pivot(index="일자", columns="상품 이름", values="매출").fillna(0)

    # 4) 상위 N개(원문대로 None이면 전체)
    top_n = None
    if top_n is not None:
        top_items = wide.sum(axis=0).sort_values(ascending=False).head(top_n).index
        wide = wide[top_items]

    # --- 방법 1) 적용: 컬럼명 정규화 + 색상 매핑 고정 -----------------------

    def norm_label(s: str) -> str:
        # 양끝 공백/여러 형태의 따옴표 제거
        return re.sub(r'^[\'"\s`’‘]+|[\'"\s`’‘]+$', '', str(s).strip())

    # 컬럼 정규화
    cols_norm = [norm_label(c) for c in wide.columns]
    wide.columns = cols_norm

    # 색상 팔레트 구성
    n = len(cols_norm)
    cmap = plt.get_cmap('tab20', n) if n > 0 else None
    color_map = {col: cmap(i) for i, col in enumerate(cols_norm)} if n > 0 else {}

    # ‘그 외 상품들’을 밝은 회색으로 강제
    DARK_GRAY = "#525252"
    color_map["그 외 상품들"] = DARK_GRAY

    # ----------------------------------------------------------------------

    # 5) 누적 막대
    fig, ax = plt.subplots(figsize=(20, 6))
    x = wide.index
    bottom = np.zeros(len(x), dtype=float)

    for col in wide.columns:
        vals = wide[col].to_numpy()
        ax.bar(x, vals, bottom=bottom, color=color_map.get(col), label=col)
        bottom += vals

    # 6) 포맷팅
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{int(v):,}"))
    ax.margins(x=0)
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.xticks(rotation=90)
    ax.grid(axis="y", linestyle="--", alpha=0.7)

    # 제목 (카테고리명 반영)
    _, _, CategoryListUp_Top3 = category_for_bigquery_sql(service_sub=service_sub)

    title_cat = str(CategoryListUp_Top3[0]).strip().strip("'\"`’‘") if CategoryListUp_Top3 else "" # CategoryListUp_Top3[] 부분 수정
    ax.set_title(f"{title_cat} 일자별 {'상위'+str(top_n)+'개 ' if top_n else ''}상품 매출")

    # 7) 범례 중복 제거 후 표시
    handles, labels = ax.get_legend_handles_labels()
    seen = set()
    uniq_h, uniq_l = [], []
    for h, l in zip(handles, labels):
        if l and l not in seen and not l.startswith("_"):
            uniq_h.append(h); uniq_l.append(l); seen.add(l)

    if uniq_l:
        ax.legend(uniq_h, uniq_l, bbox_to_anchor=(1.02, 1), loc='upper left', borderaxespad=0.)

    fig.tight_layout()
    salesByPackage_Category2 = "graph4_salesByPackage_Category2.png"

        # ⬇️ 잘림 방지용 bbox_inches
    fig.savefig(salesByPackage_Category2, dpi=160, bbox_inches='tight')
    plt.close(fig)

    blob = bucket.blob(f'{gameidx}/{salesByPackage_Category2}')
    blob.upload_from_filename(salesByPackage_Category2)

    # 메모리에 올라간 이미지 파일 삭제
    os.remove(salesByPackage_Category2)

    return f'{gameidx}/{salesByPackage_Category2}'



### 3위
def top3_graph_draw(joyplegameid: int, gameidx: str, databaseschema: str, service_sub: str, **context):

    dfs = top3_items_rev(joyplegameid, gameidx, databaseschema, service_sub, **context)

    df = dfs.get("query_result4_salesByPackage_forCategoryGraph_1")
    df["일자"] = pd.to_datetime(df["일자"])
    df["매출"] = pd.to_numeric(df["매출"], errors="coerce").fillna(0).astype("int64")

    # 3) 집계 → 피벗
    g = df.groupby(["일자", "상품 이름"], as_index=False)["매출"].sum()
    wide = g.pivot(index="일자", columns="상품 이름", values="매출").fillna(0)

    # 4) 상위 N개(원문대로 None이면 전체)
    top_n = None
    if top_n is not None:
        top_items = wide.sum(axis=0).sort_values(ascending=False).head(top_n).index
        wide = wide[top_items]

    # --- 방법 1) 적용: 컬럼명 정규화 + 색상 매핑 고정 -----------------------

    def norm_label(s: str) -> str:
        # 양끝 공백/여러 형태의 따옴표 제거
        return re.sub(r'^[\'"\s`’‘]+|[\'"\s`’‘]+$', '', str(s).strip())

    # 컬럼 정규화
    cols_norm = [norm_label(c) for c in wide.columns]
    wide.columns = cols_norm

    # 색상 팔레트 구성
    n = len(cols_norm)
    cmap = plt.get_cmap('tab20', n) if n > 0 else None
    color_map = {col: cmap(i) for i, col in enumerate(cols_norm)} if n > 0 else {}

    # ‘그 외 상품들’을 밝은 회색으로 강제
    DARK_GRAY = "#525252"
    color_map["그 외 상품들"] = DARK_GRAY

    # ----------------------------------------------------------------------

    # 5) 누적 막대
    fig, ax = plt.subplots(figsize=(20, 6))
    x = wide.index
    bottom = np.zeros(len(x), dtype=float)

    for col in wide.columns:
        vals = wide[col].to_numpy()
        ax.bar(x, vals, bottom=bottom, color=color_map.get(col), label=col)
        bottom += vals

    # 6) 포맷팅
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{int(v):,}"))
    ax.margins(x=0)
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.xticks(rotation=90)
    ax.grid(axis="y", linestyle="--", alpha=0.7)

    # 제목 (카테고리명 반영)
    _, _, CategoryListUp_Top3 = category_for_bigquery_sql(service_sub=service_sub)

    title_cat = str(CategoryListUp_Top3[0]).strip().strip("'\"`’‘") if CategoryListUp_Top3 else "" # CategoryListUp_Top3[] 부분 수정
    ax.set_title(f"{title_cat} 일자별 {'상위'+str(top_n)+'개 ' if top_n else ''}상품 매출")

    # 7) 범례 중복 제거 후 표시
    handles, labels = ax.get_legend_handles_labels()
    seen = set()
    uniq_h, uniq_l = [], []
    for h, l in zip(handles, labels):
        if l and l not in seen and not l.startswith("_"):
            uniq_h.append(h); uniq_l.append(l); seen.add(l)

    if uniq_l:
        ax.legend(uniq_h, uniq_l, bbox_to_anchor=(1.02, 1), loc='upper left', borderaxespad=0.)

    fig.tight_layout()
    salesByPackage_Category3 = "graph4_salesByPackage_Category3.png"

        # ⬇️ 잘림 방지용 bbox_inches
    fig.savefig(salesByPackage_Category3, dpi=160, bbox_inches='tight')
    plt.close(fig)

    blob = bucket.blob(f'{gameidx}/{salesByPackage_Category3}')
    blob.upload_from_filename(salesByPackage_Category3)

    # 메모리에 올라간 이미지 파일 삭제
    os.remove(salesByPackage_Category3)

    return f'{gameidx}/{salesByPackage_Category3}'


def rgroup_pu_top3_graph_draw(gameidx:str, **context):

    query_result4_thisWeekPUTop3 = context['task_instance'].xcom_pull(
        task_ids = 'rgroup_top3_pu',
        key='rgroup_top3_pu'
    )

    df = query_result4_thisWeekPUTop3.iloc[:, [1,2,3,4,5,6,8]]
    df = df.rename(
        columns = {"rgroup_final" : "R그룹",
                "pu_rank" : "순위",
                "package_name" : "상품명",
                "shop_category" : "상점 카테고리",
                "package_category" : "상품 카테고리",
                "price_sheet" : "상품 가격",
                "PU" : "PU 수"}
    )
    # 원하는 순서 지정
    new_order = ["R그룹", "순위","상품명", "상점 카테고리", "상품 카테고리", "상품 가격", "PU 수"]

    # df 재정렬
    df = df[new_order]

    # 숫자 포맷
    df["상품 가격"] = df["상품 가격"].map(
        lambda x: f"{int(x):,}" if pd.notna(x) else x
    )

    df["PU 수"] = df["PU 수"].map(lambda x: f"{int(x):,}")

    # ---------- 폭 계산: 상품명 넓게, 정규화는 하되 상품명 가중치 크게 ----------
    cols = df.columns.tolist()
    col_idx_map = {c: i for i, c in enumerate(cols)}

    # 각 열 최대 글자수(헤더/데이터 포함)
    max_lens = []
    for c in cols:
        head_len = len(str(c))
        body_len = max(len(str(v)) for v in df[c]) if len(df) else 0
        max_lens.append(max(head_len, body_len))

    base_w, k = 0.03, 0.035
    widths = base_w + k * np.log1p(np.array(max_lens))

    # ✅ 상품명 열 가중치 크게 (잘림 방지)
    if "상품명" in col_idx_map:
        widths[col_idx_map["상품명"]] *= 2.2   # 필요하면 2.5~3.0까지 올려도 됨

    # 최소/최대 비율 제한 후 정규화(합=1)  — 너무 좁아지지 않게 lower bound 올림
    widths = np.clip(widths, 0.08, 0.70)
    widths = widths / widths.sum()


    # ✅ 전체 가로폭을 텍스트 양에 비례해 확대
    #    (상품명 비중을 조금 더 반영)
    total_chars = sum(max_lens) + max_lens[col_idx_map["상품명"]]
    fig_w = min(20.0, max(12.0, 0.16 * total_chars))  # 12~20인치 사이 동적
    fig_h = 6.0

    fig, ax = plt.subplots(figsize=(fig_w, fig_h))
    ax.axis("off")

    table = ax.table(
        cellText=df.values,
        colLabels=cols,
        colWidths=widths.tolist(),   # 비율(합=1)
        cellLoc="center",
        loc="center"
    )

    # 폰트/스케일
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.20, 1.18)         # x 스케일 살짝 키워 가로 여유 확보

    # 헤더 색
    for c in range(len(cols)):
        table[(0, c)].set_facecolor("#eeeeee")

    # 3줄 블록 A/B
    nrows, ncols = len(df), len(cols)
    color_a, color_b = "#ffffff", "#CBE7F6"
    for r in range(1, nrows+1):
        row_color = color_a if ((r-1)//3) % 2 == 0 else color_b
        for c in range(ncols):
            table[(r, c)].set_facecolor(row_color)

    # 상품명 열만 9pt (겹침 여지 줄임)
    if "상품명" in col_idx_map:
        cidx = col_idx_map["상품명"]
        for r in range(len(df)+1):  # 헤더 포함
            table[(r, cidx)].set_fontsize(9)

    # 좌우 여백 최소화
    for (r, c), cell in table.get_celld().items():
        if hasattr(cell, "PAD"):
            cell.PAD = 0.1

    # ✅ 가능한 경우: 실제 텍스트 폭 기반으로 열 자동 폭 재설정 (matplotlib 버전에 따라 지원)
    if hasattr(table, "auto_set_column_width"):
        try:
            table.auto_set_column_width(col=list(range(ncols)))
        except Exception:
            pass

    #plt.subplots_adjust(left=0.02, right=0.98)
    #plt.tight_layout(pad=0.2)

    file_path4_thisWeekPUTop3 = "graph4_thisWeekPUTop3.png"
        # ⬇️ 잘림 방지용 bbox_inches
    fig.savefig(file_path4_thisWeekPUTop3, dpi=170, bbox_inches='tight')
    plt.close(fig)

    blob = bucket.blob(f'{gameidx}/{file_path4_thisWeekPUTop3}')
    blob.upload_from_filename(file_path4_thisWeekPUTop3)

    # 메모리에 올라간 이미지 파일 삭제
    os.remove(file_path4_thisWeekPUTop3)

    return f'{gameidx}/{file_path4_thisWeekPUTop3}'


def rgroup_rev_top3_graph_draw(gameidx:str, **context):

    query_result4_thisWeekRevTop3 = context['task_instance'].xcom_pull(
        task_ids = 'rgroup_top3_rev',
        key='rgroup_top3_rev'
    )

    df = query_result4_thisWeekRevTop3.iloc[:, [1,2,3,4,5,6,8]]
    df = df.rename(
        columns = {"rgroup_final" : "R그룹",
                "pu_rank" : "순위",
                "package_name" : "상품명",
                "shop_category" : "상점 카테고리",
                "package_category" : "상품 카테고리",
                "price_sheet" : "상품 가격",
                "PU" : "PU 수"}
    )
    # 원하는 순서 지정
    new_order = ["R그룹", "순위","상품명", "상점 카테고리", "상품 카테고리", "상품 가격", "PU 수"]

    # df 재정렬
    df = df[new_order]

    # 숫자 포맷
    df["상품 가격"] = df["상품 가격"].map(
        lambda x: f"{int(x):,}" if pd.notna(x) else x
    )

    df["PU 수"] = df["PU 수"].map(lambda x: f"{int(x):,}")

    # ---------- 폭 계산: 상품명 넓게, 정규화는 하되 상품명 가중치 크게 ----------
    cols = df.columns.tolist()
    col_idx_map = {c: i for i, c in enumerate(cols)}

    # 각 열 최대 글자수(헤더/데이터 포함)
    max_lens = []
    for c in cols:
        head_len = len(str(c))
        body_len = max(len(str(v)) for v in df[c]) if len(df) else 0
        max_lens.append(max(head_len, body_len))

    base_w, k = 0.03, 0.035
    widths = base_w + k * np.log1p(np.array(max_lens))

    # ✅ 상품명 열 가중치 크게 (잘림 방지)
    if "상품명" in col_idx_map:
        widths[col_idx_map["상품명"]] *= 2.2   # 필요하면 2.5~3.0까지 올려도 됨

    # 최소/최대 비율 제한 후 정규화(합=1)  — 너무 좁아지지 않게 lower bound 올림
    widths = np.clip(widths, 0.08, 0.70)
    widths = widths / widths.sum()


    # ✅ 전체 가로폭을 텍스트 양에 비례해 확대
    #    (상품명 비중을 조금 더 반영)
    total_chars = sum(max_lens) + max_lens[col_idx_map["상품명"]]
    fig_w = min(20.0, max(12.0, 0.16 * total_chars))  # 12~20인치 사이 동적
    fig_h = 6.0

    fig, ax = plt.subplots(figsize=(fig_w, fig_h))
    ax.axis("off")

    table = ax.table(
        cellText=df.values,
        colLabels=cols,
        colWidths=widths.tolist(),   # 비율(합=1)
        cellLoc="center",
        loc="center"
    )

    # 폰트/스케일
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.20, 1.18)         # x 스케일 살짝 키워 가로 여유 확보

    # 헤더 색
    for c in range(len(cols)):
        table[(0, c)].set_facecolor("#eeeeee")

    # 3줄 블록 A/B
    nrows, ncols = len(df), len(cols)
    color_a, color_b = "#ffffff", "#CBE7F6"
    for r in range(1, nrows+1):
        row_color = color_a if ((r-1)//3) % 2 == 0 else color_b
        for c in range(ncols):
            table[(r, c)].set_facecolor(row_color)

    # 상품명 열만 9pt (겹침 여지 줄임)
    if "상품명" in col_idx_map:
        cidx = col_idx_map["상품명"]
        for r in range(len(df)+1):  # 헤더 포함
            table[(r, cidx)].set_fontsize(9)

    # 좌우 여백 최소화
    for (r, c), cell in table.get_celld().items():
        if hasattr(cell, "PAD"):
            cell.PAD = 0.1

    # ✅ 가능한 경우: 실제 텍스트 폭 기반으로 열 자동 폭 재설정 (matplotlib 버전에 따라 지원)
    if hasattr(table, "auto_set_column_width"):
        try:
            table.auto_set_column_width(col=list(range(ncols)))
        except Exception:
            pass

    #plt.subplots_adjust(left=0.02, right=0.98)
    #plt.tight_layout(pad=0.2)

    file_path4_thisWeekSalesTop3 = "graph4_thisWeekRevTop3.png"
        # ⬇️ 잘림 방지용 bbox_inches
    fig.savefig(file_path4_thisWeekSalesTop3, dpi=170, bbox_inches='tight')
    plt.close(fig)

    blob = bucket.blob(f'{gameidx}/{file_path4_thisWeekSalesTop3}')
    blob.upload_from_filename(file_path4_thisWeekSalesTop3)

    # 메모리에 올라간 이미지 파일 삭제
    os.remove(file_path4_thisWeekSalesTop3)

    return f'{gameidx}/{file_path4_thisWeekSalesTop3}'


def rgroup_rev_upload_notion(joyplegameid: int, gameidx: str, service_sub:str, **context):

    PAGE_INFO=context['task_instance'].xcom_pull(
        task_ids = 'make_gameframework_notion_page',
        key='page_info'
    )

    ########### (1) 제목
    notion.blocks.children.append(
        PAGE_INFO['id'],
        children=[
            {
                "object": "block",
                "type": "heading_2",
                "heading_2": {
                    "rich_text": [{"type": "text", "text": {"content": "4. 이번주 상세 매출" }}]
                },
            }
        ],
    )

    notion.blocks.children.append(
        PAGE_INFO['id'],
        children=[
            {
                "object": "block",
                "type": "heading_3",
                "heading_3": {
                    "rich_text": [{"type": "text", "text": {"content": "\n(1) R그룹별 매출" }}]
                },
            }
        ],
    )

    notion.blocks.children.append(
        PAGE_INFO['id'],
        children=[
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [{"type": "text", "text": {"content": " ** 전월 과금액 기준 R그룹 입니다. \n ** 주차별 기준은 수요일~화요일 입니다. " }}]
                },
            }
        ],
    )

    try:
        gcs_path = f'{gameidx}/graph4_RgroupSales.png'
        blob = bucket.blob(gcs_path)
        image_bytes = blob.download_as_bytes()
        filename = 'graph4_RgroupSales.png'
        print(f"✓ GCS 이미지 다운로드 성공 : {gcs_path}")
    except Exception as e:
        print(f"❌ GCS 다운로드 실패: {str(e)}")
        raise

    # 공통 헤더
    headers_json = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json"
    }

    try:
        # 1) 업로드 객체 생성 (file_upload 생성)
        create_url = "https://api.notion.com/v1/file_uploads"
        payload = {
            "filename": filename,
            "content_type": "image/png"
        }

        resp = requests.post(create_url, headers=headers_json, data=json.dumps(payload))
        resp.raise_for_status()
        file_upload = resp.json()
        file_upload_id = file_upload["id"]
        print(f"✓ Notion 업로드 객체 생성: {file_upload_id}")

        # 2) 파일 바이너리 전송 (multipart/form-data)
        # ✅ 로컬 파일 대신 BytesIO 사용
        send_url = f"https://api.notion.com/v1/file_uploads/{file_upload_id}/send"
        files = {"file": (filename, BytesIO(image_bytes), "image/png")}
        headers_send = {
            "Authorization": f"Bearer {NOTION_TOKEN}",
            "Notion-Version": NOTION_VERSION
        }
        send_resp = requests.post(send_url, headers=headers_send, files=files)
        send_resp.raise_for_status()
        print(f"✓ 파일 전송 완료: {filename}")

        # 3) Notion 페이지에 이미지 블록 추가
        append_url = f"https://api.notion.com/v1/blocks/{PAGE_INFO['id']}/children"
        append_payload = {
            "children": [
                {
                    "object": "block",
                    "type": "image",
                    "image": {
                        "type": "file_upload",
                        "file_upload": {"id": file_upload_id},
                    }
                }
            ]
        }

        append_resp = requests.patch(
            append_url, headers=headers_json, data=json.dumps(append_payload)
        )
        append_resp.raise_for_status()
        print(f"✅ Notion에 이미지 추가 완료: {filename}")

    except requests.exceptions.RequestException as e:
        print(f"❌ Notion API 에러: {str(e)}")
        raise
    except Exception as e:
        print(f"❌ 예기치 않은 에러: {str(e)}")
        raise


    query_result4_RgroupSales = context['task_instance'].xcom_pull(
        task_ids = 'rev_group_rev_pu',
        key='rev_group_rev_pu'
    )

    resp = df_to_notion_table_under_toggle(
        notion=notion,
        page_id=PAGE_INFO['id'],
        df=query_result4_RgroupSales,
        toggle_title="📊 로데이터 - R그룹 ",
        max_first_batch_rows=90,
        batch_size=100,
    )

    ########### (3) 제미나이 해석

    blocks = md_to_notion_blocks(rev_group_rev_pu_gemini(joyplegameid, service_sub, **context))

    notion.blocks.children.append(
        block_id=PAGE_INFO['id'],
        children=blocks
    )

    notion.blocks.children.append(
        PAGE_INFO['id'],
        children=[
            {
                "object": "block",
                "type": "heading_3",
                "heading_3": {
                    "rich_text": [{"type": "text", "text": {"content": "\n(2) 상품군별 매출" }}]
                },
            }
        ],
    )

    notion.blocks.children.append(
        PAGE_INFO['id'],
        children=[
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [{"type": "text", "text": {"content":" ** 데이터 기준 : IAP 구매 - IAP 젬구매 - IAP 루비구매 + 유가젬 사용내역 + 유가루비 사용내역 " }}]
                },
            }
        ],
    )

    notion.blocks.children.append(
        PAGE_INFO['id'],
        children=[
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [{"type": "text", "text": {"content":" ** 젬과 루비로 어떤 상품을 구매했는지 확인하기 위해, IAP로 젬과 루비를 구매한 것은 제거한 후 유가젬/유가루비 사용내역을 매출로 집계하였습니다." }}]
                },
            }
        ],
    )



def iap_gem_ruby_upload_notion(joyplegameid: int, gameidx: str, service_sub:str, **context):

    PAGE_INFO=context['task_instance'].xcom_pull(
        task_ids = 'make_gameframework_notion_page',
        key='page_info'
    )


    try:
        gcs_path = f'{gameidx}/graph4_salesByPackage.png'
        blob = bucket.blob(gcs_path)
        image_bytes = blob.download_as_bytes()
        filename = 'graph4_salesByPackage.png'
        print(f"✓ GCS 이미지 다운로드 성공 : {gcs_path}")
    except Exception as e:
        print(f"❌ GCS 다운로드 실패: {str(e)}")
        raise


    # 공통 헤더
    headers_json = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json"
    }

    try:   
        # 1) 업로드 객체 생성 (file_upload 생성)
        create_url = "https://api.notion.com/v1/file_uploads"
        payload = {
            "filename": filename,
            "content_type": "image/png"
        }

        resp = requests.post(create_url, headers=headers_json, data=json.dumps(payload))
        resp.raise_for_status()
        file_upload = resp.json()
        file_upload_id = file_upload["id"]
        print(f"✓ Notion 업로드 객체 생성: {file_upload_id}")

        # 2) 파일 바이너리 전송 (multipart/form-data)
        # ✅ 로컬 파일 대신 BytesIO 사용
        send_url = f"https://api.notion.com/v1/file_uploads/{file_upload_id}/send"
        files = {"file": (filename, BytesIO(image_bytes), "image/png")}
        headers_send = {
            "Authorization": f"Bearer {NOTION_TOKEN}",
            "Notion-Version": NOTION_VERSION
        }
        send_resp = requests.post(send_url, headers=headers_send, files=files)
        send_resp.raise_for_status()
        print(f"✓ 파일 전송 완료: {filename}")

        # 3) Notion 페이지에 이미지 블록 추가
        append_url = f"https://api.notion.com/v1/blocks/{PAGE_INFO['id']}/children"
        append_payload = {
            "children": [
                {
                    "object": "block",
                    "type": "image",
                    "image": {
                        "type": "file_upload",
                        "file_upload": {"id": file_upload_id},
                        # 캡션을 달고 싶다면 아래 주석 해제
                        # "caption": [{"type": "text", "text": {"content": "자동 업로드된 그래프"}}]
                    }
                }
            ]
        }

        append_resp = requests.patch(
            append_url, headers=headers_json, data=json.dumps(append_payload)
        )
        append_resp.raise_for_status()
        print(f"✅ Notion에 이미지 추가 완료: {filename}")

    except requests.exceptions.RequestException as e:
        print(f"❌ Notion API 에러: {str(e)}")
        raise
    except Exception as e:
        print(f"❌ 예기치 않은 에러: {str(e)}")
        raise

    query_result4_salesByPackage = context['task_instance'].xcom_pull(
        task_ids = 'iap_gem_ruby',
        key='iap_gem_ruby'
        )

    resp = df_to_notion_table_under_toggle(
        notion=notion,
        page_id=PAGE_INFO['id'],
        df=query_result4_salesByPackage,
        toggle_title="📊 로데이터 - 상품군별 매출 ",
        max_first_batch_rows=90,
        batch_size=100,
        )
    
    blocks = md_to_notion_blocks(iap_gem_ruby_gemini(service_sub, **context))

    notion.blocks.children.append(
        block_id=PAGE_INFO['id'],
        children=blocks
    )

    # 프롬프트 결과 중간에 그래프 삽입을 위한 결과 텍스트 5분할

    text = top3_items_by_category_gemini(service_sub)

    # 줄 단위 분리
    lines = [line.strip() for line in text.split("\n") if line.strip()]

    # 블록 단위 분리
    blocks_raw = []
    current_block = []

    for line in lines:
        if line.startswith("**") and line.endswith("**"):
            # 새로운 블록 시작 → 기존 블록 저장
            if current_block:
                blocks_raw.append(current_block)
            current_block = [line]
        else:
            current_block.append(line)

    # 마지막 블록 저장
    if current_block:
        blocks_raw.append(current_block)

    # 이제 blocks_raw = [[헤더, 내용...], [헤더, 내용...], ...]

    # 최종 결과 저장
    blocks_bucket = {"blocks_1": [], "blocks_2": [], "blocks_3": [], "blocks_4": [], "blocks_5": []}

    found_first = False
    for block in blocks_raw:
        header = block[0]

        # 매출 1위 전까지는 blocks_1
        if not found_first:
            if "(매출 1위)" in header:
                found_first = True
                blocks_bucket["blocks_2"] = block
            else:
                blocks_bucket["blocks_1"].extend(block)
            continue

        # 이후 매출 2위, 3위, 나머지 구분
        if "(매출 2위)" in header:
            blocks_bucket["blocks_3"] = block
        elif "(매출 3위)" in header:
            blocks_bucket["blocks_4"] = block
        else:
            blocks_bucket["blocks_5"].extend(block)

    for k, v in blocks_bucket.items():
        if isinstance(v, list):
            blocks_bucket[k] = "\n".join(v)  # 리스트 → 문자열 변환

    blocks = md_to_notion_blocks(blocks_bucket["blocks_1"], 1)

    notion.blocks.children.append(
        block_id=PAGE_INFO['id'],
        children=blocks
    )


## 상품카테고리별 매출 1위 그래프 삽입
    try:
        gcs_path = f'{gameidx}/graph4_salesByPackage_Category1.png'
        blob = bucket.blob(gcs_path)
        image_bytes = blob.download_as_bytes()
        filename = 'graph4_salesByPackage_Category1.png'
        print(f"✓ GCS 이미지 다운로드 성공 : {gcs_path}")
    except Exception as e:
        print(f"❌ GCS 다운로드 실패: {str(e)}")
        raise

    # 공통 헤더
    headers_json = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json"
    }

    try:   
        # 1) 업로드 객체 생성 (file_upload 생성)
        create_url = "https://api.notion.com/v1/file_uploads"
        payload = {
            "filename": filename,
            "content_type": "image/png"
        }

        resp = requests.post(create_url, headers=headers_json, data=json.dumps(payload))
        resp.raise_for_status()
        file_upload = resp.json()
        file_upload_id = file_upload["id"]
        print(f"✓ Notion 업로드 객체 생성: {file_upload_id}")

        # 2) 파일 바이너리 전송 (multipart/form-data)
        # ✅ 로컬 파일 대신 BytesIO 사용
        send_url = f"https://api.notion.com/v1/file_uploads/{file_upload_id}/send"
        files = {"file": (filename, BytesIO(image_bytes), "image/png")}
        headers_send = {
            "Authorization": f"Bearer {NOTION_TOKEN}",
            "Notion-Version": NOTION_VERSION
        }
        send_resp = requests.post(send_url, headers=headers_send, files=files)
        send_resp.raise_for_status()
        print(f"✓ 파일 전송 완료: {filename}")

        # 3) Notion 페이지에 이미지 블록 추가
        append_url = f"https://api.notion.com/v1/blocks/{PAGE_INFO['id']}/children"
        append_payload = {
            "children": [
                {
                    "object": "block",
                    "type": "image",
                    "image": {
                        "type": "file_upload",
                        "file_upload": {"id": file_upload_id},
                        # 캡션을 달고 싶다면 아래 주석 해제
                        # "caption": [{"type": "text", "text": {"content": "자동 업로드된 그래프"}}]
                    }
                }
            ]
        }

        append_resp = requests.patch(
            append_url, headers=headers_json, data=json.dumps(append_payload)
        )
        append_resp.raise_for_status()
        print(f"✅ Notion에 이미지 추가 완료: {filename}")

    except requests.exceptions.RequestException as e:
        print(f"❌ Notion API 에러: {str(e)}")
        raise
    except Exception as e:
        print(f"❌ 예기치 않은 에러: {str(e)}")
        raise

    blocks = md_to_notion_blocks(blocks_bucket["blocks_2"], 1)

    notion.blocks.children.append(
        block_id=PAGE_INFO['id'],
        children=blocks
    )


    ## 상품카테고리별 매출 2위 그래프 삽입
    try:
        gcs_path = f'{gameidx}/graph4_salesByPackage_Category2.png'
        blob = bucket.blob(gcs_path)
        image_bytes = blob.download_as_bytes()
        filename = 'graph4_salesByPackage_Category2.png'
        print(f"✓ GCS 이미지 다운로드 성공 : {gcs_path}")
    except Exception as e:
        print(f"❌ GCS 다운로드 실패: {str(e)}")
        raise

    try:   
        # 1) 업로드 객체 생성 (file_upload 생성)
        create_url = "https://api.notion.com/v1/file_uploads"
        payload = {
            "filename": filename,
            "content_type": "image/png"
        }

        resp = requests.post(create_url, headers=headers_json, data=json.dumps(payload))
        resp.raise_for_status()
        file_upload = resp.json()
        file_upload_id = file_upload["id"]
        print(f"✓ Notion 업로드 객체 생성: {file_upload_id}")

        # 2) 파일 바이너리 전송 (multipart/form-data)
        # ✅ 로컬 파일 대신 BytesIO 사용
        send_url = f"https://api.notion.com/v1/file_uploads/{file_upload_id}/send"
        files = {"file": (filename, BytesIO(image_bytes), "image/png")}
        headers_send = {
            "Authorization": f"Bearer {NOTION_TOKEN}",
            "Notion-Version": NOTION_VERSION
        }
        send_resp = requests.post(send_url, headers=headers_send, files=files)
        send_resp.raise_for_status()
        print(f"✓ 파일 전송 완료: {filename}")

        # 3) Notion 페이지에 이미지 블록 추가
        append_url = f"https://api.notion.com/v1/blocks/{PAGE_INFO['id']}/children"
        append_payload = {
            "children": [
                {
                    "object": "block",
                    "type": "image",
                    "image": {
                        "type": "file_upload",
                        "file_upload": {"id": file_upload_id},
                        # 캡션을 달고 싶다면 아래 주석 해제
                        # "caption": [{"type": "text", "text": {"content": "자동 업로드된 그래프"}}]
                    }
                }
            ]
        }

        append_resp = requests.patch(
            append_url, headers=headers_json, data=json.dumps(append_payload)
        )
        append_resp.raise_for_status()
        print(f"✅ Notion에 이미지 추가 완료: {filename}")

    except requests.exceptions.RequestException as e:
        print(f"❌ Notion API 에러: {str(e)}")
        raise
    except Exception as e:
        print(f"❌ 예기치 않은 에러: {str(e)}")
        raise

    blocks = md_to_notion_blocks(blocks_bucket["blocks_3"], 1)

    notion.blocks.children.append(
        block_id=PAGE_INFO['id'],
        children=blocks
    )

    ## 상품카테고리별 매출 3위 그래프 삽입
    try:
        gcs_path = f'{gameidx}/graph4_salesByPackage_Category3.png'
        blob = bucket.blob(gcs_path)
        image_bytes = blob.download_as_bytes()
        filename = 'graph4_salesByPackage_Category3.png'
        print(f"✓ GCS 이미지 다운로드 성공 : {gcs_path}")
    except Exception as e:
        print(f"❌ GCS 다운로드 실패: {str(e)}")
        raise

    try:   
        # 1) 업로드 객체 생성 (file_upload 생성)
        create_url = "https://api.notion.com/v1/file_uploads"
        payload = {
            "filename": filename,
            "content_type": "image/png"
        }

        resp = requests.post(create_url, headers=headers_json, data=json.dumps(payload))
        resp.raise_for_status()
        file_upload = resp.json()
        file_upload_id = file_upload["id"]
        print(f"✓ Notion 업로드 객체 생성: {file_upload_id}")

        # 2) 파일 바이너리 전송 (multipart/form-data)
        # ✅ 로컬 파일 대신 BytesIO 사용
        send_url = f"https://api.notion.com/v1/file_uploads/{file_upload_id}/send"
        files = {"file": (filename, BytesIO(image_bytes), "image/png")}
        headers_send = {
            "Authorization": f"Bearer {NOTION_TOKEN}",
            "Notion-Version": NOTION_VERSION
        }
        send_resp = requests.post(send_url, headers=headers_send, files=files)
        send_resp.raise_for_status()
        print(f"✓ 파일 전송 완료: {filename}")

        # 3) Notion 페이지에 이미지 블록 추가
        append_url = f"https://api.notion.com/v1/blocks/{PAGE_INFO['id']}/children"
        append_payload = {
            "children": [
                {
                    "object": "block",
                    "type": "image",
                    "image": {
                        "type": "file_upload",
                        "file_upload": {"id": file_upload_id},
                        # 캡션을 달고 싶다면 아래 주석 해제
                        # "caption": [{"type": "text", "text": {"content": "자동 업로드된 그래프"}}]
                    }
                }
            ]
        }

        append_resp = requests.patch(
            append_url, headers=headers_json, data=json.dumps(append_payload)
        )
        append_resp.raise_for_status()
        print(f"✅ Notion에 이미지 추가 완료: {filename}")

    except requests.exceptions.RequestException as e:
        print(f"❌ Notion API 에러: {str(e)}")
        raise
    except Exception as e:
        print(f"❌ 예기치 않은 에러: {str(e)}")
        raise

    blocks = md_to_notion_blocks(blocks_bucket["blocks_4"], 1)

    notion.blocks.children.append(
        block_id=PAGE_INFO['id'],
        children=blocks
    )

    blocks = md_to_notion_blocks(blocks_bucket["blocks_5"], 1)

    notion.blocks.children.append(
        block_id=PAGE_INFO['id'],
        children=blocks
    )

    return True


def iap_toggle_add(gameidx: str, service_sub:str, **context):
    
    PAGE_INFO=context['task_instance'].xcom_pull(
        task_ids = 'make_gameframework_notion_page',
        key='page_info'
    )

    toggle_resp = notion.blocks.children.append(
        PAGE_INFO['id'],
        children=[
            {
                "object": "block",
                "type": "toggle",
                "toggle": {
                    "rich_text": [
                        {"type": "text", "text": {"content": "(IAP) 상품군별 매출"}, "annotations": {"bold": True}}
                    ]
                },
            }
        ],
    )
    toggle_id = toggle_resp["results"][0]["id"]

    create_url = "https://api.notion.com/v1/file_uploads"

        # 공통 헤더
    headers_json = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json"
    }

    try:
        gcs_path = f'{gameidx}/graph4_salesByPackage_IAP.png'
        blob = bucket.blob(gcs_path)
        image_bytes = blob.download_as_bytes()
        filename = 'graph4_salesByPackage_IAP.png'
        print(f"✓ GCS 이미지 다운로드 성공 : {gcs_path}")
    except Exception as e:
        print(f"❌ GCS 다운로드 실패: {str(e)}")
        raise

    try:   
        # 1) 업로드 객체 생성 (file_upload 생성)
        create_url = "https://api.notion.com/v1/file_uploads"
        payload = {
            "filename": filename,
            "content_type": "image/png"
        }

        resp = requests.post(create_url, headers=headers_json, data=json.dumps(payload))
        resp.raise_for_status()
        file_upload = resp.json()
        file_upload_id = file_upload["id"]
        print(f"✓ Notion 업로드 객체 생성: {file_upload_id}")

        # 2) 파일 바이너리 전송 (multipart/form-data)
        # ✅ 로컬 파일 대신 BytesIO 사용
        send_url = f"https://api.notion.com/v1/file_uploads/{file_upload_id}/send"
        files = {"file": (filename, BytesIO(image_bytes), "image/png")}
        headers_send = {
            "Authorization": f"Bearer {NOTION_TOKEN}",
            "Notion-Version": NOTION_VERSION
        }
        send_resp = requests.post(send_url, headers=headers_send, files=files)
        send_resp.raise_for_status()
        print(f"✓ 파일 전송 완료: {filename}")

        # 3) Notion 페이지에 이미지 블록 추가
        append_url = f"https://api.notion.com/v1/blocks/{toggle_id}/children"
        append_payload = {
            "children": [
                {
                    "object": "block",
                    "type": "image",
                    "image": {
                        "type": "file_upload",
                        "file_upload": {"id": file_upload_id},
                        # 캡션을 달고 싶다면 아래 주석 해제
                        # "caption": [{"type": "text", "text": {"content": "자동 업로드된 그래프"}}]
                    }
                }
            ]
        }

        append_resp = requests.patch(
            append_url, headers=headers_json, data=json.dumps(append_payload)
        )
        append_resp.raise_for_status()
        print(f"✅ Notion에 이미지 추가 완료: {filename}")

    except requests.exceptions.RequestException as e:
        print(f"❌ Notion API 에러: {str(e)}")
        raise
    except Exception as e:
        print(f"❌ 예기치 않은 에러: {str(e)}")
        raise

    query_result4_salesByPackage_IAP = context['task_instance'].xcom_pull(
        task_ids='iap_df',
        key='iap_df'
    )

    resp = df_to_notion_table_under_toggle(
        notion=notion,
        page_id=toggle_id,
        df=query_result4_salesByPackage_IAP,
        toggle_title="📊 로데이터 - (IAP) 상품군별 매출 ",
        max_first_batch_rows=90,
        batch_size=100,
    )

    blocks = md_to_notion_blocks(iap_df_gemini(service_sub))

    notion.blocks.children.append(
        block_id=toggle_id,
        children=blocks
    )

    return True

def gem_toggle_add(gameidx: str, service_sub:str, **context):

    PAGE_INFO=context['task_instance'].xcom_pull(
        task_ids = 'make_gameframework_notion_page',
        key='page_info'
    )

    toggle_resp = notion.blocks.children.append(
    PAGE_INFO['id'],
    children=[
            {
                "object": "block",
                "type": "toggle",
                "toggle": {
                    "rich_text": [
                        {"type": "text", "text": {"content": "(젬) 상품군별 매출"}, "annotations": {"bold": True}}
                    ]
                },
            }
        ],
    )
    toggle_id = toggle_resp["results"][0]["id"]

    create_url = "https://api.notion.com/v1/file_uploads"

    # 공통 헤더
    headers_json = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json"
    }

    try:
        gcs_path = f'{gameidx}/graph4_salesByPackage_GEM.png'
        blob = bucket.blob(gcs_path)
        image_bytes = blob.download_as_bytes()
        filename = 'graph4_salesByPackage_GEM.png'
        print(f"✓ GCS 이미지 다운로드 성공 : {gcs_path}")
    except Exception as e:
        print(f"❌ GCS 다운로드 실패: {str(e)}")
        raise

    try:   
        # 1) 업로드 객체 생성 (file_upload 생성)
        create_url = "https://api.notion.com/v1/file_uploads"
        payload = {
            "filename": filename,
            "content_type": "image/png"
        }

        resp = requests.post(create_url, headers=headers_json, data=json.dumps(payload))
        resp.raise_for_status()
        file_upload = resp.json()
        file_upload_id = file_upload["id"]
        print(f"✓ Notion 업로드 객체 생성: {file_upload_id}")

        # 2) 파일 바이너리 전송 (multipart/form-data)
        # ✅ 로컬 파일 대신 BytesIO 사용
        send_url = f"https://api.notion.com/v1/file_uploads/{file_upload_id}/send"
        files = {"file": (filename, BytesIO(image_bytes), "image/png")}
        headers_send = {
            "Authorization": f"Bearer {NOTION_TOKEN}",
            "Notion-Version": NOTION_VERSION
        }
        send_resp = requests.post(send_url, headers=headers_send, files=files)
        send_resp.raise_for_status()
        print(f"✓ 파일 전송 완료: {filename}")

        # 3) Notion 페이지에 이미지 블록 추가
        append_url = f"https://api.notion.com/v1/blocks/{toggle_id}/children"
        append_payload = {
            "children": [
                {
                    "object": "block",
                    "type": "image",
                    "image": {
                        "type": "file_upload",
                        "file_upload": {"id": file_upload_id},
                        # 캡션을 달고 싶다면 아래 주석 해제
                        # "caption": [{"type": "text", "text": {"content": "자동 업로드된 그래프"}}]
                    }
                }
            ]
        }

        append_resp = requests.patch(
            append_url, headers=headers_json, data=json.dumps(append_payload)
        )
        append_resp.raise_for_status()
        print(f"✅ Notion에 이미지 추가 완료: {filename}")

    except requests.exceptions.RequestException as e:
        print(f"❌ Notion API 에러: {str(e)}")
        raise
    except Exception as e:
        print(f"❌ 예기치 않은 에러: {str(e)}")
        raise

    query_result4_salesByPackage_GEM = context['task_instance'].xcom_pull(
        task_ids='gem_df',
        key='gem_df'
    )

    resp = df_to_notion_table_under_toggle(
        notion=notion,
        page_id=toggle_id,
        df=query_result4_salesByPackage_GEM,
        toggle_title="📊 로데이터 - (젬) 상품군별 매출 ",
        max_first_batch_rows=90,
        batch_size=100,
    )

    blocks = md_to_notion_blocks(gem_df_gemini(service_sub))

    notion.blocks.children.append(
        block_id=toggle_id,
        children=blocks
    )

    return True


def ruby_toggle_add(gameidx: str, service_sub:str, **context):

    PAGE_INFO=context['task_instance'].xcom_pull(
        task_ids = 'make_gameframework_notion_page',
        key='page_info'
    )

    toggle_resp = notion.blocks.children.append(
        PAGE_INFO['id'],
        children=[
            {
                "object": "block",
                "type": "toggle",
                "toggle": {
                    "rich_text": [
                        {"type": "text", "text": {"content": "(루비) 상품군별 매출"}, "annotations": {"bold": True}}
                    ]
                },
            }
        ],
    )
    toggle_id = toggle_resp["results"][0]["id"]

    create_url = "https://api.notion.com/v1/file_uploads"

    # 공통 헤더
    headers_json = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json"
    }

    try:
        gcs_path = f'{gameidx}/graph4_salesByPackage_RUBY.png'
        blob = bucket.blob(gcs_path)
        image_bytes = blob.download_as_bytes()
        filename = 'graph4_salesByPackage_RUBY.png'
        print(f"✓ GCS 이미지 다운로드 성공 : {gcs_path}")
    except Exception as e:
        print(f"❌ GCS 다운로드 실패: {str(e)}")
        raise

    try:   
        # 1) 업로드 객체 생성 (file_upload 생성)
        create_url = "https://api.notion.com/v1/file_uploads"
        payload = {
            "filename": filename,
            "content_type": "image/png"
        }

        resp = requests.post(create_url, headers=headers_json, data=json.dumps(payload))
        resp.raise_for_status()
        file_upload = resp.json()
        file_upload_id = file_upload["id"]
        print(f"✓ Notion 업로드 객체 생성: {file_upload_id}")

        # 2) 파일 바이너리 전송 (multipart/form-data)
        # ✅ 로컬 파일 대신 BytesIO 사용
        send_url = f"https://api.notion.com/v1/file_uploads/{file_upload_id}/send"
        files = {"file": (filename, BytesIO(image_bytes), "image/png")}
        headers_send = {
            "Authorization": f"Bearer {NOTION_TOKEN}",
            "Notion-Version": NOTION_VERSION
        }
        send_resp = requests.post(send_url, headers=headers_send, files=files)
        send_resp.raise_for_status()
        print(f"✓ 파일 전송 완료: {filename}")

        # 3) Notion 페이지에 이미지 블록 추가
        append_url = f"https://api.notion.com/v1/blocks/{toggle_id}/children"
        append_payload = {
            "children": [
                {
                    "object": "block",
                    "type": "image",
                    "image": {
                        "type": "file_upload",
                        "file_upload": {"id": file_upload_id},
                        # 캡션을 달고 싶다면 아래 주석 해제
                        # "caption": [{"type": "text", "text": {"content": "자동 업로드된 그래프"}}]
                    }
                }
            ]
        }

        append_resp = requests.patch(
            append_url, headers=headers_json, data=json.dumps(append_payload)
        )
        append_resp.raise_for_status()
        print(f"✅ Notion에 이미지 추가 완료: {filename}")

    except requests.exceptions.RequestException as e:
        print(f"❌ Notion API 에러: {str(e)}")
        raise
    except Exception as e:
        print(f"❌ 예기치 않은 에러: {str(e)}")
        raise

    query_result4_salesByPackage_RUBY = context['task_instance'].xcom_pull(
        task_ids='ruby_df',
        key='ruby_df'
    )

    resp = df_to_notion_table_under_toggle(
        notion=notion,
        page_id=toggle_id,
        df=query_result4_salesByPackage_RUBY,
        toggle_title="📊 로데이터 - (루비) 상품군별 매출 ",
        max_first_batch_rows=90,
        batch_size=100,
    )

    blocks = md_to_notion_blocks(ruby_df_gemini(service_sub))

    notion.blocks.children.append(
        block_id=toggle_id,
        children=blocks
    )

    return True


def rgroup_top3_upload_notion(gameidx: str, service_sub:str, **context):

    PAGE_INFO=context['task_instance'].xcom_pull(
        task_ids = 'make_gameframework_notion_page',
        key='page_info'
    )

    notion.blocks.children.append(
    PAGE_INFO['id'],
    children=[
            {
                "object": "block",
                "type": "heading_3",
                "heading_3": {
                    "rich_text": [{"type": "text", "text": {"content": "\n(3) 과금그룹별 매출/PU 상위 3개 상품 \n" }}]
                },
            }
        ],
    )

    # 공통 헤더
    headers_json = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json"
    }

    try:
        gcs_path = f'{gameidx}/graph4_thisWeekPUTop3.png'
        blob = bucket.blob(gcs_path)
        image_bytes = blob.download_as_bytes()
        filename = 'graph4_thisWeekPUTop3.png'
        print(f"✓ GCS 이미지 다운로드 성공 : {gcs_path}")
    except Exception as e:
        print(f"❌ GCS 다운로드 실패: {str(e)}")
        raise

    try:   
        # 1) 업로드 객체 생성 (file_upload 생성)
        create_url = "https://api.notion.com/v1/file_uploads"
        payload = {
            "filename": filename,
            "content_type": "image/png"
        }

        resp = requests.post(create_url, headers=headers_json, data=json.dumps(payload))
        resp.raise_for_status()
        file_upload = resp.json()
        file_upload_id = file_upload["id"]
        print(f"✓ Notion 업로드 객체 생성: {file_upload_id}")

        # 2) 파일 바이너리 전송 (multipart/form-data)
        # ✅ 로컬 파일 대신 BytesIO 사용
        send_url = f"https://api.notion.com/v1/file_uploads/{file_upload_id}/send"
        files = {"file": (filename, BytesIO(image_bytes), "image/png")}
        headers_send = {
            "Authorization": f"Bearer {NOTION_TOKEN}",
            "Notion-Version": NOTION_VERSION
        }
        send_resp = requests.post(send_url, headers=headers_send, files=files)
        send_resp.raise_for_status()
        print(f"✓ 파일 전송 완료: {filename}")

        # 3) Notion 페이지에 이미지 블록 추가
        append_url = f"https://api.notion.com/v1/blocks/{PAGE_INFO['id']}/children"
        append_payload = {
            "children": [
                {
                    "object": "block",
                    "type": "image",
                    "image": {
                        "type": "file_upload",
                        "file_upload": {"id": file_upload_id},
                        # 캡션을 달고 싶다면 아래 주석 해제
                        # "caption": [{"type": "text", "text": {"content": "자동 업로드된 그래프"}}]
                    }
                }
            ]
        }

        append_resp = requests.patch(
            append_url, headers=headers_json, data=json.dumps(append_payload)
        )
        append_resp.raise_for_status()
        print(f"✅ Notion에 이미지 추가 완료: {filename}")

    except requests.exceptions.RequestException as e:
        print(f"❌ Notion API 에러: {str(e)}")
        raise
    except Exception as e:
        print(f"❌ 예기치 않은 에러: {str(e)}")
        raise

    ### 파일 업로드 객체 
    try:
        gcs_path = f'{gameidx}/graph4_thisWeekRevTop3.png'
        blob = bucket.blob(gcs_path)
        image_bytes = blob.download_as_bytes()
        filename = 'graph4_thisWeekRevTop3.png'
        print(f"✓ GCS 이미지 다운로드 성공 : {gcs_path}")
    except Exception as e:
        print(f"❌ GCS 다운로드 실패: {str(e)}")
        raise

    try:   
        # 1) 업로드 객체 생성 (file_upload 생성)
        create_url = "https://api.notion.com/v1/file_uploads"
        payload = {
            "filename": filename,
            "content_type": "image/png"
        }

        resp = requests.post(create_url, headers=headers_json, data=json.dumps(payload))
        resp.raise_for_status()
        file_upload = resp.json()
        file_upload_id = file_upload["id"]
        print(f"✓ Notion 업로드 객체 생성: {file_upload_id}")

        # 2) 파일 바이너리 전송 (multipart/form-data)
        # ✅ 로컬 파일 대신 BytesIO 사용
        send_url = f"https://api.notion.com/v1/file_uploads/{file_upload_id}/send"
        files = {"file": (filename, BytesIO(image_bytes), "image/png")}
        headers_send = {
            "Authorization": f"Bearer {NOTION_TOKEN}",
            "Notion-Version": NOTION_VERSION
        }
        send_resp = requests.post(send_url, headers=headers_send, files=files)
        send_resp.raise_for_status()
        print(f"✓ 파일 전송 완료: {filename}")

        # 3) Notion 페이지에 이미지 블록 추가
        append_url = f"https://api.notion.com/v1/blocks/{PAGE_INFO['id']}/children"
        append_payload = {
            "children": [
                {
                    "object": "block",
                    "type": "image",
                    "image": {
                        "type": "file_upload",
                        "file_upload": {"id": file_upload_id},
                        # 캡션을 달고 싶다면 아래 주석 해제
                        # "caption": [{"type": "text", "text": {"content": "자동 업로드된 그래프"}}]
                    }
                }
            ]
        }

        append_resp = requests.patch(
            append_url, headers=headers_json, data=json.dumps(append_payload)
        )
        append_resp.raise_for_status()
        print(f"✅ Notion에 이미지 추가 완료: {filename}")

    except requests.exceptions.RequestException as e:
        print(f"❌ Notion API 에러: {str(e)}")
        raise
    except Exception as e:
        print(f"❌ 예기치 않은 에러: {str(e)}")
        raise


    query_result4_thisWeekPUTop3 = context['task_instance'].xcom_pull(
        task_ids = 'rgroup_top3_rev',
        key='rgroup_top3_rev'
    )

    query_result4_thisWeekSalesTop3 = context['task_instance'].xcom_pull(
        task_ids = 'rgroup_top3_rev',
        key='rgroup_top3_rev'
    )

    resp = df_to_notion_table_under_toggle(
        notion=notion,
        page_id=PAGE_INFO['id'],
        df=query_result4_thisWeekPUTop3,
        toggle_title="📊 로데이터 - R그룹별 상위3개 상품(PU)",
        max_first_batch_rows=90,
        batch_size=100,
    )

    resp = df_to_notion_table_under_toggle(
        notion=notion,
        page_id=PAGE_INFO['id'],
        df=query_result4_thisWeekSalesTop3,
        toggle_title="📊 로데이터 - R그룹별 상위3개 상품(매출) ",
        max_first_batch_rows=90,
        batch_size=100,
    )

    
    blocks = md_to_notion_blocks(rgroup_top3_gemini(send_resp, **context))

    notion.blocks.children.append(
        block_id=PAGE_INFO['id'],
        children=blocks
    )


### 월별 일 평균 매출
def monthly_day_average_rev(joyplegameid:int, gameidx:str, databaseschema:str, **context):
    query = f"""
    select month
    , cast(sum(pricekrw) as int64) as `총매출`
    , max(day) `일 수`
    , cast( sum(pricekrw)/max(day) as int64) as `일평균 매출`
    from
    (select * , cast(format_date('%d',  logdatekst ) as int64) as day
    , format_date('%Y-%m',  logdatekst ) as month
    from `dataplatform-reporting.DataService.V_0317_0000_AuthAccountPerformance_V`
    where joyplegameid = {joyplegameid}
    and logdatekst>='2024-01-01'
    and logdatekst<=DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY)
    )

    group by 1
    order by 1
    """

    query_result =query_run_method('5_logterm_sales', query)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    gcs_path = f"{gameidx}/{timestamp}.parquet"
        
    saved_path = save_df_to_gcs(query_result, bucket, gcs_path)

    return saved_path

######### 월별 일 평균 매출 - 제미나이 코멘트 생성
def monthly_day_average_rev_gemini(joyplegameid: int, service_sub: str, path_monthly_day_average_rev:str, **context):

    from google.genai import Client
    genai_client = Client(vertexai=True,project=PROJECT_ID,location=LOCATION)

    query_result5_dailyAvgRevenue = load_df_from_gcs(bucket, path_monthly_day_average_rev)

    RUN_ID = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    LABELS = {"datascience_division_service": 'gameinsight_framework',
            "run_id": RUN_ID,
            "datascience_division_service_sub" : service_sub}
    
    response5_dailyAvgRevenue = genai_client.models.generate_content(
    model=MODEL_NAME,
    contents = f"""

    이번달은 KST로 어제날짜 기준이야. 어제날짜가 기준이라고 명시하지마

    이번달 일평균 매출에 대해 장기적인 흐름에 대해서 간단히 요약해줘.
    장기적인 관점으로도 비교하되, 최근 월들과도 비교해줘.
    작년과 올해를 비교해줘


    <서식 요구사항>
    1. 한문장당 줄바꿈 한번 해줘.
    2. 매출 1원단위까지 다 쓰지 말고 대략 말해줘. e.g. 8700만원
    3. 한 문장마다 노션의 마크다운 리스트 문법을 사용해줘. e.g. * 당월 매출은 이렇습니다.


    <월별 일평균 매출>
    {query_result5_dailyAvgRevenue}


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

    # 코멘트 출력
    return response5_dailyAvgRevenue.text


###### 과금그룹별 매출
def rgroup_rev_DOD(joyplegameid:int, gameidx:str, databaseschema:str, **context):
    query = f"""
    with sales_raw as ( ## 6208778
    select *
    , format_date('%Y-%m',  logdatekst ) as month
    , cast(format_date('%d',  logdatekst ) as int64) as day

    from `dataplatform-reporting.DataService.V_0317_0000_AuthAccountPerformance_V`
    where joyplegameid = {joyplegameid}
    and logdatekst>='2024-01-01'
    ),


    monthly_rev as (
    select authaccountname, logmonth, month, regmonth, ifnull(sum(pricekrw),0) as rev
    from
    (select *
    , format_date('%Y-%m-01',  logdatekst ) as logmonth
    , format_date('%Y-%m',  AuthAccountRegDateKST ) as regmonth
    from sales_raw
    where logdatekst>='2024-01-01'
    and day<=cast(format_date('%d',  DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY) ) as int64)  )
    group by 1,2,3,4
    ),

    r_group as (
    select *
    , case
    when rev>=10000000 then 'R0'
    when rev>=1000000  then 'R1'
    when rev>=100000   then 'R2'
    when rev>=10000    then 'R3'
    when rev>=1        then 'R4'
    when rev=0         then 'nonPU'
    else 'ETC' end as rgroup
    from monthly_rev
    ),

    final as (


    select a.*, c.rgroup
    from
    ## iap 매출 raw
    (select *, format_date('%Y-%m',  AuthAccountRegDateKST ) as regmonth
    , format_date('%Y',  AuthAccountRegDateKST ) as regyear
    from sales_raw
    where day<=cast(format_date('%d',  DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY) ) as int64)
    ) as a

    ## rgoup
    left join
    (select *
    from r_group) as c
    on a.authaccountname = c.authaccountname and a.month = c.month


    )

    select month
    , cast(sum(if(rgroup = 'R0' , pricekrw, 0)) as int64) as R0_rev
    , cast(sum(if(rgroup = 'R1' , pricekrw, 0)) as int64) as R1_rev
    , cast(sum(if(rgroup = 'R2' , pricekrw, 0)) as int64) as R2_rev
    , cast(sum(if(rgroup = 'R3' , pricekrw, 0)) as int64) as R3_rev
    , cast(sum(if(rgroup = 'R4' , pricekrw, 0)) as int64) as R4_rev
    , cast(sum(if(rgroup = 'nonPU' , pricekrw, 0)) as int64) as nonPU_rev
    , cast(sum(pricekrw) as int64) as ALL_rev
    , count(distinct if(rgroup='R0', authaccountname, null)) as R0_user
    , count(distinct if(rgroup='R1', authaccountname, null)) as R1_user
    , count(distinct if(rgroup='R2', authaccountname, null)) as R2_user
    , count(distinct if(rgroup='R3', authaccountname, null)) as R3_user
    , count(distinct if(rgroup='R4', authaccountname, null)) as R4_user
    , count(distinct if(rgroup in ('R0','R1','R2','R3','R4'), authaccountname, null)) as PU
    #, count(distinct if(rgroup in ('R0','R1','R2','R3','R4'), authaccountname, null))/count(distinct authaccountname) as PUR
    , count(distinct if(rgroup='nonPU', authaccountname, null)) as nonPU_user
    , count(distinct authaccountname) as ALL_user
    from final
    group by month
    order by month
    """

    query_result =query_run_method('5_logterm_sales', query)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    gcs_path = f"{gameidx}/{timestamp}.parquet"
        
    saved_path = save_df_to_gcs(query_result, bucket, gcs_path)

    return saved_path

####### 과금그룹별 총 매출
def rgroup_rev_total(joyplegameid:int, gameidx:str, databaseschema:str, **context):
    query = f"""

    with sales_raw as ( ## 6208778
    select *
    , format_date('%Y-%m',  logdatekst ) as month
    , cast(format_date('%d',  logdatekst ) as int64) as day

    from `dataplatform-reporting.DataService.V_0317_0000_AuthAccountPerformance_V`
    where joyplegameid = {joyplegameid}
    and logdatekst>='2024-01-01'
    ),


    monthly_rev as (
    select authaccountname, logmonth, month, regmonth, ifnull(sum(pricekrw),0) as rev
    from
    (select *
    , format_date('%Y-%m-01',  logdatekst ) as logmonth
    , format_date('%Y-%m',  AuthAccountRegDateKST ) as regmonth
    from sales_raw
    where logdatekst>='2024-01-01'
    and logdatekst<= DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY) )
    group by 1,2,3,4

    ),

    r_group as (
    select *
    , case
    when rev>=10000000 then 'R0'
    when rev>=1000000  then 'R1'
    when rev>=100000   then 'R2'
    when rev>=10000    then 'R3'
    when rev>=1        then 'R4'
    when rev=0         then 'nonPU'
    else 'ETC' end as rgroup
    from monthly_rev
    ),

    final as (


    select a.*, c.rgroup
    from
    ## iap 매출 raw
    (select *, format_date('%Y-%m',  AuthAccountRegDateKST ) as regmonth
    , format_date('%Y',  AuthAccountRegDateKST ) as regyear
    from sales_raw
    where logdatekst<= DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY)
    ) as a

    ## rgoup
    left join
    (select *
    from r_group) as c
    on a.authaccountname = c.authaccountname and a.month = c.month
    )

    select month
    , cast(sum(if(rgroup = 'R0' , pricekrw, 0)) as int64) as R0_rev
    , cast(sum(if(rgroup = 'R1' , pricekrw, 0)) as int64) as R1_rev
    , cast(sum(if(rgroup = 'R2' , pricekrw, 0)) as int64) as R2_rev
    , cast(sum(if(rgroup = 'R3' , pricekrw, 0)) as int64) as R3_rev
    , cast(sum(if(rgroup = 'R4' , pricekrw, 0)) as int64) as R4_rev
    , cast(sum(if(rgroup = 'nonPU' , pricekrw, 0)) as int64) as nonPU_rev
    , cast(sum(pricekrw) as int64) as ALL_rev
    , count(distinct if(rgroup='R0', authaccountname, null)) as R0_user
    , count(distinct if(rgroup='R1', authaccountname, null)) as R1_user
    , count(distinct if(rgroup='R2', authaccountname, null)) as R2_user
    , count(distinct if(rgroup='R3', authaccountname, null)) as R3_user
    , count(distinct if(rgroup='R4', authaccountname, null)) as R4_user
    , count(distinct if(rgroup in ('R0','R1','R2','R3','R4'), authaccountname, null)) as PU
    , count(distinct if(rgroup='nonPU', authaccountname, null)) as nonPU_user
    , count(distinct authaccountname) as ALL_user
    from final
    group by month
    order by month
    """

    query_result =query_run_method('5_logterm_sales', query)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    gcs_path = f"{gameidx}/{timestamp}.parquet"
        
    saved_path = save_df_to_gcs(query_result, bucket, gcs_path)

    return saved_path


####### 과금그룹별 총 매출 - 제미나이 코멘트 생성
def rgroup_rev_total_gemini(joyplegameid: int, service_sub: str, path_rgroup_rev_DOD:str, **context):

    from google.genai import Client
    genai_client = Client(vertexai=True,project=PROJECT_ID,location=LOCATION)

    query_result5_monthlyRgroupRevenue = load_df_from_gcs(bucket, path_rgroup_rev_DOD)

    RUN_ID = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    LABELS = {"datascience_division_service": 'gameinsight_framework',
            "run_id": RUN_ID,
            "datascience_division_service_sub" : service_sub}
    
    response5_monthlyRgroup = genai_client.models.generate_content(
    model=MODEL_NAME,
    contents = f"""

    맨 서두에 Bold 체로 전월대비 어떤 R그룹에서 증가했고 어떤 R그룹에서 감소했는지 써줘.
    그리고 간단하게 이전에 비해 트렌드가 어떤지 10줄정도로 요약해줘.
    PU 수와 매출이 증가했는지에 대해서도 알려줘.

    매출이 1억6천인데 16억이라고 쓰고 그러지마 잘 확인해


    <서식 요구사항>
    1. 한문장당 줄바꿈 한번 해줘.
    2. 매출 1원단위까지 다 쓰지 말고 대략 말해줘. 예) 1.54억, 750만원 9500만원
    3. 모든 숫자는 아라비아 숫자로 표기하고 천 단위마다 쉼표(,) 를 써줘. 예) 3123 → 3,123
    한글 숫자 표기(삼천백스물삼명 등)는 금지
    4. 한 문장마다 노션의 마크다운 리스트 문법을 사용해줘. e.g. * 당월 매출은 이렇습니다.

    < 데이터 설명>
    1. 월별 동기간 데이터야. 어제자 기준으로 이번달이 15일만 지났으면, 전월들도 15일까지만 집계돼
    2. 이번달 과금액 기준으로 R그룹을 나눴어.
    3. nonPU 는 이번달 무과금유저라서 PU 가 아니라는 뜻이야. PU 수 구할때 nonPU 는 더하면 안돼.
    4. R그룹 정의는 다음과 같아.
    R0 : 당월 과금액 1천만원 이상
    R1 : 당월 과금액 1천만원 미만 ~ 1백만원 이상
    R2 : 당월 과금액 1백만원 미만 ~ 10만원 이상
    R3 : 당월 과금액 10만원 미만 ~ 1만원 이상
    R4 : 당월 과금액 1만원 미만 ~ 0원 초과
    nonPU : 당월 무과금 유저

    <월별 R그룹별 매출과 PU>
    {query_result5_monthlyRgroupRevenue}


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

    # 코멘트 출력
    return response5_monthlyRgroup.text

## 가입연도별 매출
def rev_cohort_year(joyplegameid:int, gameidx:str, databaseschema:str, **context):
    query = f"""
    with sales_raw as (
    select *
    , format_date('%Y-%m',  logdatekst ) as month
    , format_date('%Y',  authaccountregdatekst ) as regyear
    , cast(format_date('%d',  logdatekst ) as int64) as day
    , EXTRACT(DAY FROM LAST_DAY(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)) as maxday
    from `dataplatform-reporting.DataService.V_0317_0000_AuthAccountPerformance_V`
    where joyplegameid = {joyplegameid}
    and logdatekst>='2024-01-01'
    and logdatekst<= DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY)
    and authaccountregdatekst is not null
    ),

    sales_this_month as (
    select month, regyear, (rev/day)*(maxday) as rev_pred
    from
    (select month, regyear, maxday, max(day) as day, sum(pricekrw) as rev
    from sales_raw
    where logdatekst>=DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
    and logdatekst <= DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY)
    group by 1,2,3)
    )

    ## 전월까지 실측
    select month, regyear, sum(pricekrw) as rev
    from sales_raw
    where logdatekst < DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
    group by 1,2

    union all

    ## 이번달 예측 (일할계산)
    select concat(month,'(예측)') as month , regyear, rev_pred as rev
    from sales_this_month

    /*
    union all

    ## 이번달 실측
    select concat(month,'(실측)') as month, regyear, rev from(
    select month, regyear, sum(pricekrw) as rev
    from sales_raw
    where logdatekst>=DATE_TRUNC(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
    and logdatekst <= DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY)
    group by 1,2)
    */
    """

    query_result= query_run_method('5_logterm_sales', query)

    #################### 가입연도별 매출을 피벗형태로 전처리
    df = query_result.copy()
    df['rev'] = pd.to_numeric(df['rev'], errors='coerce')#.fillna(0)
    df['regyear']  = pd.to_numeric(df['regyear'], errors='coerce').astype('Int64')

    # 시간 정렬용 파생(원본 month는 그대로)
    df['_month_dt'] = pd.to_datetime(df['month'], errors='coerce')

    # 피벗형태
    pv2 = (
        df.groupby(['month','regyear'])['rev'].sum()
        .unstack('regyear'#, fill_value=0
                )
    )

    # 행을 실제 날짜 순으로 정렬
    #pv2 = pv2.loc[pv2.assign(_order=pd.to_datetime(pv2.index)).sort_values('_order').index]
    pv2 = pv2.sort_index(axis=1)  # 열(연도) 오름차순

    # ✅ 위에 뜨는 'regyear' 배너 제거 (columns.name 제거)
    pv2.columns.name = None

    # 인덱스 이름을 'month'로 지정한 뒤 컬럼으로 리셋
    pv2 = pv2.rename_axis('month').reset_index()

    # (선택) 'month' 다음에 연도들 오도록 정렬 보장
    year_cols = [c for c in pv2.columns if c != 'month']
    year_cols_sorted = sorted(year_cols, key=lambda x: int(x))
    pv2 = pv2[['month'] + year_cols_sorted]

    # 총합 열 추가
    pv2['총합'] = pv2[year_cols_sorted].fillna(0).sum(axis=1)

    # xcom에 insert
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    gcs_path_1 = f"{gameidx}/{timestamp}_1.parquet"
    gcs_path_2 = f"{gameidx}/{timestamp}_2.parquet"
        
    path_regyearRevenue = save_df_to_gcs(query_result, bucket, gcs_path_1)
    path_regyearRevenue_pv2 = save_df_to_gcs(pv2, bucket, gcs_path_2)

    return path_regyearRevenue, path_regyearRevenue_pv2


def rev_cohort_year_gemini(joyplegameid: int, service_sub: str, path_rev_cohort_year:str, **context):

    from google.genai import Client
    genai_client = Client(vertexai=True,project=PROJECT_ID,location=LOCATION)

    pv2 = load_df_from_gcs(bucket, path_rev_cohort_year)

    pv2 = context['task_instance'].xcom_pull(
        task_ids = 'rev_cohort_year',
        key='rev_cohort_year'
    )

    RUN_ID = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    LABELS = {"datascience_division_service": 'gameinsight_framework',
            "run_id": RUN_ID,
            "datascience_division_service_sub" : service_sub}
    
    response5_regyearRevenue = genai_client.models.generate_content(
    model=MODEL_NAME,
    contents = f"""

    * 이번달 : KST 어제날짜 기준

    가입연도별 월 매출이야. 이번달은 일할계산해서 예측치로 두었어.
    먼저 서두에 각 연도별로 매출기여순서를 알려줘 (예시 : 20년>21년> ...)
    이번달 매출에 가입연도별 장기적인 매출이 어떻게 됐는지 간단히 요약해서 알려줘.
    그리고 이번년도 가입유저의 매출액이 크지만 그건 신규유저라서 향후엔 낮아질 수 있음을 고려해줘.


    <서식 요구사항>
    1. 한문장당 줄바꿈 한번 해줘.
    2. 매출 1원단위까지 다 쓰지 말고 대략 말해줘.(예: 27.5억 / 3,500만원 )
    3. 모든 숫자는 아라비아 숫자로 표기하고 천 단위마다 쉼표(,) 를 써줘. 예) 3123 → 3,123
    한글 숫자 표기(삼천백스물삼명 등)는 금지
    4. 한 문장마다 노션의 마크다운 리스트 문법을 사용해줘. e.g. * 당월 매출은 이렇습니다.


    <월별 R그룹별 매출과 PU>
    {pv2}


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

    # 코멘트 출력
    return response5_regyearRevenue.text


def monthly_day_average_rev_table_draw(gameidx:str, **context):
    df = context['task_instance'].xcom_pull(
        task_ids = 'monthly_day_average_rev',
        key='monthly_day_average_rev'
    )

    def render_table_image(
        df: pd.DataFrame,
        gameidx: str,
        out_path: str = "graph5_dailyAvgRevenueTable.png",
        dpi: int = 200,
        header_bg="#D9E1F2",
        border_color="#000000", ## 표 테두리 색깔
        cond_min="#5B9BD5",
        cond_mid="#FFFFFF",
        cond_max="#FF0000",
        font_family="NanumGothic",
    ):
        """
        DataFrame df -> Excel-like table PNG with:
        - Malgun Gothic font
        - Thousands separators for numeric columns
        - 3-color scale conditional formatting per numeric column
        - Auto-fit column widths by content length
        """
        # 0) 컬럼 순서 보장
        cols = ["month", "총매출", "일 수", "일평균 매출"]
        df = df.loc[:, cols].copy()

        # 1) 폰트 설정 (설치되어 있어야 함. 없으면 기본 폰트로 폴백됨)
        rcParams["font.family"] = font_family

        # 2) 문자열 변환 (천단위 콤마 / NaN 처리)
        display_df = df.copy()
        for c in cols[1:]:
            display_df[c] = display_df[c].apply(
                lambda x: "" if pd.isna(x) else f"{int(x):,}"
            )
        display_df["month"] = display_df["month"].astype(str).fillna("")

        # 3) 열 너비 계산(문자 수 기반 대략치: 문자폭≈7px, 좌우 패딩 포함)
        def col_pixel_width(series, header, is_numeric=False):
            # 숫자는 콤마 포함 표시 길이 기준
            max_chars = max([len(str(header))] + [len(str(s)) for s in series])
            # 숫자열은 우측정렬 & 약간 더 여유
            base = 10.0  # 1글자당 px 추정치
            padding = 24 if is_numeric else 20
            return int(max_chars * base + padding)

        col_widths = []
        for i, c in enumerate(cols):
            is_num = i > 0
            w = col_pixel_width(display_df[c], c, is_numeric=is_num)
            # 너무 좁거나 과도하게 넓지 않도록 가드
            w = max(w, 70)       # 최소
            w = min(w, 360)      # 최대
            col_widths.append(w)

        # 4) 행 높이/스타일
        header_h = 36  # 헤더 높이(px)
        row_h = 30     # 데이터 행 높이(px)
        n_rows = len(display_df)
        n_cols = len(cols)

        # 5) 전체 캔버스 크기(px)
        inner_w = sum(col_widths)
        inner_h = header_h + n_rows * row_h
        pad = 2  # 테두리 오차 방지용
        total_w = inner_w + pad
        total_h = inner_h + pad

        # 6) Figure 생성 (픽셀 -> 인치)
        fig_w_in = total_w / dpi
        fig_h_in = total_h / dpi
        fig, ax = plt.subplots(figsize=(fig_w_in, fig_h_in), dpi=dpi)
        ax.set_xlim(0, total_w)
        ax.set_ylim(total_h, 0)  # y축 아래로 증가하도록 뒤집음
        ax.axis("off")

        # 7) 컬러 보간 함수 (3색 스케일)
        def hex_to_rgb01(hx):
            hx = hx.lstrip("#")
            return tuple(int(hx[i:i+2], 16) / 255 for i in (0, 2, 4))

        c_min = np.array(hex_to_rgb01(cond_min))
        c_mid = np.array(hex_to_rgb01(cond_mid))
        c_max = np.array(hex_to_rgb01(cond_max))

        def interp_color(v, vmin, vmid, vmax):
            if pd.isna(v) or vmin is None or vmax is None or vmax == vmin:
                return (1, 1, 1)  # white
            if v <= vmid:
                t = 0.0 if vmid == vmin else (v - vmin) / (vmid - vmin)
                return tuple(c_min * (1 - t) + c_mid * t)
            else:
                t = 0.0 if vmax == vmid else (v - vmid) / (vmax - vmid)
                return tuple(c_mid * (1 - t) + c_max * t)

        # 8) 각 숫자열의 min/중앙값/ max 계산
        stats = {}
        for c in cols[1:]:
            series = pd.to_numeric(df[c], errors="coerce")
            if series.notna().any():
                vmin = float(series.min())
                vmax = float(series.max())
                vmid = float(series.quantile(0.5))
            else:
                vmin = vmid = vmax = None
            stats[c] = (vmin, vmid, vmax)

        # 9) 그리드(헤더 + 바디 셀) 그리기
        # 열 x 시작좌표 누적
        x_starts = np.cumsum([0] + col_widths[:-1]).tolist()
        # 헤더
        for j, c in enumerate(cols):
            x = x_starts[j]
            ## 표 테두리
            # linewith = 표 테두리 굵기
            rect = Rectangle((x, 0), col_widths[j], header_h,
                            facecolor=header_bg, edgecolor=border_color, linewidth=0.5)
            ax.add_patch(rect)
            ax.text(x + col_widths[j] / 2, header_h / 2 + 1,
                    c, ha="center", va="center", fontsize=5, fontweight="bold")

        # 바디
        for i in range(n_rows):
            y = header_h + i * row_h
            for j, c in enumerate(cols):
                x = x_starts[j]
                # 배경색 (month는 조건부서식 제외, 숫자열에만 적용)
                if j == 0:
                    bg = (1, 1, 1)
                else:
                    raw_val = pd.to_numeric(df.iloc[i, j], errors="coerce")
                    vmin, vmid, vmax = stats[c]
                    bg = interp_color(raw_val, vmin, vmid, vmax)

                rect = Rectangle((x, y), col_widths[j], row_h,
                                facecolor=bg, edgecolor=border_color, linewidth=0.5)
                ax.add_patch(rect)

                # 텍스트
                text = str(display_df.iloc[i, j])
                if j == 0:
                    # month: 좌측 정렬 + 좌우 패딩
                    ax.text(x + 8, y + row_h / 2,
                            text, ha="left", va="center", fontsize=5)
                else:
                    # 숫자: 우측 정렬
                    ax.text(x + col_widths[j] - 8, y + row_h / 2,
                            text, ha="right", va="center", fontsize=5)

        # 헤더 바로 위에 제목 추가 (왼쪽정렬)
        ax.text(0, -5, "월별 일평균 매출",
                ha="left", va="bottom", fontsize=8, fontweight="bold")
        
        # 10) 이미지 저장
        plt.savefig(out_path, bbox_inches="tight", pad_inches=0.1)
        plt.close(fig)

        blob = bucket.blob(f'{gameidx}/{out_path}')
        blob.upload_from_filename(out_path, content_type='image/png')

        # 메모리에 올라간 이미지 파일 삭제
        os.remove(out_path)

        return f'{gameidx}/{out_path}'
    
    gcs_path = render_table_image(df=df, gameidx=gameidx)
    return gcs_path


### 일 평균 매출 그래프 그리기
def monthly_day_average_rev_graph_draw(gameidx:str, **context):

    query_result5_dailyAvgRevenue = context['task_instance'].xcom_pull(
        task_ids = 'monthly_day_average_rev',
        key='monthly_day_average_rev'
    )

    sns.lineplot(x= query_result5_dailyAvgRevenue.columns[0],
             y=query_result5_dailyAvgRevenue.columns[3],
             data=query_result5_dailyAvgRevenue,
             marker="o")

    
    # y축 천 단위 구분 기호 넣기
    plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{int(x):,}"))

    # x축 눈금을 7개 단위로만 표시 (예: 1주일 간격)
    plt.xticks(query_result5_dailyAvgRevenue[query_result5_dailyAvgRevenue.columns[0]][::1], rotation=45)
    # x,y 축 글자 크기 조정
    plt.tick_params(axis="both", labelsize=10)

    # 표 제목
    plt.title("월별 일평균 매출")

    # y축 0부터 시작
    #plt.ylim(0, None)   # None이면 최대값은 자동으로 맞춰짐
    # y축 보조선
    plt.grid(axis='y', linestyle='--', alpha=0.7) # alpha=투명도

    # x,y축 제거
    plt.xlabel(None)
    plt.ylabel(None)

    #plt.show()
    # 그래프 안잘리게
    plt.tight_layout()


    # 향후 노션업로드하기 위해 저장
    # #print(os.getcwd()) 이 곳에 저장되고, colab 환경이라 좌측 폴더모양 누르면 png 있음.
    # 세션 종료시 자동으로 삭제됨
    file_path5_dailyAvgRevenueLine = "graph5_dailyAvgRevenueLine.png"
    plt.savefig(file_path5_dailyAvgRevenueLine, dpi=120) # dpi : 해상도
    plt.close()

    blob = bucket.blob(f'{gameidx}/{file_path5_dailyAvgRevenueLine}')
    blob.upload_from_filename(file_path5_dailyAvgRevenueLine)

    # 메모리에 올라간 이미지 파일 삭제
    os.remove(file_path5_dailyAvgRevenueLine)

    return f'{gameidx}/{file_path5_dailyAvgRevenueLine}'


def monthly_day_average_merge_graph(gameidx:str, **context):
    # 1) 파일 경로
    p1 = monthly_day_average_rev_table_draw(gameidx, **context)   # 첫 번째 이미지
    p2 = monthly_day_average_rev_graph_draw(gameidx, **context)   # 두 번째 이미지
    save_to = 'graph5_dailyAvgRevenue.png'  # 저장 경로

    # 2) 이미지 열기 (투명 보존 위해 RGBA)
    im1 = Image.open(p1).convert("RGBA")
    im2 = Image.open(p2).convert("RGBA")

    # ---- [옵션 A] 원본 크기 유지 + 세로 패딩으로 높이 맞추기 (권장: 왜곡 없음) ----
    target_h = max(im1.height, im2.height)

    def pad_to_height(img, h, bg=(255, 255, 255, 0)):  # 투명 배경: 알파 0
        if img.height == h:
            return img
        canvas = Image.new("RGBA", (img.width, h), bg)
        # 가운데 정렬로 붙이기 (위에 맞추려면 y=0)
        y = (h - img.height) // 2
        canvas.paste(img, (0, y))
        return canvas

    im1_p = pad_to_height(im1, target_h)
    im2_p = pad_to_height(im2, target_h)

    gap = 0  # 이미지 사이 여백(px). 필요하면 20 등으로 변경
    bg = (255, 255, 255, 0)  # 전체 배경(투명). 흰색 원하면 (255,255,255,255)

    out = Image.new("RGBA", (im1_p.width + gap + im2_p.width, target_h), bg)
    out.paste(im1_p, (0, 0), im1_p)
    out.paste(im2_p, (im1_p.width + gap, 0), im2_p)

    # PNG로 저장
    blob = bucket.blob(f'{gameidx}/{save_to}')
    blob.upload_from_filename(save_to)

    # 메모리에 올라간 이미지 파일 삭제
    os.remove(save_to)

    return f'{gameidx}/{save_to}'


#### 월별 R 그룹별 매출 동기간 표
def rgroup_rev_DOD_table_draw(gameidx:str, **context):
    query_result5_monthlyRgroupRevenue = context['task_instance'].xcom_pull(
        task_ids = 'rgroup_rev_DOD',
        key='rgroup_rev_DOD'
    )

    df = query_result5_monthlyRgroupRevenue.iloc[:, [0,1,2,3,4,5,7]]
    
    df = df.rename(
    columns = {"month" : "month",
               "R0_rev" : "R0",
               "R1_rev" : "R1",
               "R2_rev" : "R2",
               "R3_rev" : "R3",
               "R4_rev" : "R4",
               "ALL_rev" : "총합",
               }
    )

    def render_table_image(
        df: pd.DataFrame,
        out_path: str = "graph5_monthlyRgroupRevenue.png",
        dpi: int = 200,
        header_bg="#D9E1F2",
        border_color="#000000", ## 표 테두리 색깔
        cond_min="#5B9BD5",
        cond_mid="#FFFFFF",
        cond_max="#FF0000",
        font_family="NanumGothic",
    ):


        """
        DataFrame df -> Excel-like table PNG with:
        - Malgun Gothic font
        - Thousands separators for numeric columns
        - 3-color scale conditional formatting per numeric column
        - Auto-fit column widths by content length
        """
        # 0) 컬럼 순서 보장
        cols = ["month", "R0", "R1", "R2", "R3", "R4", "총합"]
        df = df.loc[:, cols].copy()

        # 1) 폰트 설정 (설치되어 있어야 함. 없으면 기본 폰트로 폴백됨)
        rcParams["font.family"] = font_family

        # 2) 문자열 변환 (천단위 콤마 / NaN 처리)
        display_df = df.copy()
        for c in cols[1:]:
            display_df[c] = display_df[c].apply(
                lambda x: "" if pd.isna(x) else f"{int(x):,}"
            )
        display_df["month"] = display_df["month"].astype(str).fillna("")

        # 3) 열 너비 계산(문자 수 기반 대략치: 문자폭≈7px, 좌우 패딩 포함)
        def col_pixel_width(series, header, is_numeric=False):
            # 숫자는 콤마 포함 표시 길이 기준
            max_chars = max([len(str(header))] + [len(str(s)) for s in series])
            # 숫자열은 우측정렬 & 약간 더 여유
            base = 10.0  # 1글자당 px 추정치
            padding = 24 if is_numeric else 20
            return int(max_chars * base + padding)

        col_widths = []
        for i, c in enumerate(cols):
            is_num = i > 0
            w = col_pixel_width(display_df[c], c, is_numeric=is_num)
            # 너무 좁거나 과도하게 넓지 않도록 가드
            w = max(w, 70)       # 최소
            w = min(w, 360)      # 최대
            col_widths.append(w)

        # 4) 행 높이/스타일
        header_h = 36  # 헤더 높이(px)
        row_h = 30     # 데이터 행 높이(px)
        n_rows = len(display_df)
        n_cols = len(cols)

        # 5) 전체 캔버스 크기(px)
        inner_w = sum(col_widths)
        inner_h = header_h + n_rows * row_h
        pad = 2  # 테두리 오차 방지용
        total_w = inner_w + pad
        total_h = inner_h + pad

        # 6) Figure 생성 (픽셀 -> 인치)
        fig_w_in = total_w / dpi
        fig_h_in = total_h / dpi
        fig, ax = plt.subplots(figsize=(fig_w_in, fig_h_in), dpi=dpi)
        ax.set_xlim(0, total_w)
        ax.set_ylim(total_h, 0)  # y축 아래로 증가하도록 뒤집음
        ax.axis("off")

        # 7) 컬러 보간 함수 (3색 스케일)
        def hex_to_rgb01(hx):
            hx = hx.lstrip("#")
            return tuple(int(hx[i:i+2], 16) / 255 for i in (0, 2, 4))

        c_min = np.array(hex_to_rgb01(cond_min))
        c_mid = np.array(hex_to_rgb01(cond_mid))
        c_max = np.array(hex_to_rgb01(cond_max))

        def interp_color(v, vmin, vmid, vmax):
            if pd.isna(v) or vmin is None or vmax is None or vmax == vmin:
                return (1, 1, 1)  # white
            if v <= vmid:
                t = 0.0 if vmid == vmin else (v - vmin) / (vmid - vmin)
                return tuple(c_min * (1 - t) + c_mid * t)
            else:
                t = 0.0 if vmax == vmid else (v - vmid) / (vmax - vmid)
                return tuple(c_mid * (1 - t) + c_max * t)

        # 8) 각 숫자열의 min/중앙값/ max 계산
        stats = {}
        for c in cols[1:]:
            series = pd.to_numeric(df[c], errors="coerce")
            if series.notna().any():
                vmin = float(series.min())
                vmax = float(series.max())
                vmid = float(series.quantile(0.5))
            else:
                vmin = vmid = vmax = None
            stats[c] = (vmin, vmid, vmax)

        # 9) 그리드(헤더 + 바디 셀) 그리기
        # 열 x 시작좌표 누적
        x_starts = np.cumsum([0] + col_widths[:-1]).tolist()
        # 헤더
        for j, c in enumerate(cols):
            x = x_starts[j]
            ## 표 테두리
            # linewith = 표 테두리 굵기
            rect = Rectangle((x, 0), col_widths[j], header_h,
                            facecolor=header_bg, edgecolor=border_color, linewidth=0.5)
            ax.add_patch(rect)
            ax.text(x + col_widths[j] / 2, header_h / 2 + 1,
                    c, ha="center", va="center", fontsize=5, fontweight="bold")

        # 바디
        for i in range(n_rows):
            y = header_h + i * row_h
            for j, c in enumerate(cols):
                x = x_starts[j]
                # 배경색 (month는 조건부서식 제외, 숫자열에만 적용)
                if j == 0:
                    bg = (1, 1, 1)
                else:
                    raw_val = pd.to_numeric(df.iloc[i, j], errors="coerce")
                    vmin, vmid, vmax = stats[c]
                    bg = interp_color(raw_val, vmin, vmid, vmax)

                rect = Rectangle((x, y), col_widths[j], row_h,
                                facecolor=bg, edgecolor=border_color, linewidth=0.5)
                ax.add_patch(rect)

                # 텍스트
                text = str(display_df.iloc[i, j])
                if j == 0:
                    # month: 좌측 정렬 + 좌우 패딩
                    ax.text(x + 8, y + row_h / 2,
                            text, ha="left", va="center", fontsize=5)
                else:
                    # 숫자: 우측 정렬
                    ax.text(x + col_widths[j] - 8, y + row_h / 2,
                            text, ha="right", va="center", fontsize=5)

        # 헤더 바로 위에 제목 추가 (왼쪽정렬)
        ax.text(0, -5, "월별 R그룹별 매출(동기간)",
                ha="left", va="bottom", fontsize=8, fontweight="bold")
        # 10) 이미지 저장
        plt.savefig(out_path, bbox_inches="tight", pad_inches=0.2)
        plt.close(fig)
        
        blob = bucket.blob(f'{gameidx}/{out_path}')
        blob.upload_from_filename(out_path, content_type='image/png')

        # 메모리에 올라간 이미지 파일 삭제
        os.remove(out_path)

        return f'{gameidx}/{out_path}'
    
    gcs_path = render_table_image(df=df, gameidx=gameidx)
    return gcs_path



#### 월별 R 그룹별 PU 수 동기간 표
def rgroup_pu_DOD_table_draw(gameidx:str, **context):
    query_result5_monthlyRgroupRevenue = context['task_instance'].xcom_pull(
        task_ids = 'rgroup_rev_DOD',
        key='rgroup_rev_DOD'
    )
    df = query_result5_monthlyRgroupRevenue.iloc[:, [0,8,9,10,11,12,14,15,13]]

    df = df.rename(
        columns = {"month" : "month",
                "R0_user" : "R0",
                "R1_user" : "R1",
                "R2_user" : "R2",
                "R3_user" : "R3",
                "R4_user" : "R4",
                "nonPU_user" : "nonPU",
                "PU" : "PU",
                "ALL_user" : "총합",
                }
    )

    def render_table_image(
        df: pd.DataFrame,
        out_path: str = "graph5_monthlyRgroupPU.png",
        dpi: int = 200,
        header_bg="#D9E1F2",
        border_color="#000000", ## 표 테두리 색깔
        cond_min="#5B9BD5",
        cond_mid="#FFFFFF",
        cond_max="#FF0000",
        font_family="NanumGothic",
    ):


        """
        DataFrame df -> Excel-like table PNG with:
        - Malgun Gothic font
        - Thousands separators for numeric columns
        - 3-color scale conditional formatting per numeric column
        - Auto-fit column widths by content length
        """
        # 0) 컬럼 순서 보장
        cols = ["month", "R0", "R1", "R2", "R3", "R4", "nonPU", "PU", "총합"]
        df = df.loc[:, cols].copy()

        # 1) 폰트 설정 (설치되어 있어야 함. 없으면 기본 폰트로 폴백됨)
        rcParams["font.family"] = font_family

        # # 2) 문자열 변환 (천단위 콤마 / NaN 처리)
        # display_df = df.copy()
        # for c in cols[1:]:
        #     display_df[c] = display_df[c].apply(
        #         lambda x: "" if pd.isna(x) else f"{int(x):,}"
        #     )
        # display_df["month"] = display_df["month"].astype(str).fillna("")

        # 2) 문자열 변환 (천단위 콤마 / NaN 처리)
        display_df = df.copy()
        for c in cols[1:]:
            if c == "PUR":   # PUR 열만 퍼센트로 표시 ( PUR 컬럼 없음 )
                display_df[c] = display_df[c].apply(
                    lambda x: "" if pd.isna(x) else f"{x:.1%}"  # 소수점 1자리까지 퍼센트
                )
            else:
                display_df[c] = display_df[c].apply(
                    lambda x: "" if pd.isna(x) else f"{int(x):,}"
                )
        display_df["month"] = display_df["month"].astype(str).fillna("")




        # 3) 열 너비 계산(문자 수 기반 대략치: 문자폭≈7px, 좌우 패딩 포함)
        def col_pixel_width(series, header, is_numeric=False):
            # 숫자는 콤마 포함 표시 길이 기준
            max_chars = max([len(str(header))] + [len(str(s)) for s in series])
            # 숫자열은 우측정렬 & 약간 더 여유
            base = 10.0  # 1글자당 px 추정치
            padding = 24 if is_numeric else 20
            return int(max_chars * base + padding)

        col_widths = []
        for i, c in enumerate(cols):
            is_num = i > 0
            w = col_pixel_width(display_df[c], c, is_numeric=is_num)
            # 너무 좁거나 과도하게 넓지 않도록 가드
            w = max(w, 70)       # 최소
            w = min(w, 360)      # 최대
            col_widths.append(w)

        # 4) 행 높이/스타일
        header_h = 36  # 헤더 높이(px)
        row_h = 30     # 데이터 행 높이(px)
        n_rows = len(display_df)
        n_cols = len(cols)

        # 5) 전체 캔버스 크기(px)
        inner_w = sum(col_widths)
        inner_h = header_h + n_rows * row_h
        pad = 2  # 테두리 오차 방지용
        total_w = inner_w + pad
        total_h = inner_h + pad

        # 6) Figure 생성 (픽셀 -> 인치)
        fig_w_in = total_w / dpi
        fig_h_in = total_h / dpi
        fig, ax = plt.subplots(figsize=(fig_w_in, fig_h_in), dpi=dpi)
        ax.set_xlim(0, total_w)
        ax.set_ylim(total_h, 0)  # y축 아래로 증가하도록 뒤집음
        ax.axis("off")

        # 7) 컬러 보간 함수 (3색 스케일)
        def hex_to_rgb01(hx):
            hx = hx.lstrip("#")
            return tuple(int(hx[i:i+2], 16) / 255 for i in (0, 2, 4))

        c_min = np.array(hex_to_rgb01(cond_min))
        c_mid = np.array(hex_to_rgb01(cond_mid))
        c_max = np.array(hex_to_rgb01(cond_max))

        def interp_color(v, vmin, vmid, vmax):
            if pd.isna(v) or vmin is None or vmax is None or vmax == vmin:
                return (1, 1, 1)  # white
            if v <= vmid:
                t = 0.0 if vmid == vmin else (v - vmin) / (vmid - vmin)
                return tuple(c_min * (1 - t) + c_mid * t)
            else:
                t = 0.0 if vmax == vmid else (v - vmid) / (vmax - vmid)
                return tuple(c_mid * (1 - t) + c_max * t)

        # 8) 각 숫자열의 min/중앙값/ max 계산
        stats = {}
        for c in cols[1:]:
            series = pd.to_numeric(df[c], errors="coerce")
            if series.notna().any():
                vmin = float(series.min())
                vmax = float(series.max())
                vmid = float(series.quantile(0.5))
            else:
                vmin = vmid = vmax = None
            stats[c] = (vmin, vmid, vmax)

        # 9) 그리드(헤더 + 바디 셀) 그리기
        # 열 x 시작좌표 누적
        x_starts = np.cumsum([0] + col_widths[:-1]).tolist()
        # 헤더
        for j, c in enumerate(cols):
            x = x_starts[j]
            ## 표 테두리
            # linewith = 표 테두리 굵기
            rect = Rectangle((x, 0), col_widths[j], header_h,
                            facecolor=header_bg, edgecolor=border_color, linewidth=0.5)
            ax.add_patch(rect)
            ax.text(x + col_widths[j] / 2, header_h / 2 + 1,
                    c, ha="center", va="center", fontsize=5, fontweight="bold")

        # 바디
        for i in range(n_rows):
            y = header_h + i * row_h
            for j, c in enumerate(cols):
                x = x_starts[j]
                # 배경색 (month는 조건부서식 제외, 숫자열에만 적용)
                if j == 0:
                    bg = (1, 1, 1)
                else:
                    raw_val = pd.to_numeric(df.iloc[i, j], errors="coerce")
                    vmin, vmid, vmax = stats[c]
                    bg = interp_color(raw_val, vmin, vmid, vmax)

                rect = Rectangle((x, y), col_widths[j], row_h,
                                facecolor=bg, edgecolor=border_color, linewidth=0.5)
                ax.add_patch(rect)

                # 텍스트
                text = str(display_df.iloc[i, j])
                if j == 0:
                    # month: 좌측 정렬 + 좌우 패딩
                    ax.text(x + 8, y + row_h / 2,
                            text, ha="left", va="center", fontsize=5)
                else:
                    # 숫자: 우측 정렬
                    ax.text(x + col_widths[j] - 8, y + row_h / 2,
                            text, ha="right", va="center", fontsize=5)
        # 헤더 바로 위에 제목 추가 (왼쪽정렬)
        ax.text(0, -5, "월별 R그룹별 PU수(동기간)",
                ha="left", va="bottom", fontsize=8, fontweight="bold")

        # 10) 이미지 저장
        plt.savefig(out_path, bbox_inches="tight", pad_inches=0.2)
        plt.close(fig)
        
        blob = bucket.blob(f'{gameidx}/{out_path}')
        blob.upload_from_filename(out_path, content_type='image/png')

        # 메모리에 올라간 이미지 파일 삭제
        os.remove(out_path)

        return f'{gameidx}/{out_path}'
    
    gcs_path = render_table_image(df=df, gameidx=gameidx)
    return gcs_path


def merge_rgroup_rev_pu_ALL_table(joyplegameid: int, gameidx: str, **context):
    p1 = rgroup_rev_DOD_table_draw(gameidx, **context)
    p2 = rgroup_pu_DOD_table_draw(gameidx, **context)

    # 2) 이미지 열기 (투명 보존 위해 RGBA)
    blob1 = bucket.blob(p1)
    blob2 = bucket.blob(p2)

    im1 = blob1.download_as_bytes()
    im2 = blob2.download_as_bytes()

    im1 = Image.open(BytesIO(im1)).convert("RGBA")
    im2 = Image.open(BytesIO(im2)).convert("RGBA")

    target_h = max(im1.height, im2.height)

    def pad_to_height(img, h, bg=(255, 255, 255, 0)):  # 투명 배경: 알파 0
        if img.height == h:
            return img
        canvas = Image.new("RGBA", (img.width, h), bg)
        # 가운데 정렬로 붙이기 (위에 맞추려면 y=0)
        y = (h - img.height) // 2
        canvas.paste(img, (0, y))
        return canvas

    im1_p = pad_to_height(im1, target_h)
    im2_p = pad_to_height(im2, target_h)

    gap = 0  # 이미지 사이 여백(px). 필요하면 20 등으로 변경
    bg = (255, 255, 255, 0)  # 전체 배경(투명). 흰색 원하면 (255,255,255,255)

    out = Image.new("RGBA", (im1_p.width + gap + im2_p.width, target_h), bg)
    out.paste(im1_p, (0, 0), im1_p)
    out.paste(im2_p, (im1_p.width + gap, 0), im2_p)

    # 3) GCS에 저장
    output_buffer = BytesIO()
    out.save(output_buffer, format='PNG')
    output_buffer.seek(0)

    # GCS 경로
    gcs_path = f'{gameidx}/graph5_monthlyRgroupALL.png'
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(output_buffer.getvalue(), content_type='image/png')

    return gcs_path


def merge_rgroup_rev_pu_table(gameidx:str, **context):
    p1 = rgroup_rev_DOD_table_draw(gameidx, **context) # 첫 번째 이미지
    p2 = rgroup_pu_DOD_table_draw(gameidx, **context)   # 두 번째 이미지

    # 2) 이미지 열기 (투명 보존 위해 RGBA)
    blob1 = bucket.blob(p1)
    blob2 = bucket.blob(p2)

    im1 = blob1.download_as_bytes()
    im2 = blob2.download_as_bytes()

    im1 = Image.open(BytesIO(im1)).convert("RGBA")
    im2 = Image.open(BytesIO(im2)).convert("RGBA") 

    # ---- [옵션 A] 원본 크기 유지 + 세로 패딩으로 높이 맞추기 (권장: 왜곡 없음) ----
    target_h = max(im1.height, im2.height)

    def pad_to_height(img, h, bg=(255, 255, 255, 0)):  # 투명 배경: 알파 0
        if img.height == h:
            return img
        canvas = Image.new("RGBA", (img.width, h), bg)
        # 가운데 정렬로 붙이기 (위에 맞추려면 y=0)
        y = (h - img.height) // 2
        canvas.paste(img, (0, y))
        return canvas

    im1_p = pad_to_height(im1, target_h)
    im2_p = pad_to_height(im2, target_h)

    gap = 0  # 이미지 사이 여백(px). 필요하면 20 등으로 변경
    bg = (255, 255, 255, 0)  # 전체 배경(투명). 흰색 원하면 (255,255,255,255)

    out = Image.new("RGBA", (im1_p.width + gap + im2_p.width, target_h), bg)
    out.paste(im1_p, (0, 0), im1_p)
    out.paste(im2_p, (im1_p.width + gap, 0), im2_p)

    # 3) GCS에 저장
    output_buffer = BytesIO()
    out.save(output_buffer, format='PNG')
    output_buffer.seek(0)

    # GCS 경로
    gcs_path = f'{gameidx}/graph5_monthlyRgroup.png'
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(output_buffer.getvalue(), content_type='image/png')

    return gcs_path

###############################################################



#### 월별 R그룹 매출 전체기간 표
def rgroup_rev_total_table_draw(gameidx:str, **context):
    query_result5_monthlyRgroupRevenue = context['task_instance'].xcom_pull(
        task_ids = 'rgroup_rev_total',
        key='rgroup_rev_total'
    )

    df = query_result5_monthlyRgroupRevenue.iloc[:, [0,1,2,3,4,5,7]]

    df = df.rename(
        columns = {"month" : "month",
                "R0_rev" : "R0",
                "R1_rev" : "R1",
                "R2_rev" : "R2",
                "R3_rev" : "R3",
                "R4_rev" : "R4",
                "ALL_rev" : "총합",
                }
    )

    def render_table_image(
        df: pd.DataFrame,
        out_path: str = "graph5_monthlyRgroupRevenueALL.png",
        dpi: int = 200,
        header_bg="#D9E1F2",
        border_color="#000000", ## 표 테두리 색깔
        cond_min="#5B9BD5",
        cond_mid="#FFFFFF",
        cond_max="#FF0000",
        font_family="NanumGothic",
    ):


        """
        DataFrame df -> Excel-like table PNG with:
        - Malgun Gothic font
        - Thousands separators for numeric columns
        - 3-color scale conditional formatting per numeric column
        - Auto-fit column widths by content length
        """
        # 0) 컬럼 순서 보장
        cols = ["month", "R0", "R1", "R2", "R3", "R4", "총합"]
        df = df.loc[:, cols].copy()

        # 1) 폰트 설정 (설치되어 있어야 함. 없으면 기본 폰트로 폴백됨)
        rcParams["font.family"] = font_family

        # 2) 문자열 변환 (천단위 콤마 / NaN 처리)
        display_df = df.copy()
        for c in cols[1:]:
            display_df[c] = display_df[c].apply(
                lambda x: "" if pd.isna(x) else f"{int(x):,}"
            )
        display_df["month"] = display_df["month"].astype(str).fillna("")

        # 3) 열 너비 계산(문자 수 기반 대략치: 문자폭≈7px, 좌우 패딩 포함)
        def col_pixel_width(series, header, is_numeric=False):
            # 숫자는 콤마 포함 표시 길이 기준
            max_chars = max([len(str(header))] + [len(str(s)) for s in series])
            # 숫자열은 우측정렬 & 약간 더 여유
            base = 10.0  # 1글자당 px 추정치
            padding = 24 if is_numeric else 20
            return int(max_chars * base + padding)

        col_widths = []
        for i, c in enumerate(cols):
            is_num = i > 0
            w = col_pixel_width(display_df[c], c, is_numeric=is_num)
            # 너무 좁거나 과도하게 넓지 않도록 가드
            w = max(w, 70)       # 최소
            w = min(w, 360)      # 최대
            col_widths.append(w)

        # 4) 행 높이/스타일
        header_h = 36  # 헤더 높이(px)
        row_h = 30     # 데이터 행 높이(px)
        n_rows = len(display_df)
        n_cols = len(cols)

        # 5) 전체 캔버스 크기(px)
        inner_w = sum(col_widths)
        inner_h = header_h + n_rows * row_h
        pad = 2  # 테두리 오차 방지용
        total_w = inner_w + pad
        total_h = inner_h + pad

        # 6) Figure 생성 (픽셀 -> 인치)
        fig_w_in = total_w / dpi
        fig_h_in = total_h / dpi
        fig, ax = plt.subplots(figsize=(fig_w_in, fig_h_in), dpi=dpi)
        ax.set_xlim(0, total_w)
        ax.set_ylim(total_h, 0)  # y축 아래로 증가하도록 뒤집음
        ax.axis("off")

        # 7) 컬러 보간 함수 (3색 스케일)
        def hex_to_rgb01(hx):
            hx = hx.lstrip("#")
            return tuple(int(hx[i:i+2], 16) / 255 for i in (0, 2, 4))

        c_min = np.array(hex_to_rgb01(cond_min))
        c_mid = np.array(hex_to_rgb01(cond_mid))
        c_max = np.array(hex_to_rgb01(cond_max))

        def interp_color(v, vmin, vmid, vmax):
            if pd.isna(v) or vmin is None or vmax is None or vmax == vmin:
                return (1, 1, 1)  # white
            if v <= vmid:
                t = 0.0 if vmid == vmin else (v - vmin) / (vmid - vmin)
                return tuple(c_min * (1 - t) + c_mid * t)
            else:
                t = 0.0 if vmax == vmid else (v - vmid) / (vmax - vmid)
                return tuple(c_mid * (1 - t) + c_max * t)

        # 8) 각 숫자열의 min/중앙값/ max 계산
        stats = {}
        for c in cols[1:]:
            series = pd.to_numeric(df[c], errors="coerce")
            if series.notna().any():
                vmin = float(series.min())
                vmax = float(series.max())
                vmid = float(series.quantile(0.5))
            else:
                vmin = vmid = vmax = None
            stats[c] = (vmin, vmid, vmax)

        # 9) 그리드(헤더 + 바디 셀) 그리기
        # 열 x 시작좌표 누적
        x_starts = np.cumsum([0] + col_widths[:-1]).tolist()
        # 헤더
        for j, c in enumerate(cols):
            x = x_starts[j]
            ## 표 테두리
            # linewith = 표 테두리 굵기
            rect = Rectangle((x, 0), col_widths[j], header_h,
                            facecolor=header_bg, edgecolor=border_color, linewidth=0.5)
            ax.add_patch(rect)
            ax.text(x + col_widths[j] / 2, header_h / 2 + 1,
                    c, ha="center", va="center", fontsize=5, fontweight="bold")

        # 바디
        for i in range(n_rows):
            y = header_h + i * row_h
            for j, c in enumerate(cols):
                x = x_starts[j]
                # 배경색 (month는 조건부서식 제외, 숫자열에만 적용)
                if j == 0:
                    bg = (1, 1, 1)
                else:
                    raw_val = pd.to_numeric(df.iloc[i, j], errors="coerce")
                    vmin, vmid, vmax = stats[c]
                    bg = interp_color(raw_val, vmin, vmid, vmax)

                rect = Rectangle((x, y), col_widths[j], row_h,
                                facecolor=bg, edgecolor=border_color, linewidth=0.5)
                ax.add_patch(rect)

                # 텍스트
                text = str(display_df.iloc[i, j])
                if j == 0:
                    # month: 좌측 정렬 + 좌우 패딩
                    ax.text(x + 8, y + row_h / 2,
                            text, ha="left", va="center", fontsize=5)
                else:
                    # 숫자: 우측 정렬
                    ax.text(x + col_widths[j] - 8, y + row_h / 2,
                            text, ha="right", va="center", fontsize=5)

        # 헤더 바로 위에 제목 추가 (왼쪽정렬)
        ax.text(0, -5, "월별 R그룹별 매출(동기간)",
                ha="left", va="bottom", fontsize=8, fontweight="bold")
        # 10) 이미지 저장
        plt.savefig(out_path, bbox_inches="tight", pad_inches=0.2)
        plt.close(fig)
        
        blob = bucket.blob(f'{gameidx}/{out_path}')
        blob.upload_from_filename(out_path, content_type='image/png')

        # 메모리에 올라간 이미지 파일 삭제
        os.remove(out_path)

        return f'{gameidx}/{out_path}'
    
    gcs_path = render_table_image(df=df, gameidx=gameidx)
    return gcs_path


def rgroup_pu_total_table_draw(gameidx:str, **context):
    query_result5_monthlyRgroupRevenue = context['task_instance'].xcom_pull(
        task_ids = 'rgroup_rev_total',
        key='rgroup_rev_total'
    )

    df = query_result5_monthlyRgroupRevenue.iloc[:, [0,8,9,10,11,12,14,15,13]]

    df = df.rename(
        columns = {"month" : "month",
                "R0_rev" : "R0",
                "R1_rev" : "R1",
                "R2_rev" : "R2",
                "R3_rev" : "R3",
                "R4_rev" : "R4",
                "ALL_rev" : "총합",
                }
        )

    def render_table_image(
        df: pd.DataFrame,
        out_path: str = "graph5_monthlyRgroupPUALL.png",
        dpi: int = 200,
        header_bg="#D9E1F2",
        border_color="#000000", ## 표 테두리 색깔
        cond_min="#5B9BD5",
        cond_mid="#FFFFFF",
        cond_max="#FF0000",
        font_family="NanumGothic",
        ):


        """
        DataFrame df -> Excel-like table PNG with:
        - Malgun Gothic font
        - Thousands separators for numeric columns
        - 3-color scale conditional formatting per numeric column
        - Auto-fit column widths by content length
        """
        # 0) 컬럼 순서 보장
        cols = ["month", "R0", "R1", "R2", "R3", "R4", "nonPU", "PU", "총합"]
        df = df.loc[:, cols].copy()

        # 1) 폰트 설정 (설치되어 있어야 함. 없으면 기본 폰트로 폴백됨)
        rcParams["font.family"] = font_family

        # # 2) 문자열 변환 (천단위 콤마 / NaN 처리)
        # display_df = df.copy()
        # for c in cols[1:]:
        #     display_df[c] = display_df[c].apply(
        #         lambda x: "" if pd.isna(x) else f"{int(x):,}"
        #     )
        # display_df["month"] = display_df["month"].astype(str).fillna("")

        # 2) 문자열 변환 (천단위 콤마 / NaN 처리)
        display_df = df.copy()
        for c in cols[1:]:
            if c == "PUR":   # PUR 열만 퍼센트로 표시 ( PUR 컬럼 없음 )
                display_df[c] = display_df[c].apply(
                    lambda x: "" if pd.isna(x) else f"{x:.1%}"  # 소수점 1자리까지 퍼센트
                )
            else:
                display_df[c] = display_df[c].apply(
                    lambda x: "" if pd.isna(x) else f"{int(x):,}"
                )
        display_df["month"] = display_df["month"].astype(str).fillna("")




        # 3) 열 너비 계산(문자 수 기반 대략치: 문자폭≈7px, 좌우 패딩 포함)
        def col_pixel_width(series, header, is_numeric=False):
            # 숫자는 콤마 포함 표시 길이 기준
            max_chars = max([len(str(header))] + [len(str(s)) for s in series])
            # 숫자열은 우측정렬 & 약간 더 여유
            base = 10.0  # 1글자당 px 추정치
            padding = 24 if is_numeric else 20
            return int(max_chars * base + padding)

        col_widths = []
        for i, c in enumerate(cols):
            is_num = i > 0
            w = col_pixel_width(display_df[c], c, is_numeric=is_num)
            # 너무 좁거나 과도하게 넓지 않도록 가드
            w = max(w, 70)       # 최소
            w = min(w, 360)      # 최대
            col_widths.append(w)

        # 4) 행 높이/스타일
        header_h = 36  # 헤더 높이(px)
        row_h = 30     # 데이터 행 높이(px)
        n_rows = len(display_df)
        n_cols = len(cols)

        # 5) 전체 캔버스 크기(px)
        inner_w = sum(col_widths)
        inner_h = header_h + n_rows * row_h
        pad = 2  # 테두리 오차 방지용
        total_w = inner_w + pad
        total_h = inner_h + pad

        # 6) Figure 생성 (픽셀 -> 인치)
        fig_w_in = total_w / dpi
        fig_h_in = total_h / dpi
        fig, ax = plt.subplots(figsize=(fig_w_in, fig_h_in), dpi=dpi)
        ax.set_xlim(0, total_w)
        ax.set_ylim(total_h, 0)  # y축 아래로 증가하도록 뒤집음
        ax.axis("off")

        # 7) 컬러 보간 함수 (3색 스케일)
        def hex_to_rgb01(hx):
            hx = hx.lstrip("#")
            return tuple(int(hx[i:i+2], 16) / 255 for i in (0, 2, 4))

        c_min = np.array(hex_to_rgb01(cond_min))
        c_mid = np.array(hex_to_rgb01(cond_mid))
        c_max = np.array(hex_to_rgb01(cond_max))

        def interp_color(v, vmin, vmid, vmax):
            if pd.isna(v) or vmin is None or vmax is None or vmax == vmin:
                return (1, 1, 1)  # white
            if v <= vmid:
                t = 0.0 if vmid == vmin else (v - vmin) / (vmid - vmin)
                return tuple(c_min * (1 - t) + c_mid * t)
            else:
                t = 0.0 if vmax == vmid else (v - vmid) / (vmax - vmid)
                return tuple(c_mid * (1 - t) + c_max * t)

        # 8) 각 숫자열의 min/중앙값/ max 계산
        stats = {}
        for c in cols[1:]:
            series = pd.to_numeric(df[c], errors="coerce")
            if series.notna().any():
                vmin = float(series.min())
                vmax = float(series.max())
                vmid = float(series.quantile(0.5))
            else:
                vmin = vmid = vmax = None
            stats[c] = (vmin, vmid, vmax)

        # 9) 그리드(헤더 + 바디 셀) 그리기
        # 열 x 시작좌표 누적
        x_starts = np.cumsum([0] + col_widths[:-1]).tolist()
        # 헤더
        for j, c in enumerate(cols):
            x = x_starts[j]
            ## 표 테두리
            # linewith = 표 테두리 굵기
            rect = Rectangle((x, 0), col_widths[j], header_h,
                            facecolor=header_bg, edgecolor=border_color, linewidth=0.5)
            ax.add_patch(rect)
            ax.text(x + col_widths[j] / 2, header_h / 2 + 1,
                    c, ha="center", va="center", fontsize=5, fontweight="bold")

        # 바디
        for i in range(n_rows):
            y = header_h + i * row_h
            for j, c in enumerate(cols):
                x = x_starts[j]
                # 배경색 (month는 조건부서식 제외, 숫자열에만 적용)
                if j == 0:
                    bg = (1, 1, 1)
                else:
                    raw_val = pd.to_numeric(df.iloc[i, j], errors="coerce")
                    vmin, vmid, vmax = stats[c]
                    bg = interp_color(raw_val, vmin, vmid, vmax)

                rect = Rectangle((x, y), col_widths[j], row_h,
                                facecolor=bg, edgecolor=border_color, linewidth=0.5)
                ax.add_patch(rect)

                # 텍스트
                text = str(display_df.iloc[i, j])
                if j == 0:
                    # month: 좌측 정렬 + 좌우 패딩
                    ax.text(x + 8, y + row_h / 2,
                            text, ha="left", va="center", fontsize=5)
                else:
                    # 숫자: 우측 정렬
                    ax.text(x + col_widths[j] - 8, y + row_h / 2,
                            text, ha="right", va="center", fontsize=5)
        # 헤더 바로 위에 제목 추가 (왼쪽정렬)
        ax.text(0, -5, "월별 R그룹별 PU수(동기간)",
                ha="left", va="bottom", fontsize=8, fontweight="bold")

        # 10) 이미지 저장
        plt.savefig(out_path, bbox_inches="tight", pad_inches=0.2)
        plt.close(fig)
        
        blob = bucket.blob(f'{gameidx}/{out_path}')
        blob.upload_from_filename(out_path, content_type='image/png')

        # 메모리에 올라간 이미지 파일 삭제
        os.remove(out_path)

        return f'{gameidx}/{out_path}'
    
    gcs_path = render_table_image(df=df, gameidx=gameidx)
    return gcs_path


#### 월별 R 그룹별 매출, PU 표 합치기
def merge_rgroup_total_rev_pu_table(joyplegameid: int, gameidx: str, **context):
    p1 = rgroup_rev_total_table_draw(gameidx, **context)
    p2 = rgroup_pu_total_table_draw(gameidx, **context)

    # 2) 이미지 열기 (투명 보존 위해 RGBA)
    blob1 = bucket.blob(p1)
    blob2 = bucket.blob(p2)

    im1 = blob1.download_as_bytes()
    im2 = blob2.download_as_bytes()

    im1 = Image.open(BytesIO(im1)).convert("RGBA")
    im2 = Image.open(BytesIO(im2)).convert("RGBA")

    # ---- [옵션 A] 원본 크기 유지 + 세로 패딩으로 높이 맞추기 (권장: 왜곡 없음) ----
    target_h = max(im1.height, im2.height)

    def pad_to_height(img, h, bg=(255, 255, 255, 0)):  # 투명 배경: 알파 0
        if img.height == h:
            return img
        canvas = Image.new("RGBA", (img.width, h), bg)
        # 가운데 정렬로 붙이기 (위에 맞추려면 y=0)
        y = (h - img.height) // 2
        canvas.paste(img, (0, y))
        return canvas

    im1_p = pad_to_height(im1, target_h)
    im2_p = pad_to_height(im2, target_h)

    gap = 0  # 이미지 사이 여백(px). 필요하면 20 등으로 변경
    bg = (255, 255, 255, 0)  # 전체 배경(투명). 흰색 원하면 (255,255,255,255)

    out = Image.new("RGBA", (im1_p.width + gap + im2_p.width, target_h), bg)
    out.paste(im1_p, (0, 0), im1_p)
    out.paste(im2_p, (im1_p.width + gap, 0), im2_p)

    # 3) GCS에 저장
    output_buffer = BytesIO()
    out.save(output_buffer, format='PNG')
    output_buffer.seek(0)

    # GCS 경로
    gcs_path = f'{gameidx}/graph5_monthlyRgroupHap.png'
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(output_buffer.getvalue(), content_type='image/png')

    return gcs_path


######### 가입연도별 매출 표

def cohort_rev_table_draw(gameidx:str, **context):
    df = context['task_instance'].xcom_pull(
        task_ids = 'rev_cohort_year',
        key='rev_cohort_year'
    )

    def render_table_image(
        df: pd.DataFrame,
        out_path: str = "graph5_regyearRevenue.png",
        dpi: int = 200,
        header_bg="#D9E1F2",
        border_color="#000000", ## 표 테두리 색깔
        cond_min="#5B9BD5",
        cond_mid="#FFFFFF",
        cond_max="#FF0000",
        font_family="NanumGothic",
    ):


        """
        DataFrame df -> Excel-like table PNG with:
        - Malgun Gothic font
        - Thousands separators for numeric columns
        - 3-color scale conditional formatting per numeric column
        - Auto-fit column widths by content length
        """
        # 0) 컬럼 순서 보장
        cols = df.columns.tolist()     # astype(str) 제거
        # cols = ["month", "R0", "R1", "R2", "R3", "R4", "총합"]
        df = df.loc[:, cols].copy()

        # 1) 폰트 설정 (설치되어 있어야 함. 없으면 기본 폰트로 폴백됨)
        rcParams["font.family"] = font_family

        # 2) 문자열 변환 (천단위 콤마 / NaN 처리)
        display_df = df.copy()
        for c in cols[1:]:
            display_df[c] = display_df[c].apply(
                lambda x: "" if pd.isna(x) else f"{int(x):,}"
            )
        display_df["month",'regyear'] = display_df["month"].astype(str).fillna("")

        # 3) 열 너비 계산(문자 수 기반 대략치: 문자폭≈7px, 좌우 패딩 포함)
        def col_pixel_width(series, header, is_numeric=False):
            # 숫자는 콤마 포함 표시 길이 기준
            max_chars = max([len(str(header))] + [len(str(s)) for s in series])
            # 숫자열은 우측정렬 & 약간 더 여유
            base = 10.0  # 1글자당 px 추정치
            padding = 24 if is_numeric else 20
            return int(max_chars * base + padding)

        col_widths = []
        for i, c in enumerate(cols):
            is_num = i > 0
            w = col_pixel_width(display_df[c], c, is_numeric=is_num)
            # 너무 좁거나 과도하게 넓지 않도록 가드
            w = max(w, 70)       # 최소
            w = min(w, 360)      # 최대
            col_widths.append(w)

        # 4) 행 높이/스타일
        header_h = 36  # 헤더 높이(px)
        row_h = 30     # 데이터 행 높이(px)
        n_rows = len(display_df)
        n_cols = len(cols)

        # 5) 전체 캔버스 크기(px)
        inner_w = sum(col_widths)
        inner_h = header_h + n_rows * row_h
        pad = 2  # 테두리 오차 방지용
        total_w = inner_w + pad
        total_h = inner_h + pad

        # 6) Figure 생성 (픽셀 -> 인치)
        fig_w_in = total_w / dpi
        fig_h_in = total_h / dpi
        fig, ax = plt.subplots(figsize=(fig_w_in, fig_h_in), dpi=dpi)
        ax.set_xlim(0, total_w)
        ax.set_ylim(total_h, 0)  # y축 아래로 증가하도록 뒤집음
        ax.axis("off")

        # 7) 컬러 보간 함수 (3색 스케일)
        def hex_to_rgb01(hx):
            hx = hx.lstrip("#")
            return tuple(int(hx[i:i+2], 16) / 255 for i in (0, 2, 4))

        c_min = np.array(hex_to_rgb01(cond_min))
        c_mid = np.array(hex_to_rgb01(cond_mid))
        c_max = np.array(hex_to_rgb01(cond_max))

        def interp_color(v, vmin, vmid, vmax):
            if pd.isna(v) or vmin is None or vmax is None or vmax == vmin:
                return (1, 1, 1)  # white
            if v <= vmid:
                t = 0.0 if vmid == vmin else (v - vmin) / (vmid - vmin)
                return tuple(c_min * (1 - t) + c_mid * t)
            else:
                t = 0.0 if vmax == vmid else (v - vmid) / (vmax - vmid)
                return tuple(c_mid * (1 - t) + c_max * t)

        # 8) 각 숫자열의 min/중앙값/ max 계산
        stats = {}
        for c in cols[1:]:
            series = pd.to_numeric(df[c], errors="coerce")
            if series.notna().any():
                vmin = float(series.min())
                vmax = float(series.max())
                vmid = float(series.quantile(0.5))
            else:
                vmin = vmid = vmax = None
            stats[c] = (vmin, vmid, vmax)

        # 9) 그리드(헤더 + 바디 셀) 그리기
        # 열 x 시작좌표 누적
        x_starts = np.cumsum([0] + col_widths[:-1]).tolist()
        # 헤더
        for j, c in enumerate(cols):
            x = x_starts[j]
            ## 표 테두리
            # linewith = 표 테두리 굵기
            rect = Rectangle((x, 0), col_widths[j], header_h,
                            facecolor=header_bg, edgecolor=border_color, linewidth=0.5)
            ax.add_patch(rect)
            ax.text(x + col_widths[j] / 2, header_h / 2 + 1,
                    c, ha="center", va="center", fontsize=5, fontweight="bold")

        # 바디
        for i in range(n_rows):
            y = header_h + i * row_h
            for j, c in enumerate(cols):
                x = x_starts[j]
                # 배경색 (month는 조건부서식 제외, 숫자열에만 적용)
                if j == 0:
                    bg = (1, 1, 1)
                else:
                    raw_val = pd.to_numeric(df.iloc[i, j], errors="coerce")
                    vmin, vmid, vmax = stats[c]
                    bg = interp_color(raw_val, vmin, vmid, vmax)

                rect = Rectangle((x, y), col_widths[j], row_h,
                                facecolor=bg, edgecolor=border_color, linewidth=0.5)
                ax.add_patch(rect)

                # 텍스트
                text = str(display_df.iloc[i, j])
                if j == 0:
                    # month: 좌측 정렬 + 좌우 패딩
                    ax.text(x + 8, y + row_h / 2,
                            text, ha="left", va="center", fontsize=5)
                else:
                    # 숫자: 우측 정렬
                    ax.text(x + col_widths[j] - 8, y + row_h / 2,
                            text, ha="right", va="center", fontsize=5)

        # 헤더 바로 위에 제목 추가 (왼쪽정렬)
        ax.text(0, -5,  "가입연도별 월 매출",
                ha="left", va="bottom", fontsize=8, fontweight="bold")

        # 10) 이미지 저장
        plt.savefig(out_path, bbox_inches="tight", pad_inches=0.2)
        plt.close(fig)
        
        blob = bucket.blob(f'{gameidx}/{out_path}')
        blob.upload_from_filename(out_path, content_type='image/png')

        # 메모리에 올라간 이미지 파일 삭제
        os.remove(out_path)

        return f'{gameidx}/{out_path}'
    

########### 장기적 매출 현황 업로드 to 노션
def longterm_rev_upload_notion(joyplegameid: int, gameidx:str, service_sub:str, **context):

    PAGE_INFO=context['task_instance'].xcom_pull(
        task_ids = 'make_gameframework_notion_page',
        key='page_info'
    )

    notion.blocks.children.append(
        PAGE_INFO['id'],
        children=[
            {
                "object": "block",
                "type": "heading_2",
                "heading_2": {
                    "rich_text": [{"type": "text", "text": {"content": "5. 장기 매출 트렌드" }}]
                },
            }
        ],
    )

    notion.blocks.children.append(
        PAGE_INFO['id'],
        children=[
            {
                "object": "block",
                "type": "heading_3",
                "heading_3": {
                    "rich_text": [{"type": "text", "text": {"content": "\n(1) 일평균 매출" }}]
                },
            }
        ],
    )

    # 공통 헤더
    headers_json = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json"
    }

    try:
        gcs_path = f'{gameidx}/filePath5_dailyAvgRevenue.png'
        blob = bucket.blob(gcs_path)
        image_bytes = blob.download_as_bytes()
        filename = 'filePath5_dailyAvgRevenue.png'
        print(f"✓ GCS 이미지 다운로드 성공 : {gcs_path}")
    except Exception as e:
        print(f"❌ GCS 다운로드 실패: {str(e)}")
        raise

    try:   
        # 1) 업로드 객체 생성 (file_upload 생성)
        create_url = "https://api.notion.com/v1/file_uploads"
        payload = {
            "filename": filename,
            "content_type": "image/png"
        }

        resp = requests.post(create_url, headers=headers_json, data=json.dumps(payload))
        resp.raise_for_status()
        file_upload = resp.json()
        file_upload_id = file_upload["id"]
        print(f"✓ Notion 업로드 객체 생성: {file_upload_id}")

        # 2) 파일 바이너리 전송 (multipart/form-data)
        # ✅ 로컬 파일 대신 BytesIO 사용
        send_url = f"https://api.notion.com/v1/file_uploads/{file_upload_id}/send"
        files = {"file": (filename, BytesIO(image_bytes), "image/png")}
        headers_send = {
            "Authorization": f"Bearer {NOTION_TOKEN}",
            "Notion-Version": NOTION_VERSION
        }
        send_resp = requests.post(send_url, headers=headers_send, files=files)
        send_resp.raise_for_status()
        print(f"✓ 파일 전송 완료: {filename}")

        # 3) Notion 페이지에 이미지 블록 추가
        append_url = f"https://api.notion.com/v1/blocks/{PAGE_INFO['id']}/children"
        append_payload = {
            "children": [
                {
                    "object": "block",
                    "type": "image",
                    "image": {
                        "type": "file_upload",
                        "file_upload": {"id": file_upload_id},
                        # 캡션을 달고 싶다면 아래 주석 해제
                        # "caption": [{"type": "text", "text": {"content": "자동 업로드된 그래프"}}]
                    }
                }
            ]
        }

        append_resp = requests.patch(
            append_url, headers=headers_json, data=json.dumps(append_payload)
        )
        append_resp.raise_for_status()
        print(f"✅ Notion에 이미지 추가 완료: {filename}")

    except requests.exceptions.RequestException as e:
        print(f"❌ Notion API 에러: {str(e)}")
        raise
    except Exception as e:
        print(f"❌ 예기치 않은 에러: {str(e)}")
        raise

    query_result5_dailyAvgRevenue = context['task_instance'].xcom_pull(
        task_ids = 'monthly_day_average_rev',
        key='monthly_day_average_rev'
    )

    resp = df_to_notion_table_under_toggle(
        notion=notion,
        page_id=PAGE_INFO['id'],
        df=query_result5_dailyAvgRevenue,
        toggle_title="📊 로데이터 - 월별 일평균 매출",
        max_first_batch_rows=90,
        batch_size=100,
    )

    blocks = md_to_notion_blocks(monthly_day_average_rev_gemini(joyplegameid, service_sub, **context))

    notion.blocks.children.append(
        block_id=PAGE_INFO['id'],
        children=blocks
    )


########### 월별 R그룹별 매출 PU 수
def longterm_rev_upload_notion(joyplegameid: int, gameidx:str, service_sub:str, **context):

    PAGE_INFO=context['task_instance'].xcom_pull(
        task_ids = 'make_gameframework_notion_page',
        key='page_info'
    )


    notion.blocks.children.append(
        PAGE_INFO['id'],
        children=[
            {
                "object": "block",
                "type": "heading_3",
                "heading_3": {
                    "rich_text": [{"type": "text", "text": {"content": "\n(2) 월별 R그룹별 현황 " }}]
                },
            }
        ],
    )

    notion.blocks.children.append(
        PAGE_INFO['id'],
        children=[
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [{"type": "text", "text": {"content": " ** 당월 과금액 기준 R그룹 입니다. " }}]
                },
            }
        ],
    )

    
    # 공통 헤더
    headers_json = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json"
    }

    try:
        gcs_path = f'{gameidx}/filePath5_monthlyRgroupHap.png'
        blob = bucket.blob(gcs_path)
        image_bytes = blob.download_as_bytes()
        filename = 'filePath5_monthlyRgroupHap.png'
        print(f"✓ GCS 이미지 다운로드 성공 : {gcs_path}")
    except Exception as e:
        print(f"❌ GCS 다운로드 실패: {str(e)}")
        raise

    try:   
        # 1) 업로드 객체 생성 (file_upload 생성)
        create_url = "https://api.notion.com/v1/file_uploads"
        payload = {
            "filename": filename,
            "content_type": "image/png"
        }

        resp = requests.post(create_url, headers=headers_json, data=json.dumps(payload))
        resp.raise_for_status()
        file_upload = resp.json()
        file_upload_id = file_upload["id"]
        print(f"✓ Notion 업로드 객체 생성: {file_upload_id}")

        # 2) 파일 바이너리 전송 (multipart/form-data)
        # ✅ 로컬 파일 대신 BytesIO 사용
        send_url = f"https://api.notion.com/v1/file_uploads/{file_upload_id}/send"
        files = {"file": (filename, BytesIO(image_bytes), "image/png")}
        headers_send = {
            "Authorization": f"Bearer {NOTION_TOKEN}",
            "Notion-Version": NOTION_VERSION
        }
        send_resp = requests.post(send_url, headers=headers_send, files=files)
        send_resp.raise_for_status()
        print(f"✓ 파일 전송 완료: {filename}")

        # 3) Notion 페이지에 이미지 블록 추가
        append_url = f"https://api.notion.com/v1/blocks/{PAGE_INFO['id']}/children"
        append_payload = {
            "children": [
                {
                    "object": "block",
                    "type": "image",
                    "image": {
                        "type": "file_upload",
                        "file_upload": {"id": file_upload_id},
                        # 캡션을 달고 싶다면 아래 주석 해제
                        # "caption": [{"type": "text", "text": {"content": "자동 업로드된 그래프"}}]
                    }
                }
            ]
        }

        append_resp = requests.patch(
            append_url, headers=headers_json, data=json.dumps(append_payload)
        )
        append_resp.raise_for_status()
        print(f"✅ Notion에 이미지 추가 완료: {filename}")

    except requests.exceptions.RequestException as e:
        print(f"❌ Notion API 에러: {str(e)}")
        raise
    except Exception as e:
        print(f"❌ 예기치 않은 에러: {str(e)}")
        raise


    query_result5_monthlyRgroupRevenueALL = context['task_instance'].xcom_pull(
        task_ids = 'rgroup_rev_total',
        key='rgroup_rev_total'
    )

    query_result5_monthlyRgroupRevenue = context['task_instance'].xcom_pull(
        task_ids = 'rgroup_rev_DOD',
        key='rgroup_rev_DOD'
    )


    resp = df_to_notion_table_under_toggle(
        notion=notion,
        page_id= PAGE_INFO['id'],
        df=query_result5_monthlyRgroupRevenueALL,
        toggle_title="📊 로데이터 - 월별 R그룹 매출(전체기간) ",
        max_first_batch_rows=90,
        batch_size=100,
    )

    resp = df_to_notion_table_under_toggle(
        notion=notion,
        page_id= PAGE_INFO['id'],
        df=query_result5_monthlyRgroupRevenue,
        toggle_title="📊 로데이터 - 월별 R그룹 매출(동기간) ",
        max_first_batch_rows=90,
        batch_size=100,
    )

    notion.blocks.children.append(
        PAGE_INFO['id'],
        children=[
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [{"type": "text", "text": {"content": " ** 동기간 R그룹 비교에 대한 해석입니다.  \n " }}]
                },
            }
        ],
    )

    ## 프롬프트
    blocks = md_to_notion_blocks(rgroup_rev_total_gemini(joyplegameid, service_sub, **context))
    notion.blocks.children.append(
        block_id=PAGE_INFO['id'],
        children=blocks
    )

    return True


############## 가입연도 매출 데이터 
def cohort_rev_upload_notion(joyplegameid:int, gameidx:str, service_sub:str, **context):

    PAGE_INFO=context['task_instance'].xcom_pull(
        task_ids = 'make_gameframework_notion_page',
        key='page_info'
    )

    notion.blocks.children.append(
        PAGE_INFO['id'],
        children=[
            {
                "object": "block",
                "type": "heading_3",
                "heading_3": {
                    "rich_text": [{"type": "text", "text": {"content": "\n(3) 가입연도별 매출 " }}]
                },
            }
        ],
    )

    # 공통 헤더
    headers_json = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json"
    }

    try:
        gcs_path = f'{gameidx}/file_path5_regyearRevenue.png'
        blob = bucket.blob(gcs_path)
        image_bytes = blob.download_as_bytes()
        filename = 'file_path5_regyearRevenue.png'
        print(f"✓ GCS 이미지 다운로드 성공 : {gcs_path}")
    except Exception as e:
        print(f"❌ GCS 다운로드 실패: {str(e)}")
        raise

    try:   
        # 1) 업로드 객체 생성 (file_upload 생성)
        create_url = "https://api.notion.com/v1/file_uploads"
        payload = {
            "filename": filename,
            "content_type": "image/png"
        }

        resp = requests.post(create_url, headers=headers_json, data=json.dumps(payload))
        resp.raise_for_status()
        file_upload = resp.json()
        file_upload_id = file_upload["id"]
        print(f"✓ Notion 업로드 객체 생성: {file_upload_id}")

        # 2) 파일 바이너리 전송 (multipart/form-data)
        # ✅ 로컬 파일 대신 BytesIO 사용
        send_url = f"https://api.notion.com/v1/file_uploads/{file_upload_id}/send"
        files = {"file": (filename, BytesIO(image_bytes), "image/png")}
        headers_send = {
            "Authorization": f"Bearer {NOTION_TOKEN}",
            "Notion-Version": NOTION_VERSION
        }
        send_resp = requests.post(send_url, headers=headers_send, files=files)
        send_resp.raise_for_status()
        print(f"✓ 파일 전송 완료: {filename}")

        # 3) Notion 페이지에 이미지 블록 추가
        append_url = f"https://api.notion.com/v1/blocks/{PAGE_INFO['id']}/children"
        append_payload = {
            "children": [
                {
                    "object": "block",
                    "type": "image",
                    "image": {
                        "type": "file_upload",
                        "file_upload": {"id": file_upload_id},
                        # 캡션을 달고 싶다면 아래 주석 해제
                        # "caption": [{"type": "text", "text": {"content": "자동 업로드된 그래프"}}]
                    }
                }
            ]
        }

        append_resp = requests.patch(
            append_url, headers=headers_json, data=json.dumps(append_payload)
        )
        append_resp.raise_for_status()
        print(f"✅ Notion에 이미지 추가 완료: {filename}")

    except requests.exceptions.RequestException as e:
        print(f"❌ Notion API 에러: {str(e)}")
        raise
    except Exception as e:
        print(f"❌ 예기치 않은 에러: {str(e)}")
        raise


    query_result5_regyearRevenue = context['task_instance'].xcom_pull(
        task_ids = 'rev_cohort_year',
        key='rev_cohort_year_original'
    )

    resp = df_to_notion_table_under_toggle(
        notion=notion,
        page_id=PAGE_INFO['id'],
        df=query_result5_regyearRevenue,
        toggle_title="📊 로데이터 - 가입연도별 매출 ",
        max_first_batch_rows=90,
        batch_size=100,
    )

    blocks = md_to_notion_blocks(rev_cohort_year_gemini(joyplegameid, service_sub, **context))
    notion.blocks.children.append(
        block_id=PAGE_INFO['id'],
        children=blocks
    )

    return True

## 신규 유저 회수 현황
## 6_newuser_roas

def result6_monthlyROAS(joyplegameid:int, gameidx:str, databaseschema:str, **context):
    
    query= """
    WITH revraw AS(
    select JoypleGameID, Month
    , concat("D_",date_diff(date_sub(current_date('Asia/Seoul'), interval 1 day), date_sub(cast(concat(Month, '-01') as date), interval 1 day), day)) as matured_daydiff
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
        where JoypleGameID = 133
        and RegdateAuthAccountDateKST >= DATE_SUB(DATE(CONCAT(FORMAT_DATE('%Y-%m', DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY)),'-01')), INTERVAL 24 MONTH)
        and RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -8 DAY) AS Date)
    group by JoypleGameID, RegdateAuthAccountDateKST
    ) group by JoypleGameID, month
    )


    , final AS(
    select  JoypleGameID,  Month, matured_daydiff, RU,
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
    where joyplegameid = 133
    and cmpgndate >= DATE_SUB(DATE(CONCAT(FORMAT_DATE('%Y-%m', DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY)),'-01')), INTERVAL 24 MONTH)
    and cmpgndate <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -8 DAY) AS Date)
    group by joyplegameid,  format_date('%Y-%m', cmpgndate)

    ) as b
    on a.joyplegameid = b.joyplegameid
    and a.month = b.month
    )

    select month as `가입월`,
    matured_daydiff as `지표확정 최대기간`,
    cost_exclude_credit as `마케팅 비용`,
    ru*d1_ltv/cost_exclude_credit as `ROAS D1`,
    ru*d3_ltv/cost_exclude_credit as `ROAS D3`,
    ru*d7_ltv/cost_exclude_credit as `ROAS D7`,
    ru*d14_ltv/cost_exclude_credit as `ROAS D14`,
    ru*d30_ltv/cost_exclude_credit as `ROAS D30`,
    ru*d60_ltv/cost_exclude_credit as `ROAS D60`,
    ru*d90_ltv/cost_exclude_credit as `ROAS D90`,
    ru*d120_ltv/cost_exclude_credit as `ROAS D120`,
    ru*d150_ltv/cost_exclude_credit as `ROAS D150`,
    ru*d180_ltv/cost_exclude_credit as `ROAS D180`,
    ru*d210_ltv/cost_exclude_credit as `ROAS D210`,
    ru*d240_ltv/cost_exclude_credit as `ROAS D240`,
    ru*d270_ltv/cost_exclude_credit as `ROAS D270`,
    ru*d300_ltv/cost_exclude_credit as `ROAS D300`,
    ru*d330_ltv/cost_exclude_credit as `ROAS D330`,
    ru*d360_ltv/cost_exclude_credit as `ROAS D360`,
    ru*d14_ltv_p/cost_exclude_credit as `ROAS D14 예측치`,
    ru*d30_ltv_p/cost_exclude_credit as `ROAS D30 예측치`,
    ru*d60_ltv_p/cost_exclude_credit as `ROAS D60 예측치`,
    ru*d90_ltv_p/cost_exclude_credit as `ROAS D90 예측치`,
    ru*d120_ltv_p/cost_exclude_credit as `ROAS D120 예측치`,
    ru*d150_ltv_p/cost_exclude_credit as `ROAS D150 예측치`,
    ru*d180_ltv_p/cost_exclude_credit as `ROAS D180 예측치`,
    ru*d210_ltv_p/cost_exclude_credit as `ROAS D210 예측치`,
    ru*d240_ltv_p/cost_exclude_credit as `ROAS D240 예측치`,
    ru*d270_ltv_p/cost_exclude_credit as `ROAS D270 예측치`,
    ru*d300_ltv_p/cost_exclude_credit as `ROAS D300 예측치`,
    ru*d330_ltv_p/cost_exclude_credit as `ROAS D330 예측치`,
    ru*d360_ltv_p/cost_exclude_credit as `ROAS D360 예측치`
    from final2
    order by `가입월`
    """

    query_result6_monthlyROAS =query_run_method('6_newuser_roas', query)
    query_result6_monthlyROAS['지표확정 최대기간'] = (
    query_result6_monthlyROAS['지표확정 최대기간'].astype(str).str.replace('_', '', regex=False)
    )

    context['task_instance'].xcom_push(key='result6_monthlyROAS', value=query_result6_monthlyROAS)

    return True

def result6_pLTV(joyplegameid:int, gameidx:str, databaseschema:str, **context):

    ## pLTV D360
    query = f"""

    with perfo_raw AS(
    select a.*
    , b.countrycode, b.os
    , b.gcat, b.mediacategory, b.class, b.media, b.adsetname, b.adname, b.optim, b.oscam, b.geocam, b.targetgroup
    from(
    select *,
    case when logdatekst < current_date('Asia/Seoul') then pricekrw else daypred_low end as combined_rev_low,
    case when logdatekst < current_date('Asia/Seoul') then pricekrw else daypred_upp end as combined_rev_upp,
    FROM `data-science-division-216308.VU.Performance_pLTV`
    where authaccountregdatekst >= DATE_SUB(DATE(CONCAT(FORMAT_DATE('%Y-%m', DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY)),'-01')), INTERVAL 24 MONTH)
    and authaccountregdatekst <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -8 DAY) AS Date)
    and JoypleGameID = 133
    ) as a
    left join (select  *
    from `dataplatform-reporting.DataService.V_0316_0000_AuthAccountInfo_V`
    where JoypleGameID = 133
    ) as b
    on a.authaccountname = b.authaccountname
    and a.joyplegameid = b.joyplegameid
    )

    select format_date('%Y-%m',AuthAccountRegDateKST) as `가입월`
        ,count(distinct if(daysfromregisterdate = 0, authaccountname, null)) as RU
        ,sum(if(daysfromregisterdate <= 360, combined_rev, null)) as `매출 D360 예측치`
        , max(authaccountregdatekst) as `최대 가입일자`
    from perfo_raw
    group by joyplegameid, format_date('%Y-%m',AuthAccountRegDateKST)

    """
    query_result6_pLTV =query_run_method('6_newuser_roas', query)

    context['task_instance'].xcom_push(key='result6_pLTV', value=query_result6_pLTV)

    return True

##### 복귀 유저 데이터
def result6_return(joyplegameid:int, gameidx:str, databaseschema:str, **context):

    query = f"""
    with raw AS(
    select *
    , sum(d90diff) over(partition by joyplegameid, authaccountname order by logdatekst) as cum_d90diff
    from(
    select *
    , date_diff(logdatekst,AuthAccountLastAccessBeforeDateKST, day ) as daydiff_beforeaccess   -- authaccountlastaccessbeforedatekst : Access 기준으로 로깅
    , case when  date_diff(logdatekst,AuthAccountLastAccessBeforeDateKST, day )  >= 90 then 1 else 0  end as d90diff
    FROM `dataplatform-reporting.DataService.T_0317_0000_AuthAccountPerformance_V`
    WHERE joyplegameid = 133
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
            where joyplegameid = 133
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
    and (
        -- (joyplegameid = 131 and regmonth not in ('2024-04','2024-05','2024-06')) or joyplegameid in (133,30001,30003)
        joyplegameid = 133
        )
    )
    where rownum <= 6 -- 최근 6개월
    group by joyplegameid
    )

    select a.regmonth as `가입월`
        , a.RU
        , a.RU_all
        , a.cost_exclude_credit as `마케팅 비용`
        , rev_D360_return as `복귀유저 매출 D360`
        , status as `데이터 완성 여부`
    , rev_D360_return/cost_exclude_credit as `복귀유저 ROAS D360`
    , case when status = 'mature' then rev_D360_return/cost_exclude_credit
        else b.d360_return_roas_med end as `복귀유저 ROAS D360 예측치`
    from final  as a
    left join return_user_proas as b
    on a.joyplegameid = b.joyplegameid
    """

    query_result6_return =query_run_method('6_newuser_roas', query)

    context['task_instance'].xcom_push(key='result6_return', value=query_result6_return)

    return True

### 수수료 적용 BEP 계산
def result6_BEP(joyplegameid:int, gameidx:str, databaseschema:str, **context):

    query = f"""
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
    where joyplegameid = 133
    and authaccountregdatekst >= DATE_SUB(DATE(CONCAT(FORMAT_DATE('%Y-%m', DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY)),'-01')), INTERVAL 24 MONTH)
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

    select distinct regmonth as `가입월`, bep_commission as `수수료 적용후 BEP`
    from (
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
        )
    order by `가입월`
    """

    query_result6_BEP =query_run_method('6_newuser_roas', query)

    context['task_instance'].xcom_push(key='result6_BEP', value=query_result6_BEP)

    return True


### ROAS KPI
def result6_roaskpi(joyplegameid:int, gameidx:str, databaseschema:str, **context):

    query = f"""
    select kpi_d1, kpi_d3, kpi_d7, kpi_d14, kpi_d30, kpi_d60, kpi_d90, kpi_d120, kpi_d150, kpi_d180, kpi_d210, kpi_d240, kpi_d270, kpi_d300, kpi_d330, kpi_d360
    from
    (select * ,row_number() OVER (partition by project ORDER BY updateDate desc ) AS row_
    from `data-science-division-216308.MetaData.roas_kpi`
    where project='GBTW'
    and operationStatus = '운영 중')
    where row_=1

    """

    query_result6_roaskpi = query_run_method('6_newuser_roas', query)

    context['task_instance'].xcom_push(key='result6_roaskpi', value=query_result6_roaskpi)

    return True


def roas_kpi(joyplegameid:int, gameidx:str, databaseschema:str, **context):

    query_result6_roaskpi = context['task_instance'].xcom_pull(
        task_ids = 'result6_roaskpi',
        key='result6_roaskpi'
    )

    query_result6_roaskpi = query_result6_roaskpi * 100
    data = query_result6_roaskpi.rename(columns={
            'kpi_d1' : 'ROAS D1',
            'kpi_d3' : 'ROAS D3',
            'kpi_d7' : 'ROAS D7',
            'kpi_d14' : 'ROAS D14',
            'kpi_d30' : 'ROAS D30',
            'kpi_d60' : 'ROAS D60',
            'kpi_d90' : 'ROAS D90',
            'kpi_d120' : 'ROAS D120',
            'kpi_d150' : 'ROAS D150',
            'kpi_d180' : 'ROAS D180',
            'kpi_d210' : 'ROAS D210',
            'kpi_d240' : 'ROAS D240',
            'kpi_d270' : 'ROAS D270',
            'kpi_d300' : 'ROAS D300',
            'kpi_d330' : 'ROAS D330',
            'kpi_d360' : 'ROAS D360'
            })
    # 데이터프레임 생성
    roas_kpi = pd.DataFrame(data)

    context['task_instance'].xcom_push(key='roas_kpi', value=roas_kpi)

    return True

###
def roas_dataframe_preprocessing(**context):
    query_result6_monthlyROAS = context['task_instance'].xcom_pull(
        task_ids = 'result6_monthlyROAS',
        key='result6_monthlyROAS'
    )
    query_result6_pLTV = context['task_instance'].xcom_pull(
        task_ids = 'result6_pLTV',
        key='result6_pLTV'
    )
    query_result6_return = context['task_instance'].xcom_pull(
        task_ids = 'result6_return',
        key='result6_return'
    )
    query_result6_BEP = context['task_instance'].xcom_pull(
        task_ids = 'result6_BEP',
        key='result6_BEP'
    )

    query6_monthlyROAS = pd.merge(query_result6_monthlyROAS, query_result6_pLTV[['가입월', '매출 D360 예측치']], on = ['가입월'], how = "left")
    query6_monthlyROAS = pd.merge(query6_monthlyROAS
                                , query_result6_return[['가입월', '복귀유저 ROAS D360', '복귀유저 ROAS D360 예측치', '데이터 완성 여부']]
                                , on = ['가입월'], how = 'left')
    query6_monthlyROAS = pd.merge(query6_monthlyROAS, query_result6_BEP, on = ['가입월'], how = 'left')

    target_columns = ["ROAS D14","ROAS D30","ROAS D60","ROAS D90","ROAS D120",
            "ROAS D150","ROAS D180","ROAS D210","ROAS D240","ROAS D270",
            "ROAS D300","ROAS D330","ROAS D360"]

    ## 실측치 컬럼 빈칸에 예측치 컬럼값으로 채우기
    for col in target_columns:
        pred = f"{col} 예측치"
        if col in query6_monthlyROAS.columns and pred in query6_monthlyROAS.columns:
            query6_monthlyROAS[col] = query6_monthlyROAS[col].fillna(query6_monthlyROAS[pred])

    ## 예측치 컬럼 제외(복귀유저 예측치는 그대로 두기)
    cols_to_drop = [
        c for c in query6_monthlyROAS.columns
        if ("예측치" in c) and ("복귀유저" not in c)
    ]
    query6_monthlyROAS = query6_monthlyROAS.drop(columns=cols_to_drop)

    ## 복귀유저 포함 D360 ROAS 계산 -> mature 안된 경우 예측치로 계산
    mature_mask = query6_monthlyROAS['데이터 완성 여부'].eq('mature')

    query6_monthlyROAS['복귀유저 포함 ROAS D360'] = np.where(
        mature_mask,
        query6_monthlyROAS['ROAS D360'] + query6_monthlyROAS['복귀유저 ROAS D360'],
        query6_monthlyROAS['ROAS D360'] + query6_monthlyROAS['복귀유저 ROAS D360 예측치']
    )

    query6_monthlyROAS['기본 BEP'] = 1.429

    ## 2) 컬럼을 'ROAS D360' 다음으로 이동
    cols = query6_monthlyROAS.columns.tolist()
    cols.remove('복귀유저 포함 ROAS D360')
    cols.remove('기본 BEP')
    insert_at = cols.index('ROAS D360') + 1
    cols.insert(insert_at, '복귀유저 포함 ROAS D360')
    insert_at = cols.index('데이터 완성 여부') + 1
    cols.insert(insert_at, '기본 BEP')
    cols.remove('데이터 완성 여부')
    insert_at = cols.index('수수료 적용후 BEP') + 1
    cols.insert(insert_at, '데이터 완성 여부')
    query6_monthlyROAS = query6_monthlyROAS[cols]

    roas_days = [1, 3, 7, 14, 30, 60, 90, 120, 150, 180, 210, 240, 270, 300, 330, 360]

    # 성장세 컬럼 생성
    for i in range(1, len(roas_days)):
        prev_day = roas_days[i - 1]
        curr_day = roas_days[i]

        prev_col = f"ROAS D{prev_day}"
        curr_col = f"ROAS D{curr_day}"
        new_col = f"LTV 성장세 D{curr_day}"

        query6_monthlyROAS[new_col] = query6_monthlyROAS[curr_col] / query6_monthlyROAS[prev_col]

    context['task_instance'].xcom_push(key='monthlyBEP_ROAS', value=query6_monthlyROAS)

    return True


########## ROAS 프롬프트
def result6_ROAS_gemini(**context):

    # KST 타임존 정의 (UTC+9)
    kst = timezone(timedelta(hours=9))

    # 어제 날짜 (KST 기준)
    yesterday_kst = datetime.now(kst) - timedelta(days=1)

    # 어제 날짜의 연도
    year = yesterday_kst.year

    #print("어제 날짜(KST):", yesterday_kst.date())
    #print("어제 연도:", year)

    query6_monthlyROAS = context['task_instance'].xcom_pull(
        task_ids = 'result6_BEP',
        key='monthlyBEP_ROAS'
    )

    roas_kpi = context['task_instance'].xcom_pull(
        task_ids = 'roas_kpi',
        key='roas_kpi'
    )

    response6_monthlyROAS = genai_client.models.generate_content(
        model=MODEL_NAME,
        contents = f"""
    < 월별 마케팅비용과 ROAS>
    {query6_monthlyROAS.to_csv(index=False)}
    다음은 가입월별 ROAS 야. "지표확정 최대기간" 이후의 ROAS는 예측치이니, "지표확정  최대기간" 이후의 ROAS를 언급할 때에는 예측치라고 말해줘.
    어느 가입월이 "복귀유저 포함 ROAS D360"이 KPI를 달성했는지, 또는 달성하지 못했는지를 서두에 Bold 체로 한줄로 언급해줘.
    {year} 연도만 적어줘.

    KPI 는 수수료 적용후 BEP 로 판단하면돼.

    그리고 "복귀유저 포함 ROAS D360" 이 KPI 달성하지 못한 올해 월들은
    아래 ROAS KPI 와 비교해서 어떤 코호트부터 미달하여 달성하지 못했다고 간단히 알려줘.
    달성하지 못한 월들만 언급해줘.
    한 문장마다 노션의 마크다운 리스트 문법을 사용해줘. e.g. * 당월 매출은 이렇습니다.
    ROAS와 KPI 수치는 소수점 첫째자리까지 %로 표시해줘.

    <ROAS KPI>
    {roas_kpi}




    """,
        config=types.GenerateContentConfig(
            system_instruction=SYSTEM_INSTRUCTION
            #,tools=[RAG]
            ,temperature=0.1
            ,labels=LABELS
            # max_output_tokens=2048
        )
    )
    
    return response6_monthlyROAS.text


########## LTV 성장세 부분 프롬프트
def monthlyLTVgrowth_gemini(**context):

    query6_monthlyROAS = context['task_instance'].xcom_pull(
        task_ids = 'result6_BEP',
        key='monthlyBEP_ROAS'
    )

    response6_monthlyLTVgrowth = genai_client.models.generate_content(
    model=MODEL_NAME,
    contents = f"""
    < 월별 마케팅비용과 ROAS>
    {query6_monthlyROAS.to_csv(index=False)}
    다음은 가입월별 ROAS와 LTV 성장세야. "지표확정 최대기간" 이후의 LTV 성장세는 예측치이니, "지표확정  최대기간" 이후의 LTV 성장세를 언급할 때에는 예측치라고 말해줘.
    가입월에 따라서 어느 구간의 성장세가 증가하거나, 하락하는지 트렌드만 언급해줘. e.g. D7 LTV 성장세가 2025년 1월부터 하락했습니다.
    구간은 D3부터 D30까지 초반, D60부터 D180까지 중반, D180부터 D360까지는 후반으로 나눠서 각 구간에 대한 트렌드로 언급해줘.
    가입월별로 하지말고 가입월에 따른 각 구간의 트렌드를 요약해서 언급하되, 특정 가입월에서 크게 상승하거나 크게 하락했다면 그 가입월에 대해서는 수치와 함께 언급해줘.
    LTV 성장세는 소수점 첫째자리까지 %로 표시해줘.
    10줄 이내로 작성해줘.
    한 문장마다 노션의 마크다운 리스트 문법을 사용해줘. e.g. * 당월 매출은 이렇습니다.
    """,
        config=types.GenerateContentConfig(
            system_instruction=SYSTEM_INSTRUCTION,
            # tools=[RAG],
            temperature=0.1
            ,labels=LABELS
            # max_output_tokens=2048
        )
    )
    return response6_monthlyLTVgrowth.text


### ROAS 현황 및 KPI 표 이미지 생성

#### growth 예측치 회색 음영 반영
## 예측 기준 dn을 각 row(regmonth)별로 추론
def infer_cohort_dn_map(df):
    cohort_map = {}
    for idx, row in df.iterrows():
        regmonth = idx[1] if isinstance(idx, tuple) else row.get('가입월', None)
        for col in df.columns:
            if re.search(r"예측치$", col):
                if pd.notna(row[col]):
                    dn = int(re.findall(r'\d+', col)[0])
                    # 예측치가 존재하는 가장 작은 dn 값을 기준으로 설정
                    if regmonth not in cohort_map or dn < cohort_map[regmonth]:
                        cohort_map[regmonth] = dn
    return cohort_map

## 컬럼 이름에서 dn 값 추출
def extract_dn(col):
    match = re.match(r'(ROAS|LTV 성장세) D(\d+)', col)
    return int(match.group(2)) if match else None

## 스타일 함수 정의 - mautred 되지 않은 구간 회색처리
def highlight_based_on_dn(row):
    regmonth = row['가입월']
    cohort_dn = cohort_dn_map.get(regmonth, np.inf)

    styles = []
    for col in row.index:
        clean_col = col.replace("<br>", " ").strip()

        # 두 숫자가 있으면 마지막 숫자를 dn으로
        match = re.findall(r'D(\d+)', clean_col)
        dn_val = int(match[-1]) if match else None

        if (
            (clean_col.startswith('ROAS D') or clean_col.startswith('LTV 성장세'))
            and dn_val is not None
            and dn_val >= cohort_dn
            and pd.notna(row[col])
        ):
            styles.append('background-color: lightgray')
        else:
            styles.append('')
    return styles

#### roas 달성 구간 빨간색 음영
def highlight_roas_vs_bep(row):
    styles = []
    for col in row.index:
        style = ""
        try:
            # 값 변환
            if isinstance(row[col], str) and row[col].endswith('%'):
                roas_val = float(row[col].replace('%', '')) / 100
            elif isinstance(row[col], (int, float)):
                roas_val = row[col]
            else:
                roas_val = None

            # 기준 bep_base 비교
            if col.startswith("ROAS D") and pd.notnull(row.get("기본<br>BEP")):
                bep_val = row["기본<br>BEP"]
                if pd.notnull(roas_val) and roas_val > bep_val:
                    style = "background-color: #fbe4e6"

            # d360 plus 비교 vs bep_commission
            elif col == "복귀유저 포함<br>ROAS D360" and pd.notnull(row.get("수수료 적용후<br>BEP")):
                bep_comm = row["수수료 적용후<br>BEP"]
                if pd.notnull(roas_val) and roas_val > bep_comm:
                    style = "background-color: #fbe4e6"

        except Exception as e:
            print(f"[DEBUG] {col} 처리 중 오류: {e}")
            style = ""

        styles.append(style)
    return styles

def roas_table_draw(**context):

    query6_monthlyROAS = context['task_instance'].xcom_pull(
        task_ids = 'result6_BEP',
        key='monthlyBEP_ROAS'
    )

    query_result6_monthlyROAS = context['task_instance'].xcom_pull(
        task_ids = 'result6_monthlyROAS',
        key='result6_monthlyROAS'
    )

    df_numeric = query6_monthlyROAS.drop(columns=['데이터 완성 여부']).copy()
    df_numeric = df_numeric.reset_index(drop=True)

    nest_asyncio.apply()

    cohort_dn_map = infer_cohort_dn_map(query_result6_monthlyROAS)

    # dn_values는 <br> 없는 clean 컬럼명 기준으로 생성
    dn_values = {col: extract_dn(col) for col in query6_monthlyROAS.columns if col.startswith("ROAS D") or col.startswith("LTV 성장세 D")}

    # 개행할 컬럼 지정 및 개행 입력한 컬럼명으로 변경
    custom_colnames = {
        "지표확정 최대기간": "지표확정<br>최대기간",
        "복귀유저 포함 ROAS D360": "복귀유저 포함<br>ROAS D360",
        "복귀유저 ROAS D360": "복귀유저<br>ROAS D360",
        "복귀유저 ROAS D360 예측치": "복귀유저<br>ROAS D360<br>예측치",
        "기본 BEP": "기본<br>BEP",
        "수수료 적용후 BEP": "수수료 적용후<br>BEP",
        "LTV 성장세 D3": "LTV<br>성장세<br>D1 D3",
        "LTV 성장세 D7": "LTV<br>성장세<br>D3 D7",
        "LTV 성장세 D14": "LTV<br>성장세<br>D7 D14",
        "LTV 성장세 D30": "LTV<br>성장세<br>D14 D30",
        "LTV 성장세 D60": "LTV<br>성장세<br>D30 D60",
        "LTV 성장세 D90": "LTV<br>성장세<br>D60 D90",
        "LTV 성장세 D120": "LTV<br>성장세<br>D90 D120",
        "LTV 성장세 D150": "LTV<br>성장세<br>D120 D150",
        "LTV 성장세 D180": "LTV<br>성장세<br>D150 D180",
        "LTV 성장세 D210": "LTV<br>성장세<br>D180 D210",
        "LTV 성장세 D240": "LTV<br>성장세<br>D210 D240",
        "LTV 성장세 D270": "LTV<br>성장세<br>D240 D270",
        "LTV 성장세 D300": "LTV<br>성장세<br>D270 D300",
        "LTV 성장세 D330": "LTV<br>성장세<br>D300 D330",
        "LTV 성장세 D360": "LTV<br>성장세<br>D330 D360"
    }
    df_numeric = df_numeric.rename(columns=custom_colnames)

    #### ROAS 수치의 바 서식을 컬럼별이 아닌 전체 수치 기준으로 서식적용을 위한 파라미터값 설정
    roas_cols = [c for c in df_numeric.columns if c.startswith("ROAS D")] + ["복귀유저 포함<br>ROAS D360"]

    # 전체 최소/최대 구하기
    roas_global_min = df_numeric[roas_cols].min().min()
    roas_global_max = df_numeric[roas_cols].max().max()

    #### 성장세 수치의 바 서식을 컬럼별이 아닌 전체 수치 기준으로 서식적용을 위한 파라미터값 설정

    growth_cols = [c for c in df_numeric.columns if c.startswith("LTV<br>성장세<br>D")]

    # 전체 최소/최대 구하기
    growth_global_min = df_numeric[growth_cols].min().min()
    growth_global_max = df_numeric[growth_cols].max().max()

    #### style 적용
    styled = (
        df_numeric.style
        .hide(axis="index")
        .format({
            "마케팅 비용": "{:,.0f}",
            **{
                col: "{:.1%}"
                for col in df_numeric.columns
                if col.startswith("ROAS D")
                or col.startswith("LTV<br>성장세<br>D")
                or col.startswith("복귀유저")
                or col.endswith("BEP")
            }
        })
        .bar(subset=["마케팅 비용"], color="#f4cccc")
        .bar(subset=roas_cols, color="#c9daf8", vmin=roas_global_min, vmax=roas_global_max)
        .bar(subset=["복귀유저<br>ROAS D360", "복귀유저<br>ROAS D360<br>예측치"], color="#ffe599")
        .bar(subset=growth_cols, color="#b5f7a3", vmin=growth_global_min, vmax=growth_global_max)
        .set_table_styles(
            [
                {"selector": "th", "props": [("background-color", "#f0f0f0"), ("font-weight", "bold"), ("border", "1px solid black")]},
                {"selector": "td", "props": [("border", "1px solid black")]}
            ]
        )
        # 강조 함수 적용
        .apply(highlight_based_on_dn, axis=1)
        .apply(highlight_roas_vs_bep, axis=1)
        )
    
    return styled



def roas_html_draw(gameidx: str, bucket_name: str, **context):
    """
    HTML 테이블을 이미지로 캡처하여 GCS에 저장
    
    Args:
        gameidx: 게임 인덱스
        bucket_name: GCS 버킷명
        **context: Airflow 컨텍스트
    
    Returns:
        GCS 경로 (예: "potc/graph6_monthlyROAS.png")
    """
    
    logger.info("🎯 ROAS HTML 이미지 캡처 시작")
    
    try:
        # Step 1: 테이블 데이터 생성
        logger.info("📊 테이블 데이터 생성 중...")
        styled = roas_table_draw(**context)
        
        # Step 2: HTML 생성
        logger.info("🔨 HTML 생성 중...")
        html_path = create_html_file(styled)
        
        # Step 3: HTML을 이미지로 캡처
        logger.info("📸 이미지 캡처 중...")
        image_bytes = asyncio.run(capture_html_to_image_async(html_path))
        
        # Step 4: GCS에 업로드
        logger.info("📤 GCS 업로드 중...")
        gcs_path = upload_image_to_gcs(
            image_bytes=image_bytes,
            gameidx=gameidx,
            bucket_name=bucket_name,
            filename="graph6_monthlyROAS.png"
        )
        
        logger.info(f"✅ ROAS 이미지 저장 완료: {gcs_path}")
        
        # Step 5: 로컬 HTML 파일 정리
        cleanup_local_files(html_path)
        
        return gcs_path
        
    except Exception as e:
        logger.error(f"❌ ROAS 이미지 캡처 실패: {type(e).__name__} - {str(e)}", exc_info=True)
        raise


def create_html_file(styled_df) -> str:
    """
    스타일이 적용된 DataFrame을 HTML 파일로 생성
    
    Args:
        styled_df: 스타일이 적용된 Pandas DataFrame
    
    Returns:
        HTML 파일 경로
    """
    
    html_template = """
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <style>
            body { 
                font-family: Arial, sans-serif; 
                padding: 20px; 
                margin: 0;
            }
            table { 
                border-collapse: collapse; 
                font-size: 13px; 
                margin-top: 20px;
            }
            th, td {
                border: 1px solid #999;
                padding: 6px 10px;
                text-align: center;
            }
            th { background-color: #f0f0f0; font-weight: bold; }
            
            /* 특정 컬럼별 스타일 */
            th:nth-child(1), td:nth-child(1) { min-width: 50px; max-width: 60px; }
            th:nth-child(2), td:nth-child(2) { min-width: 60px; max-width: 65px; white-space: normal; }
            th:nth-child(3), td:nth-child(3) { min-width: 70px; max-width: 95px; }
            th:nth-child(20), td:nth-child(20) { min-width: 80px; white-space: normal; }
            th:nth-child(21), td:nth-child(21) { min-width: 75px; white-space: normal; }
            th:nth-child(22), td:nth-child(22) { min-width: 85px; white-space: normal; }
            th:nth-child(24), td:nth-child(24) { min-width: 80px; white-space: normal; }
            th:nth-child(28), td:nth-child(28) { min-width: 55px; white-space: normal; }
            th:nth-child(29), td:nth-child(29) { min-width: 55px; white-space: normal; }
            th:nth-child(30), td:nth-child(30) { min-width: 55px; white-space: normal; }
            th:nth-child(31), td:nth-child(31) { min-width: 60px; white-space: normal; }
            th:nth-child(32), td:nth-child(32) { min-width: 70px; white-space: normal; }
            th:nth-child(33), td:nth-child(33) { min-width: 70px; white-space: normal; }
            th:nth-child(34), td:nth-child(34) { min-width: 70px; white-space: normal; }
            th:nth-child(35), td:nth-child(35) { min-width: 70px; white-space: normal; }
            th:nth-child(36), td:nth-child(36) { min-width: 70px; white-space: normal; }
            th:nth-child(37), td:nth-child(37) { min-width: 70px; white-space: normal; }
            th:nth-child(38), td:nth-child(38) { min-width: 70px; white-space: normal; }
            th:nth-child(39), td:nth-child(39) { min-width: 70px; white-space: normal; }
            
            h2 { margin-top: 0; }
        </style>
    </head>
    <body>
        <h2>{{ game_name }} GBTW 신규유저 회수 현황</h2>
        {{ table | safe }}
    </body>
    </html>
    """
    
    try:
        # 테이블을 HTML로 변환
        table_html = styled_df.to_html()
        
        # 템플릿에 렌더링
        rendered_html = Template(html_template).render(
            game_name="GBTW",
            table=table_html
        )
        
        # HTML 파일 저장 (절대 경로 사용)
        html_path = os.path.join("/tmp", "table6_monthlyROAS.html")
        
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(rendered_html)
        
        logger.info(f"✅ HTML 파일 생성: {html_path}")
        return html_path
        
    except Exception as e:
        logger.error(f"❌ HTML 파일 생성 실패: {type(e).__name__} - {str(e)}", exc_info=True)
        raise


async def capture_html_to_image_async(html_path: str) -> bytes:
    """
    HTML 파일을 이미지로 캡처 (비동기)
    
    Args:
        html_path: HTML 파일 경로
    
    Returns:
        이미지 바이트 데이터
    """
    
    logger.info(f"🎬 Playwright 시작: {html_path}")
    
    try:
        async with async_playwright() as p:
            # ✅ 브라우저 실행
            logger.info("🌐 브라우저 실행 중...")
            browser = await p.chromium.launch(headless=True)
            
            # ✅ 페이지 생성
            page = await browser.new_page(
                viewport={"width": 1800, "height": 800}
            )
            
            # ✅ HTML 파일 로드
            file_url = f"file://{os.path.abspath(html_path)}"
            logger.info(f"📄 HTML 로드: {file_url}")
            await page.goto(file_url)
            
            # ✅ 페이지 렌더링 대기
            await page.wait_for_load_state("networkidle")
            logger.info("✅ 페이지 로딩 완료")
            
            # ✅ 이미지 캡처 (메모리에 직접)
            logger.info("📸 스크린샷 캡처 중...")
            screenshot_bytes = await page.screenshot(full_page=True)
            
            logger.info(f"✅ 스크린샷 완료 ({len(screenshot_bytes) / 1024:.1f} KB)")
            
            # ✅ 브라우저 종료
            await browser.close()
            logger.info("🔌 브라우저 종료")
            
            return screenshot_bytes
            
    except Exception as e:
        logger.error(f"❌ HTML 캡처 실패: {type(e).__name__} - {str(e)}", exc_info=True)
        raise


def upload_image_to_gcs(
    image_bytes: bytes,
    gameidx: str,
    bucket_name: str,
    filename: str = "graph6_monthlyROAS.png"
    ) -> str:
    """
    이미지 바이트를 GCS에 업로드
    
    Args:
        image_bytes: 이미지 바이트 데이터
        gameidx: 게임 인덱스
        bucket_name: GCS 버킷명
        filename: 저장할 파일명
    
    Returns:
        GCS 경로 (예: "potc/graph6_monthlyROAS.png")
    """
    
    try:
        # GCS 클라이언트 초기화
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        
        # GCS 경로 설정
        gcs_path = f"{gameidx}/{filename}"
        blob = bucket.blob(gcs_path)
        
        logger.info(f"📤 GCS 업로드: gs://{bucket_name}/{gcs_path}")
        
        # 이미지 업로드
        blob.upload_from_string(
            image_bytes,
            content_type='image/png'
        )
        
        logger.info(f"✅ GCS 업로드 완료: {len(image_bytes) / 1024:.1f} KB")
        
        return gcs_path
        
    except Exception as e:
        logger.error(f"❌ GCS 업로드 실패: {type(e).__name__} - {str(e)}", exc_info=True)
        raise


def cleanup_local_files(html_path: str) -> None:
    """
    로컬 임시 파일 정리
    
    Args:
        html_path: 삭제할 HTML 파일 경로
    """
    
    try:
        if os.path.exists(html_path):
            os.remove(html_path)
            logger.info(f"🗑️ 로컬 파일 삭제: {html_path}")
    except OSError as e:
        logger.warning(f"⚠️ 파일 삭제 실패 (무시): {type(e).__name__} - {str(e)}")


def kpi_table_draw(**context):

    roas_kpi = context['task_instance'].xcom_pull(
        task_ids = 'roas_kpi',
        key='roas_kpi'
    )

    # kpi표
    nest_asyncio.apply()

    df_numeric = roas_kpi.copy()
    df_numeric = df_numeric.reset_index(drop=True)

    # 1) ROAS % → 비율 변환
    def to_ratio_series(s: pd.Series) -> pd.Series:
        s_str = s.astype(str)
        s_num = pd.to_numeric(s_str.str.replace('%', '', regex=False), errors='coerce')
        return s_num / 100.0

    for c in df_numeric.columns:
        if c.startswith("ROAS "):
            df_numeric[c] = to_ratio_series(df_numeric[c])

    # 2) suffixes 추출
    suffixes = []
    for c in df_numeric.columns:
        m = re.search(r'\b(D\d+)\b$', str(c))
        if m and m.group(1) not in suffixes:
            suffixes.append(m.group(1))
    suffixes_tuple = tuple(suffixes)

    # 3) Styler 기본 포맷
    styled = (
        df_numeric.style
        .hide(axis="index")
        .format({col: "{:.1%}" for col in df_numeric.columns if col.startswith("ROAS ")})
        .set_table_attributes('style="table-layout:fixed; width:600px;"')
    )

    # 4) HTML 템플릿
    html_template = """
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <style>
            body { font-family: Arial, sans-serif; padding: 20px; }
            h2 { margin: 0 0 10px 0; font-size: 18px; }
            table { border-collapse: collapse; font-size: 12px; border: 1px solid black; }
            th, td {
                border: 1px solid black;
                padding: 6px 8px;
                text-align: center;
                white-space: nowrap;
            }
            th { background-color: #f0f0f0; font-weight: bold; }
        </style>
    </head>
    <body>
        <h2>GBTW ROAS KPI (신규유저 기준)</h2>
        {{ table | safe }}
    </body>
    </html>
    """

    # 5) Styler → HTML
    soup = BeautifulSoup(styled.to_html(), "html.parser")
    table = soup.find("table")

    # 6) colgroup & width 적용
    ncols = len(df_numeric.columns)
    for cg in table.find_all("colgroup"):
        cg.decompose()

    colgroup = soup.new_tag("colgroup")
    width_map = {col: (80 if col.startswith("ROAS ") else 110) for col in df_numeric.columns}
    for col_name in df_numeric.columns:
        col = soup.new_tag("col", style=f"width: {width_map[col_name]}px !important;")
        colgroup.append(col)
    table.insert(0, colgroup)

    # 7) 헤더 줄바꿈 (ROAS → ROAS<br>…)
    for th in table.find_all("th"):
        text = th.get_text(strip=True)
        if text.startswith("ROAS "):
            th.string = ""
            th.append(BeautifulSoup(text.replace("ROAS ", "ROAS<br>"), "html.parser"))

    # 8) 최종 HTML 저장
    rendered_html = Template(html_template).render(table=str(table))
    html_path = "table6_ROAS_KPI.html"
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(rendered_html)

    # 9) 스크린샷 캡처
    async def capture_html_to_image():
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page(viewport={"width": 600, "height": 160})
            await page.goto("file://" + os.path.abspath(html_path))
            await page.screenshot(path="graph6_ROAS_KPI.png", full_page=True)
            await browser.close()

    asyncio.get_event_loop().run_until_complete(capture_html_to_image())


def roas_kpi_table_merge(gameidx:str):

    p1 = f'{gameidx}/graph6_monthlyROAS.png'
    p2 = f'{gameidx}/graph6_ROAS_KPI.png'

    # 2) 이미지 열기 (투명 보존 위해 RGBA)
    blob1 = bucket.blob(p1)
    blob2 = bucket.blob(p2)

    im1 = blob1.download_as_bytes()
    im2 = blob2.download_as_bytes()

    img1 = Image.open(BytesIO(im1)).convert("RGBA")
    img2 = Image.open(BytesIO(im2)).convert("RGBA") 

    # 두 이미지의 크기 가져오기
    w1, h1 = img1.size
    w2, h2 = img2.size

    # 최종 캔버스 크기 (너비는 두 이미지 중 큰 값, 높이는 합계)
    final_width = max(w1, w2)
    final_height = h1 + h2

    # 흰색 배경의 새 캔버스 생성
    combined = Image.new("RGB", (final_width, final_height), (255, 255, 255))

    # 위에 roas_pc, 아래에 roaskpi_pc 붙이기 (왼쪽 정렬)
    combined.paste(img1, (0, 0))
    combined.paste(img2, (0, h1))

    # 저장
    combined.save("graph6_monthlyROAS_and_KPI.png", dpi=(180,180))

    # 3) GCS에 저장
    output_buffer = BytesIO()
    combined.save(output_buffer, format='PNG')
    output_buffer.seek(0)

    # GCS 경로
    gcs_path = f'{gameidx}/graph1_dailySales_monthlySales.png'
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(output_buffer.getvalue(), content_type='image/png')

    return gcs_path


def retrieve_new_user_upload_notion(gameidx:str, **context):

    PAGE_INFO=context['task_instance'].xcom_pull(
        task_ids = 'make_gameframework_notion_page',
        key='page_info'
    )

    notion.blocks.children.append(
        PAGE_INFO['id'],
        children=[
            {
            "object": "block",
            "type": "heading_2",
            "heading_2": {
                "rich_text": [{"type": "text", "text": {"content": "6. 신규유저 회수 현황" }}]
                },
            }
        ],
    )

    # 공통 헤더
    headers_json = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json"
    }

    try:
        gcs_path = f'{gameidx}/filePath6_monthlyROAS_KPI.png'
        blob = bucket.blob(gcs_path)
        image_bytes = blob.download_as_bytes()
        filename = 'filePath6_monthlyROAS_KPI.png'
        print(f"✓ GCS 이미지 다운로드 성공 : {gcs_path}")
    except Exception as e:
        print(f"❌ GCS 다운로드 실패: {str(e)}")
        raise

    try:   
        # 1) 업로드 객체 생성 (file_upload 생성)
        create_url = "https://api.notion.com/v1/file_uploads"
        payload = {
            "filename": filename,
            "content_type": "image/png"
        }

        resp = requests.post(create_url, headers=headers_json, data=json.dumps(payload))
        resp.raise_for_status()
        file_upload = resp.json()
        file_upload_id = file_upload["id"]
        print(f"✓ Notion 업로드 객체 생성: {file_upload_id}")

        # 2) 파일 바이너리 전송 (multipart/form-data)
        # ✅ 로컬 파일 대신 BytesIO 사용
        send_url = f"https://api.notion.com/v1/file_uploads/{file_upload_id}/send"
        files = {"file": (filename, BytesIO(image_bytes), "image/png")}
        headers_send = {
            "Authorization": f"Bearer {NOTION_TOKEN}",
            "Notion-Version": NOTION_VERSION
        }
        send_resp = requests.post(send_url, headers=headers_send, files=files)
        send_resp.raise_for_status()
        print(f"✓ 파일 전송 완료: {filename}")

        # 3) Notion 페이지에 이미지 블록 추가
        append_url = f"https://api.notion.com/v1/blocks/{PAGE_INFO['id']}/children"
        append_payload = {
            "children": [
                {
                    "object": "block",
                    "type": "image",
                    "image": {
                        "type": "file_upload",
                        "file_upload": {"id": file_upload_id},
                        # 캡션을 달고 싶다면 아래 주석 해제
                        # "caption": [{"type": "text", "text": {"content": "자동 업로드된 그래프"}}]
                    }
                }
            ]
        }

        append_resp = requests.patch(
            append_url, headers=headers_json, data=json.dumps(append_payload)
        )
        append_resp.raise_for_status()
        print(f"✅ Notion에 이미지 추가 완료: {filename}")

    except requests.exceptions.RequestException as e:
        print(f"❌ Notion API 에러: {str(e)}")
        raise
    except Exception as e:
        print(f"❌ 예기치 않은 에러: {str(e)}")
        raise


    query6_monthlyROAS =context['task_instance'].xcom_pull(
        task_ids='roas_dataframe_preprocessing',
        key='monthlyBEP_ROAS'
    )

    resp = df_to_notion_table_under_toggle(
        notion=notion,
        page_id=PAGE_INFO['id'],
        df=query6_monthlyROAS,
        toggle_title="📊 로데이터 - 신규유저 회수현황",
        max_first_batch_rows=90,
        batch_size=100,
    )

    blocks = md_to_notion_blocks(result6_ROAS_gemini(**context), 1)

    notion.blocks.children.append(
        block_id=PAGE_INFO['id'],
        children=blocks
    )

    blocks = md_to_notion_blocks(monthlyLTVgrowth_gemini(**context))

    notion.blocks.children.append(
        block_id=PAGE_INFO['id'],
        children=blocks
    )


##### 프롬프트 종합하여 요약

def summary_gemini(joyplegameid:int, gameidx:str, service_sub:str, **context):
    response_summary = genai_client.models.generate_content(
    model=MODEL_NAME,
    contents = f"""

    아래 내용을 10줄 이내로 요약해줘.
    요약한 내용은 ~~ 입니다 이런말 하지말고 그냥 바로 요약한 내용만 알려줘.
    {daily_revenue_gemini(joyplegameid, service_sub, **context)}
    {inhouses_revenue_gemini(joyplegameid, **context)}
    {cohort_by_gemini(joyplegameid, **context)}
    {os_by_gemini(joyplegameid, **context)}
    {rev_group_rev_pu_gemini(joyplegameid, service_sub, **context)}
    {iap_gem_ruby_gemini(service_sub, **context)}
    {top3_items_by_category_gemini(service_sub, **context)}
    {monthly_day_average_rev_gemini(joyplegameid, service_sub, **context)}
    {rgroup_rev_total_gemini(joyplegameid, service_sub, **context)}
    {rev_cohort_year_gemini(joyplegameid, service_sub, **context)}
    {result6_ROAS_gemini(**context)}

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

    # 코멘트 출력
    return response_summary.text


def summray_upload_notion(joyplegameid:int, gameidx:str, service_sub:str, **context):

    PAGE_INFO=context['task_instance'].xcom_pull(
        task_ids = 'make_gameframework_notion_page',
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
    blocks = md_to_notion_blocks(summary_gemini(joyplegameid, gameidx, service_sub, **context))

    notion.blocks.children.append(
        block_id=PAGE_INFO['id'],
        children=blocks
    )

    return True
