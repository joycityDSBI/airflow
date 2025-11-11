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
def daily_revenue_graph_draw(joyplegameid: int, gameidx: str, **context):

    df_daily = context['task_instance'].xcom_pull(
        task_ids='daily_revenue_query',  # ← 첫 번째 Task의 task_id
        key='daily_revenue_df'
    )
    
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

    