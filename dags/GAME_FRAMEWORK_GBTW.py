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
    model='gemini-2.5-flash',
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
    model='gemini-2.5-flash',

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
    model='gemini-2.5-flash',

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

    query_result3_revByOs=context['task_instance'].xcom_pull(
        task_ids='cohort_by_country_revenue',  # ← 첫 번째 Task의 task_id
        key='cohort_by_country_revenue_df'
    )
    query_result3_costByOs=context['task_instance'].xcom_pull(
        task_ids='cohort_by_country_cost',  # ← 첫 번째 Task의 task_id
        key='cohort_by_country_cost_df'
    )


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
    blocks = md_to_notion_blocks(response3_revAndCostByOs.text)
    notion.blocks.children.append(
        block_id=PAGE_INFO['id'],
        children=blocks
    )







