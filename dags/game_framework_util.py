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


## 페이지 생성 함수 //////////// task 함수
def make_gameframework_notion_page(
        gameidx: str, 
        NOTION_TOKEN, 
        DATABASE_ID,
        notion,
        **context):

    url = "https://api.notion.com/v1/pages"
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }

    page_info = None # 초기화

    # 타임존 지정
    try: 
        kst = ZoneInfo("Asia/Seoul")
        today_kst = datetime.now(kst).date()
        yesterday_kst = today_kst - timedelta(days=1)

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
                        {"name": gameidx}   # 다중 선택 옵션
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
            print(f"✅ 페이지 생성 성공 ✅ 페이지 ID : {page_info['id']}")
        else:
            print(f"⚠️ Notion API 에러 발생: {res.status_code} >> {res.text}")

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
    except Exception as e:
        print(f"⚠️ 페이지 생성 실패: {e}")

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


def query_run_method(service_sub: str, bigquery_client, query):
    RUN_ID = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    LABELS = {"datascience_division_service": 'gameinsight_framework',
            "run_id": RUN_ID,
            f"datascience_division_service_sub" : service_sub} ## 딕셔너리 형태로 붙일 수 있음.
    print("📧 RUN_ID=", RUN_ID, "📧 LABEL_ID=", LABELS)

    query_result = bigquery_client.query(query, job_config=bigquery.QueryJobConfig(labels=LABELS)).to_dataframe()
    return query_result

################################ 메인 함수 처리 ################################

