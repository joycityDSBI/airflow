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
from airflow.operators.python import get_current_context
from zoneinfo import ZoneInfo  # Python 3.9 이상
from pathlib import Path
from game_framework_util import *


## 일자별 매출
def Daily_revenue_query(joyplegameid: int, bigquery_client, **context):
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
    query_result = query_run_method('1_daily_sales', bigquery_client, query)
    # ✅ get_current_context()로 context 가져오기
    current_context = get_current_context()
    current_context['task_instance'].xcom_push(key='daily_revenue_df', value=query_result)

    return True
    
    
#### 전년 대비 월 매출 추이
def Daily_revenue_YOY_query(joyplegameid: int, bigquery_client, **context):
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
    query_result = query_run_method('1_daily_sales', bigquery_client, query)

    current_context = get_current_context()
    current_context['task_instance'].xcom_push(key='Daily_revenue_YOY_df', value=query_result)

    return True


## 현재 매출과 목표 매출
def Daily_revenue_target_revenue_query(joyplegameid: int, gameidx: str, bigquery_client, **context):
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
    where idx = '{gameidx}'
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

    query_result = query_run_method('1_daily_sales', bigquery_client, query)
    
    current_context = get_current_context()
    current_context['task_instance'].xcom_push(key='Daily_revenue_target_revenue_df', value=query_result)

    return True


## 전년 대비 월 매출 추이 수정 - 당월은 일할계산 매출
def merge_daily_revenue(**context):

    current_context = get_current_context()

    s_total = current_context['task_instance'].xcom_pull(
        task_ids = 'Daily_revenue_query',
        key='daily_revenue_df'
    )
    val_total = current_context['task_instance'].xcom_pull(
        task_ids = 'Daily_revenue_YOY_query',
        key='Daily_revenue_YOY_df'
    )

    # ✅ 데이터 검증
    print(f"📊 s_total type: {type(s_total)}, val: {s_total}")
    print(f"📊 val_total type: {type(val_total)}, val: {val_total}")
    
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
def daily_revenue_gemini(service_sub: str, genai_client, MODEL_NAME, SYSTEM_INSTRUCTION:list, **context):

    current_context = get_current_context()

    query_result1_dailySales = current_context['task_instance'].xcom_pull(
        task_ids = 'Daily_revenue_query',
        key='daily_revenue_df'
    )

    query_result1_monthlySales = current_context['task_instance'].xcom_pull(
        task_ids = 'Daily_revenue_YOY_query',
        key='Daily_revenue_YOY_df'
    )

    RUN_ID = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    LABELS = {"datascience_division_service": 'gameinsight_framework',
            "run_id": RUN_ID,
            "datascience_division_service_sub" : service_sub}

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
def daily_revenue_graph_draw(gameidx: str, bucket, **context):

    current_context = get_current_context()

    df_daily = current_context['task_instance'].xcom_pull(
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
def daily_revenue_YOY_graph_draw(gameidx: str, bucket, **context):

    current_context = get_current_context()

    query_result1_monthlySales = current_context['task_instance'].xcom_pull(
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
def merge_daily_graph(joyplegameid: int, gameidx: str, bucket):
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


def daily_revenue_data_upload_to_notion(gameidx: str, service_sub, genai_client, MOEDEL_NAME, SYSTEM_INSTRUCTION, notion, bucket, headers_json, **context):

    current_context = get_current_context()

    PAGE_INFO=current_context['task_instance'].xcom_pull(
        task_ids = 'make_gameframework_notion_page',
        key='page_info'
    )
    query_result1_dailySales=current_context['task_instance'].xcom_pull(
        task_ids='daily_revenue_query',  # ← 첫 번째 Task의 task_id
        key='daily_revenue_df'
    )

    query_result1_monthlySales=current_context['task_instance'].xcom_pull(
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
    headers_json = headers_json
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
    headers_send = headers_json

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

    headers_json_patch = headers_json
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

    response1_salesComment = daily_revenue_gemini(service_sub, genai_client, MOEDEL_NAME, SYSTEM_INSTRUCTION)

    ## 제미나이
    blocks = md_to_notion_blocks(response1_salesComment)
    notion.blocks.children.append(
        block_id=PAGE_INFO["id"],
        children=blocks
    )

    return True



