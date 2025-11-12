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
def Daily_revenue_query(joyplegameid: int, gameidx: str, bigquery_client, bucket, **context):
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

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    gcs_path = f"{gameidx}/{timestamp}.parquet"
        
    saved_path = save_df_to_gcs(query_result, bucket, gcs_path)

    return saved_path
    
    
#### 전년 대비 월 매출 추이
def Daily_revenue_YOY_query(joyplegameid: int, gameidx:str, bigquery_client, bucket, **context):
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

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    gcs_path = f"{gameidx}/{timestamp}.parquet"
        
    saved_path = save_df_to_gcs(query_result, bucket, gcs_path)

    return saved_path


## 현재 매출과 목표 매출
def Daily_revenue_target_revenue_query(joyplegameid: int, gameidx: str, bigquery_client, bucket, **context):
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

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    gcs_path = f"{gameidx}/{timestamp}.parquet"
        
    saved_path = save_df_to_gcs(query_result, bucket, gcs_path)

    return saved_path


## 전년 대비 월 매출 추이 수정 - 당월은 일할계산 매출
def merge_daily_revenue(path_daily_revenue:str, path_daily_revenue_yoy:str, bucket, **context):

    try:
        s_total = load_df_from_gcs(bucket, path_daily_revenue)
        val_total = load_df_from_gcs(bucket, path_daily_revenue_yoy)

        print("s total, val total head 값 3개")
        print(s_total.head(3))
        print(val_total.head(3))
        # ✅ 값 추출
        val = val_total.iat[0, 0]
        s = s_total.iloc[:, 2]
        
        # ✅ 마지막 non-null 인덱스 찾기
        non_null_mask = s.notna()
        if non_null_mask.any():
            idx = s[non_null_mask].index[-1]
            
            # ✅ 타입 변환 후 할당
            try:
                # 현재 컬럼의 dtype에 맞춰서 변환
                val_converted = s_total[s_total.columns[2]].dtype.type(val)
                s_total.loc[idx, s_total.columns[2]] = val_converted
                print(f"✅ 병합 완료: idx={idx}, val={val}")
            except:
                # 실패시 그냥 할당
                s_total.loc[idx, s_total.columns[2]] = val
                print(f"✅ 병합 완료 (타입 변환 스킵)")
        else:
            print(f"⚠️ 모두 null - 병합 스킵")
        
        return s_total

    except Exception as e:
        print(f"❌ merge_daily_revenue 실패: {e}")
        raise


## 프롬프트 
### 4> 일자별 매출에 대한 제미나이 코멘트
def daily_revenue_gemini(service_sub: str, genai_client, MODEL_NAME, SYSTEM_INSTRUCTION:list, path_daily_revenue, path_monthly_revenue, bucket, **context):

    query_result1_dailySales = load_df_from_gcs(bucket, path_daily_revenue)
    query_result1_monthlySales = load_df_from_gcs(bucket, path_monthly_revenue)

    RUN_ID = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    LABELS = {"datascience_division_service": 'gameinsight_framework',
            "run_id": RUN_ID,
            "datascience_division_service_sub" : service_sub}

    response1_salesComment = genai_client.generate_content(
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


## 그래프 그리기 : arg 값으로 게임 코드
def daily_revenue_graph_draw(gameidx: str, path_daily_revenue:str, bucket, **context):

    df_daily = load_df_from_gcs(bucket, path_daily_revenue)
    
    x  = df_daily.iloc[:, 0]
    y1 = pd.to_numeric(df_daily.iloc[:, 1], errors='coerce')
    y2 = pd.to_numeric(df_daily.iloc[:, 2], errors='coerce')

    # ✅ NaN 제거
    mask = x.notna() & y1.notna() & y2.notna()
    x = x[mask]
    y1 = y1[mask]
    y2 = y2[mask]
    
    if len(x) == 0:
        print(f"⚠️ 유효한 데이터가 없음")
        return None
    
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
def daily_revenue_YOY_graph_draw(gameidx: str, path_daily_revenue_yoy: str, bucket, **context):

    query_result1_monthlySales = load_df_from_gcs(bucket, path_daily_revenue_yoy)

    x  = query_result1_monthlySales.iloc[:, 0]
    y1 = pd.to_numeric(query_result1_monthlySales.iloc[:, 1], errors='coerce')
    y2 = pd.to_numeric(query_result1_monthlySales.iloc[:, 2], errors='coerce')


    # ✅ NaN 제거
    mask = x.notna() & y1.notna() & y2.notna()
    x = x[mask]
    y1 = y1[mask]
    y2 = y2[mask]
    
    if len(x) == 0:
        print(f"⚠️ 유효한 데이터가 없음")
        return None
    

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
def merge_daily_graph(gameidx: str, daily_revenue_path, daily_revenue_yoy_path, bucket, **context):
    p1 = daily_revenue_graph_draw(gameidx, daily_revenue_path, bucket)
    print(f"✅ p1 경로: {p1}")

    p2 = daily_revenue_YOY_graph_draw(gameidx, daily_revenue_yoy_path, bucket)
    print(f"✅ p2 경로: {p2}")

    # 2) 이미지 열기 (투명 보존 위해 RGBA)
    print(f"📥 GCS에서 이미지 다운로드 중...")
    blob1 = bucket.blob(p1)
    blob2 = bucket.blob(p2)

    print(f"📥 blob1 다운로드 중 ...")
    im1 = blob1.download_as_bytes()
    im2 = blob2.download_as_bytes()

    print(f"🖼️ Image 객체 생성 중...")
    im1 = Image.open(BytesIO(im1))
    im2 = Image.open(BytesIO(im2))

    # ---- [옵션 A] 원본 크기 유지 + 세로 패딩으로 높이 맞추기 (권장: 왜곡 없음) ----
    print(f"🔄 이미지 높이 맞추는 중...")
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
    print(f"✅ im1_p 크기: {im1_p.size}")
    print(f"✅ im2_p 크기: {im2_p.size}")

    print(f"🔗 이미지 합치는 중...")
    gap = 0  # 이미지 사이 여백(px). 필요하면 20 등으로 변경
    bg = (255, 255, 255, 0)  # 전체 배경(투명). 흰색 원하면 (255,255,255,255)

    print(f"💾 PNG로 인코딩 중...")
    out = Image.new("RGBA", (im1_p.width + gap + im2_p.width, target_h), bg)
    out.paste(im1_p, (0, 0), im1_p)
    out.paste(im2_p, (im1_p.width + gap, 0), im2_p)

    # 3) GCS에 저장
    print(f"📤 GCS에 업로드 중...")
    output_buffer = BytesIO()
    out.save(output_buffer, format='PNG')
    output_buffer.seek(0)

    # GCS 경로
    gcs_path = f'{gameidx}/graph1_dailySales_monthlySales.png'
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(output_buffer.getvalue(), content_type='image/png')
    print(f"✅ GCS 업로드 완료: gs://{bucket.name}/{gcs_path}")
    return gcs_path


def daily_revenue_data_upload_to_notion(gameidx: str, st1, st2, service_sub, genai_client, MODEL_NAME, SYSTEM_INSTRUCTION, notion, bucket, headers_json, NOTION_TOKEN, NOTION_VERSION,  **context):

    current_context = get_current_context()
    
    PAGE_INFO=current_context['task_instance'].xcom_pull(
        task_ids = 'make_gameframework_notion_page_wraper',
        key='page_info'
    )

    print(f"📊 page_info type: {type(PAGE_INFO)}")
    print(f"📊 page_info: {PAGE_INFO}")
    print(f"✅ PAGE_INFO 가져오기 성공")

    page_id = PAGE_INFO.get('id')

    query_result1_dailySales=load_df_from_gcs(bucket, st1)
    query_result1_monthlySales=load_df_from_gcs(bucket, st2)

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

    print(f"✅ GCS 파일 다운로드 완료")

    ########### (2) 그래프 업로드
    # 일자별 매출
    # 그래프는 파일 저장후 올리는 구조밖에 되지않아서
    # 1) 업로드 객체 생성 (file_upload 생성)
    create_url = "https://api.notion.com/v1/file_uploads"
    payload = {
        "filename": filename,
        "content_type": "image/png"
    }
    # headers_json = headers_json (이 줄은 headers_json이 상위 스코프에 정의되어 있음을 의미)
    resp = requests.post(create_url, headers=headers_json, data=json.dumps(payload))
    resp.raise_for_status()
    file_upload = resp.json()

    print(f"📊 API 응답: {file_upload}")
    file_upload_id = file_upload["id"]  # 업로드 ID

    # 2) 파일 바이너리 전송 (multipart/form-data) - 수정된 부분
    send_url = f"https://api.notion.com/v1/file_uploads/{file_upload_id}/send"
    files = {"file": (filename, BytesIO(image_bytes), "image/png")}

    # ★★★★★
    # [수정] 401 오류 해결: headers_json에서 인증 정보를 가져옵니다.
    # [수정] 400 오류 해결: Content-Type을 제거하여 requests가 자동 생성하도록 합니다.
    #
    # headers_json이 아래와 같다고 가정:
    # headers_json = {
    #     "Authorization": f"Bearer {NOTION_TOKEN}",
    #     "Notion-Version": NOTION_VERSION,
    #     "Content-Type": "application/json" 
    # }

    # Content-Type을 제외한 나머지 헤더(인증, 버전)를 사용합니다.
    headers_send = {
        "Authorization": headers_json.get("Authorization"),
        "Notion-Version": headers_json.get("Notion-Version")
    }

    try:
        # [수정] headers=headers_upload 대신 headers=headers_send 를 사용
        send_resp = requests.post(send_url, headers=headers_send, files=files) 
        send_resp.raise_for_status()
        print(f"✅ NOTION 이미지 업로드 완료")
    except Exception as e:
        print(f"작업 실패 : {e}")
        # 실패 시 응답 내용을 확인하면 디버깅에 도움이 됩니다.
        if hasattr(e, 'response') and e.response is not None:
            print(f"오류 응답: {e.response.text}")
        raise e
    
    # 3) 이미지 블록으로 페이지에 첨부
    append_url = f"https://api.notion.com/v1/blocks/{page_id}/children"
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

    print(f"✅ 이미지 블록으로 페이지 첨부 완료")

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

    print(f"GEMINI 문의 처리 시작")
    response1_salesComment = daily_revenue_gemini(service_sub, genai_client, MODEL_NAME, SYSTEM_INSTRUCTION, st1, st2, bucket)

    ## 제미나이
    blocks = md_to_notion_blocks(response1_salesComment)
    notion.blocks.children.append(
        block_id=PAGE_INFO["id"],
        children=blocks
    )

    print(f"GEMINI 답변 등록 완료")

    return True



