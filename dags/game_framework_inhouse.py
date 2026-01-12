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
from airflow.sdk import get_current_context
from game_framework_util import *

PROJECT_ID = "data-science-division-216308"
LOCATION = "us-central1"

import logging

# matplotlib.category의 로그 레벨을 WARNING으로 설정 (INFO 로그 무시)
logging.getLogger('matplotlib.category').setLevel(logging.WARNING)

## 한글 폰트 설정
setup_korean_font()

# 2. 자체결제 매출
def inhouse_sales_query(joyplegameid: int, gameidx: str, bigquery_client, bucket, **context):
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
    query_result = query_run_method('2_inhouse_sales', bigquery_client, query)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    gcs_path = f"{gameidx}/{timestamp}.parquet"
        
    saved_path = save_df_to_gcs(query_result, bucket, gcs_path)

    return saved_path
    

### 2> 24년부터 월별 자체결제 매출
def inhouse_sales_before24_query(joyplegameid: int, gameidx: str, bigquery_client, bucket, **context):
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

    query_result = query_run_method('2_inhouse_sales', bigquery_client, query)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    gcs_path = f"{gameidx}/{timestamp}.parquet"
        
    saved_path = save_df_to_gcs(query_result, bucket, gcs_path)

    return saved_path


## 제미나이 프롬프트 
def inhouses_revenue_gemini(gameidx:str, service_sub: str, genai_client, MODEL_NAME, SYSTEM_INSTRUCTION:list, path_daily_revenue, path_monthly_revenue, bucket, PROJECT_ID, LOCATION, **context):
    
    inhouse_sales = load_df_from_gcs(bucket, path_daily_revenue)
    inhouse_sales_before24 = load_df_from_gcs(bucket, path_monthly_revenue)

    RUN_ID = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    LABELS = {"datascience_division_service": 'gameinsight_framework',
            "run_id": RUN_ID,
            "datascience_division_service_sub" : service_sub}

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
            system_instruction=SYSTEM_INSTRUCTION,
            #tools=[rag_retrieval_tool_test],
            temperature=0.5,
            labels=LABELS
            # max_output_tokens=2048
        )
    )

    # GCS에 업로드
    print("📤 GCS에 제미나이 코멘트 업로드 중...")
    gcs_response_path = f"{gameidx}/response2_selfPaymentSales.text"
    blob = bucket.blob(gcs_response_path)
    blob.upload_from_string(
        response2_selfPaymentSales.text,
        content_type='text/markdown; charset=utf-8'
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

def inhouse_revenue_graph_draw(gameidx: str, gcs_path:str, bucket, **context):

    query_result2_dailySelfPaymentSales = load_df_from_gcs(bucket, gcs_path)
    
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

def inhouse_revenue_monthly_graph_draw(gameidx: str, gcs_path:str, bucket, **context):
    
    query_result2_monthlySelfPaymentSales = load_df_from_gcs(bucket, gcs_path)

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

def merge_inhouse_graph(gameidx: str, gcs_path_1:str, gcs_path_2:str, bucket, **context):

    p1 = inhouse_revenue_graph_draw(gameidx, gcs_path_1, bucket)
    print(f"✅ p1 경로: {p1}")

    p2 = inhouse_revenue_monthly_graph_draw(gameidx, gcs_path_2, bucket)
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

    gap = 0  # 이미지 사이 여백(px). 필요하면 20 등으로 변경
    bg = (255, 255, 255, 0)  # 전체 배경(투명). 흰색 원하면 (255,255,255,255)

    out = Image.new("RGBA", (im1_p.width + gap + im2_p.width, target_h), bg)
    out.paste(im1_p, (0, 0), im1_p)
    out.paste(im2_p, (im1_p.width + gap, 0), im2_p)

    # 3) GCS에 저장
    print(f"📤 GCS에 업로드 중...")
    output_buffer = BytesIO()
    out.save(output_buffer, format='PNG')
    output_buffer.seek(0)

    # GCS 경로
    gcs_path = f'{gameidx}/graph2_selfPaymentSales.png'
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(output_buffer.getvalue(), content_type='image/png')

    print(f"✅ GCS 업로드 완료: gs://{bucket.name}/{gcs_path}")

    return gcs_path



def inhouse_revenue_data_upload_to_notion(gameidx: str, st1, st2, service_sub, genai_client, MODEL_NAME, SYSTEM_INSTRUCTION, notion, bucket, headers_json, NOTION_TOKEN, NOTION_VERSION,  **context):

    if 'task_instance' in context:
        ti = context['task_instance']
    else:
        current_context = get_current_context()
        ti = current_context['task_instance']
    
    PAGE_INFO = ti.xcom_pull(
        task_ids='make_gameframework_notion_page_wraper',
        key='page_info'
    )

    print(f"📊 page_info type: {type(PAGE_INFO)}")
    print(f"📊 page_info: {PAGE_INFO}")
    print(f"✅ PAGE_INFO 가져오기 성공")

    page_id = PAGE_INFO.get('id')

    query_result1_inhouseSales=load_df_from_gcs(bucket, st1)
    query_result1_inhouseMonthlySales=load_df_from_gcs(bucket, st2)
    
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

    gcs_path = merge_inhouse_graph(gameidx=gameidx, gcs_path_1=st1, gcs_path_2=st2, bucket=bucket, **context)
    blob = bucket.blob(gcs_path)
    image_bytes = blob.download_as_bytes()
    filename = gcs_path.split('/')[-1]

    print(f"✅ GCS 파일 다운로드 완료")

    ########### (2) 그래프 업로드
    create_url = "https://api.notion.com/v1/file_uploads"
    payload = {
        "filename": filename,
        "content_type": "image/png"
    }
    headers_json = headers_json
    resp = requests.post(create_url, headers=headers_json, data=json.dumps(payload))
    resp.raise_for_status()
    file_upload = resp.json()
    
    print(f"📊 API 응답: {file_upload}")
    file_upload_id = file_upload["id"]   # 업로드 ID
    upload_url = file_upload['upload_url']

    # 2) 파일 바이너리 전송 (multipart/form-data) - 수정된 부분
    send_url = f"https://api.notion.com/v1/file_uploads/{file_upload_id}/send"
    files = {"file": (filename, BytesIO(image_bytes), "image/png")}

    # 2) 이미지 업로드
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
    print(f"1️⃣ GEMINI 문의 처리 시작")
    gemini_text = inhouses_revenue_gemini(gameidx, service_sub, genai_client, MODEL_NAME, SYSTEM_INSTRUCTION, st1, st2, bucket, PROJECT_ID=PROJECT_ID, LOCATION=LOCATION)
    blocks = md_to_notion_blocks(gemini_text)

    notion.blocks.children.append(
        block_id=PAGE_INFO['id'],
        children=blocks
    )

    print(f"2️⃣ GEMINI 답변 등록 완료")

    return True

