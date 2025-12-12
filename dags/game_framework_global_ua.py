import pandas as pd
from google.cloud import bigquery
from google import genai
from google.genai import types
from google.cloud import storage

# 그래프 관련 패키지
from io import BytesIO
from typing import List, Tuple

# 전처리 관련 패키지
import numpy as np
import os 
import time
from notion_client import Client
import requests
import json
from datetime import datetime, timezone, timedelta
from airflow.operators.python import get_current_context
from airflow.sdk import get_current_context
from game_framework_util import load_df_from_gcs, save_df_to_gcs, query_run_method, df_to_notion_table_under_toggle, md_to_notion_blocks

PROJECT_ID = "data-science-division-216308"
LOCATION = "us-central1"



def _setup_matplotlib_and_fonts():
    """matplotlib 및 폰트 설정 (함수 내부에서만 import)"""
    import matplotlib.pyplot as plt
    import seaborn as sns
    import matplotlib as mpl
    import matplotlib.font_manager as fm
    from matplotlib.ticker import FuncFormatter, StrMethodFormatter, PercentFormatter, MultipleLocator
    import matplotlib.dates as mdates
    from matplotlib import rcParams
    from matplotlib.patches import Rectangle
    
    # 폰트 설정 (여기서만 실행)
    plt.rcParams['font.sans-serif'] = ['Noto Sans CJK JP', 'DejaVu Sans', 'Noto Sans']
    plt.rcParams['font.size'] = 10
    plt.rcParams['axes.unicode_minus'] = False
    plt.rcParams['lines.linewidth'] = 1.5
    
    print("✓ Matplotlib 한글 폰트 설정 완료")
    
    return {
        'plt': plt,
        'sns': sns,
        'mpl': mpl,
        'fm': fm,
        'FuncFormatter': FuncFormatter,
        'StrMethodFormatter': StrMethodFormatter,
        'PercentFormatter': PercentFormatter,
        'MultipleLocator': MultipleLocator,
        'mdates': mdates,
    }

def _setup_image_libs():
    """이미지 관련 라이브러리"""
    from PIL import Image, ImageDraw, ImageFont
    from io import BytesIO
    
    return {
        'Image': Image,
        'ImageDraw': ImageDraw,
        'ImageFont': ImageFont,
        'BytesIO': BytesIO,
    }

## 이번달 가입 유저의 국가별 매출
def cohort_by_country_revenue(joyplegameid: int, gameidx: str, bigquery_client, bucket, **context):
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
    query_result=query_run_method(service_sub='3_global_ua',bigquery_client=bigquery_client, query=query)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    gcs_path = f"{gameidx}/{timestamp}.parquet"
        
    saved_path = save_df_to_gcs(query_result, bucket, gcs_path)
    
    return saved_path

## 이번달 국가별 COST
def cohort_by_country_cost(joyplegameid: int, gameidx: str, bigquery_client, bucket, **context):
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
    query_result =query_run_method(service_sub='3_global_ua',bigquery_client=bigquery_client, query=query)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    gcs_path = f"{gameidx}/{timestamp}.parquet"
        
    saved_path = save_df_to_gcs(query_result, bucket, gcs_path)
    
    return saved_path


## 국가별 rev, cost 프롬프트
### 4> 일자별 매출에 대한 제미나이 코멘트
def cohort_by_gemini(gameidx:str, service_sub: str, genai_client, MODEL_NAME, SYSTEM_INSTRUCTION:list, path_daily_revenue, path_monthly_revenue, bucket, PROJECT_ID, LOCATION, **context):
    
    from google.genai import Client
    genai_client = Client(vertexai=True,project=PROJECT_ID,location=LOCATION)

    cohort_country_revenue = load_df_from_gcs(bucket, path_daily_revenue)
    cohort_country_cost = load_df_from_gcs(bucket, path_monthly_revenue)

    RUN_ID = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    LABELS = {"datascience_division_service": 'gameinsight_framework',
            "run_id": RUN_ID,
            "datascience_division_service_sub" : service_sub}

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
            labels=LABELS
        )
    )

    # GCS에 업로드
    print("📤 GCS에 제미나이 코멘트 업로드 중...")
    gcs_response_path = f"{gameidx}/response3_revAndCostByCountry.text"
    blob = bucket.blob(gcs_response_path)
    blob.upload_from_string(
        response3_revAndCostByCountry.text,
        content_type='text/markdown; charset=utf-8'
    )

    # 코멘트 출력
    return response3_revAndCostByCountry.text


# 코멘트 정리 ( 향후 요약에 사용하기 용도 )
#gemini_result.loc[len(gemini_result)] = response.text

## OS별 매출
def os_rev(joyplegameid: int, gameidx: str, bigquery_client, bucket, **context):
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
    query_result = query_run_method('3_global_ua', bigquery_client, query)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    gcs_path = f"{gameidx}/{timestamp}.parquet"
        
    saved_path = save_df_to_gcs(query_result, bucket, gcs_path)

    return saved_path


## OS별 cost
def os_cost(joyplegameid: int, gameidx: str, bigquery_client, bucket, **context):
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
    query_result = query_run_method('3_global_ua', bigquery_client, query)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    gcs_path = f"{gameidx}/{timestamp}.parquet"
        
    saved_path = save_df_to_gcs(query_result, bucket, gcs_path)

    return saved_path




### 4> 일자별 매출에 대한 제미나이 코멘트

#client = genai.Client(api_key="AIzaSyAVv2B6DM6w9jd1MxiP3PbzAEMkl97SCGY")
def os_by_gemini(gameidx:str, service_sub: str, genai_client, MODEL_NAME, SYSTEM_INSTRUCTION:list, path_daily_revenue, path_monthly_revenue, bucket, PROJECT_ID, LOCATION, **context):
    
    from google.genai import Client
    genai_client = Client(vertexai=True,project=PROJECT_ID,location=LOCATION)

    os_rev_df = load_df_from_gcs(bucket, path_daily_revenue)
    os_cost_df = load_df_from_gcs(bucket, path_monthly_revenue)

    RUN_ID = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    LABELS = {"datascience_division_service": 'gameinsight_framework',
            "run_id": RUN_ID,
            "datascience_division_service_sub" : service_sub}

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
            labels=LABELS
        )

    )

    # GCS에 업로드
    print("📤 GCS에 제미나이 코멘트 업로드 중...")
    gcs_response_path = f"{gameidx}/response3_revAndCostByOs.text"
    blob = bucket.blob(gcs_response_path)
    blob.upload_from_string(
        response3_revAndCostByOs.text,
        content_type='text/markdown; charset=utf-8'
    )

    # 코멘트 출력
    return response3_revAndCostByOs.text

# 코멘트 정리 ( 향후 요약에 사용하기 용도 )
#gemini_result.loc[len(gemini_result)] = response.text

### 그래프 그리기
## 국가별 매출

def by_country_revenue_graph_draw(gameidx: str, gcs_path:str, bucket, **context):
    ## 한글 폰트 설정
    viz_libs = _setup_matplotlib_and_fonts()
    plt = viz_libs['plt']
    
    query_result3_revByCountry = load_df_from_gcs(bucket, gcs_path)
    query_result3_revByCountry = query_result3_revByCountry.sort_values(by="rev", ascending=False)

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



def by_country_cost_graph_draw(gameidx: str, gcs_path:str, bucket, **context):
    
    # Step 1: matplotlib 설정 로드
    viz_libs = _setup_matplotlib_and_fonts()
    plt = viz_libs['plt']

    query_result3_costByCountry = load_df_from_gcs(bucket, gcs_path)
    query_result3_costByCountry = query_result3_costByCountry.sort_values(by="cost", ascending=False)

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


def merge_contry_graph(gameidx: str, gcs_path_1:str, gcs_path_2:str, bucket, **context):
    # Step 1: Image 라이브러리 로드
    image_libs = _setup_image_libs()
    Image = image_libs['Image']
    BytesIO = image_libs['BytesIO']

    p1=by_country_revenue_graph_draw(gameidx, gcs_path_1, bucket)
    p2=by_country_cost_graph_draw(gameidx, gcs_path_2, bucket)

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
    gcs_path = f'{gameidx}/graph3_revAndCostByCountry.png'
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(output_buffer.getvalue(), content_type='image/png')

    print(f"✅ GCS 업로드 완료: gs://{bucket.name}/{gcs_path}")

    return gcs_path



### OS 별 매출
def os_rev_graph_draw(gameidx: str, gcs_path:str, bucket, **context):

    # Step 1: matplotlib 설정 로드
    viz_libs = _setup_matplotlib_and_fonts()
    plt = viz_libs['plt']

    query_result3_revByOs = load_df_from_gcs(bucket, gcs_path)

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
def os_cost_graph_draw(gameidx: str, gcs_path:str, bucket, **context):

    # Step 1: matplotlib 설정 로드
    viz_libs = _setup_matplotlib_and_fonts()
    plt = viz_libs['plt']

    query_result3_costByOs = load_df_from_gcs(bucket, gcs_path)

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


def merge_os_graph(gameidx: str, gcs_path_1:str, gcs_path_2:str, bucket, **context):

    # Step 1: Image 라이브러리 로드
    image_libs = _setup_image_libs()
    Image = image_libs['Image']
    BytesIO = image_libs['BytesIO']

    p1 = os_rev_graph_draw(gameidx, gcs_path_1, bucket)
    p2 = os_cost_graph_draw(gameidx, gcs_path_2, bucket)

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
    gcs_path = f'{gameidx}/graph3_revAndCostByOs.png'
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(output_buffer.getvalue(), content_type='image/png')

    print(f"✅ GCS 업로드 완료: gs://{bucket.name}/{gcs_path}")

    return gcs_path


#### 노션에 업로드

def country_data_upload_to_notion(gameidx: str, st1, st2, service_sub, genai_client, MODEL_NAME, SYSTEM_INSTRUCTION, notion, bucket, headers_json, **context):

    current_context = get_current_context()

    PAGE_INFO=current_context['task_instance'].xcom_pull(
        task_ids = 'make_gameframework_notion_page_wraper',
        key='page_info'
    )

    query_result3_revByCountry=load_df_from_gcs(bucket, st1)
    query_result3_costByCountry=load_df_from_gcs(bucket, st2)

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


    gcs_path = merge_contry_graph(gameidx=gameidx, gcs_path_1=st1, gcs_path_2=st2, bucket=bucket)
    blob = bucket.blob(gcs_path)
    image_bytes = blob.download_as_bytes()
    print(f"✅ gcs_path : {gcs_path}")
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

    headers_json_patch = headers_json
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

    text = cohort_by_gemini(
        gameidx=gameidx,
        service_sub=service_sub,
        genai_client=genai_client,
        MODEL_NAME = MODEL_NAME,
        SYSTEM_INSTRUCTION=SYSTEM_INSTRUCTION,
        path_daily_revenue=st1,
        path_monthly_revenue=st2,
        bucket=bucket,
        PROJECT_ID=PROJECT_ID,
        LOCATION=LOCATION
    )
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
def os_data_upload_to_notion(gameidx: str, st1, st2, service_sub, genai_client, MODEL_NAME, SYSTEM_INSTRUCTION, notion, bucket, headers_json, **context):

    current_context = get_current_context()

    PAGE_INFO=current_context['task_instance'].xcom_pull(
        task_ids = 'make_gameframework_notion_page_wraper',
        key='page_info'
    )

    print(f"✅ PAGE_INFO 가져오기 성공")

    page_id = PAGE_INFO.get('id')

    query_result3_costByOs=load_df_from_gcs(bucket, st1)
    query_result3_revByOs=load_df_from_gcs(bucket, st2)

    gcs_path = merge_os_graph(gameidx=gameidx, gcs_path_1=st1, gcs_path_2=st2, bucket=bucket, **context)
    blob = bucket.blob(gcs_path)
    image_bytes = blob.download_as_bytes()
    filename = gcs_path.split('/')[-1]

    print(f"✅ GCS 파일 다운로드 완료")

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

    headers_json_patch = headers_json
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
    gemini_text = os_by_gemini(
        gameidx=gameidx,
        service_sub=service_sub, 
        genai_client=genai_client, 
        MODEL_NAME=MODEL_NAME, 
        SYSTEM_INSTRUCTION=SYSTEM_INSTRUCTION,
        path_daily_revenue=st1,
        path_monthly_revenue=st2,
        bucket=bucket,
        PROJECT_ID=PROJECT_ID,
        LOCATION=LOCATION)
    blocks = md_to_notion_blocks(gemini_text)
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

def country_group_rev(joyplegameid: int, gameidx: str, bigquery_client, bucket, **context):
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
    query_result = query_run_method('3_global_ua', bigquery_client, query)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    gcs_path = f"{gameidx}/{timestamp}.parquet"
        
    saved_path = save_df_to_gcs(query_result, bucket, gcs_path)

    return saved_path


def country_group_to_df(joyplegameid:int, gameidx:str, bigquery_client, bucket, **context):

    saved_path = country_group_rev(joyplegameid=joyplegameid, gameidx=gameidx, bigquery_client=bigquery_client, bucket=bucket, **context)
    query_result = load_df_from_gcs(bucket=bucket, path=saved_path)

    query_result = query_result.sort_values(by="Sales", ascending=False)

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

    grouped_dfs = {
    country: df[df.sum().sort_values(ascending=False).index]
    for country, df in grouped_dfs.items()
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



def country_group_to_df_gemini(service_sub: str, genai_client, MODEL_NAME, SYSTEM_INSTRUCTION:list, path_daily_revenue, bucket, **context):

    from google.genai import Client
    genai_client = Client(vertexai=True,project=PROJECT_ID,location=LOCATION)

    query_result = load_df_from_gcs(bucket=bucket, path=path_daily_revenue)

    RUN_ID = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    LABELS = {"datascience_division_service": 'gameinsight_framework',
            "run_id": RUN_ID,
            f"datascience_division_service_sub" : service_sub}

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



def country_group_df_draw(joyplegameid: int, gameidx: str, bigquery_client, bucket, **context):

    # Step 1: matplotlib 설정 로드
    viz_libs = _setup_matplotlib_and_fonts()
    plt = viz_libs['plt']
    FuncFormatter = viz_libs['FuncFormatter']


    gcs_paths = []
    grouped_dfs, _ = country_group_to_df(joyplegameid=joyplegameid, gameidx=gameidx, bigquery_client=bigquery_client, bucket=bucket, **context)

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

    # Step 1: Image 라이브러리 로드
    image_libs = _setup_image_libs()
    Image = image_libs['Image']
    BytesIO = image_libs['BytesIO']

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


def merge_country_group_df_draw(joyplegameid: int, gameidx: str, bigquery_client, bucket, **context):
    """
    Airflow DAG에서 사용할 wrapper 함수
    """
    from google.cloud import storage
    
    # GCS 클라이언트 및 버킷 초기화
    client = storage.Client()
    bucket = client.bucket("game-framework1")  # 버킷명 수정 필요
    
    # 이미지 저장 경로 가져오기 (리스트)
    img_gcs_list = country_group_df_draw(joyplegameid, gameidx, bigquery_client, bucket)
    
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


def country_group_data_upload_to_notion(joyplegameid: int, gameidx: str, st1, service_sub: str, 
                                        genai_client, MODEL_NAME, SYSTEM_INSTRUCTION, notion, bigquery_client,
                                        bucket, headers_json, NOTION_TOKEN, NOTION_VERSION, 
                                        bucket_name: str = "game-framework1", merged_image_dir: str= "merged", **context):

    current_context = get_current_context()

    PAGE_INFO=current_context['task_instance'].xcom_pull(
        task_ids = 'make_gameframework_notion_page_wraper',
        key='page_info'
    )

    print(f"📊 page_info type: {type(PAGE_INFO)}")
    print(f"📊 page_info: {PAGE_INFO}")
    print(f"✅ PAGE_INFO 가져오기 성공")

    page_id = PAGE_INFO.get('id')

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
    headers_json = headers_json
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

    _, grouped_dfs_union =country_group_to_df(
        joyplegameid=joyplegameid, 
        gameidx=gameidx, 
        bigquery_client=bigquery_client,
        bucket=bucket,
        **context
    )

    resp = df_to_notion_table_under_toggle(
        notion=notion,
        page_id=PAGE_INFO['id'],
        df=grouped_dfs_union,
        toggle_title="📊 로데이터 - 국가별 X 결제처별 지표 ",
        max_first_batch_rows=90,
        batch_size=100,
    )


    text = country_group_to_df_gemini(
        service_sub=service_sub,
        genai_client=genai_client,
        MODEL_NAME=MODEL_NAME,
        SYSTEM_INSTRUCTION=SYSTEM_INSTRUCTION,
        path_daily_revenue=st1,
        bucket=bucket
    )

    blocks = md_to_notion_blocks(text)
    notion.blocks.children.append(
        block_id=PAGE_INFO['id'],
        children=blocks
    )

    return True