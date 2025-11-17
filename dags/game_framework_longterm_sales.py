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

PROJECT_ID = "data-science-division-216308"
LOCATION = "us-central1"


### 월별 일 평균 매출
def monthly_day_average_rev(joyplegameid:int, gameidx:str, bucket, **context):
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
def monthly_day_average_rev_gemini(service_sub: str, path_monthly_day_average_rev:str, MODEL_NAME:str, bucket, **context):

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
def rgroup_rev_DOD(joyplegameid:int, gameidx:str, bucket, **context):
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
def rgroup_rev_total(joyplegameid:int, gameidx:str, bucket, **context):
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
def rgroup_rev_total_gemini(service_sub: str, path_rgroup_rev_DOD:str, MODEL_NAME:str, bucket, **context):

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
def rev_cohort_year(joyplegameid:int, gameidx:str, bucket, **context):
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


def rev_cohort_year_gemini(service_sub: str, path_rev_cohort_year:str, MODEL_NAME:str, bucket, **context):

    from google.genai import Client
    genai_client = Client(vertexai=True,project=PROJECT_ID,location=LOCATION)

    pv2 = load_df_from_gcs(bucket, path_rev_cohort_year)

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


def monthly_day_average_rev_table_draw(gameidx:str, path_monthly_day_average_rev, bucket, **context):

    from google.genai import Client
    genai_client = Client(vertexai=True,project=PROJECT_ID,location=LOCATION)

    df = load_df_from_gcs(bucket, path_monthly_day_average_rev)

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
def monthly_day_average_rev_graph_draw(gameidx:str, path_monthly_day_average_rev:str, bucket, **context):

    from google.genai import Client
    genai_client = Client(vertexai=True,project=PROJECT_ID,location=LOCATION)

    query_result5_dailyAvgRevenue = load_df_from_gcs(bucket, path_monthly_day_average_rev)

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


def monthly_day_average_merge_graph(gameidx:str, bucket, **context):
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
def rgroup_rev_DOD_table_draw(gameidx:str, path_rgroup_rev_DOD:str, bucket, **context):

    from google.genai import Client
    genai_client = Client(vertexai=True,project=PROJECT_ID,location=LOCATION)

    query_result5_monthlyRgroupRevenue = load_df_from_gcs(bucket, path_rgroup_rev_DOD)
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
def rgroup_pu_DOD_table_draw(gameidx:str, path_rgroup_rev_DOD:str, bucket, **context):

    from google.genai import Client
    genai_client = Client(vertexai=True,project=PROJECT_ID,location=LOCATION)

    query_result5_monthlyRgroupRevenue = load_df_from_gcs(bucket, path_rgroup_rev_DOD)
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


def merge_rgroup_rev_pu_ALL_table(gameidx: str, bucket, **context):
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


def merge_rgroup_rev_pu_table(gameidx:str, bucket, **context):
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
def rgroup_rev_total_table_draw(gameidx:str, path_rgroup_rev_total:str, bucket, **context):

    from google.genai import Client
    genai_client = Client(vertexai=True,project=PROJECT_ID,location=LOCATION)

    query_result5_monthlyRgroupRevenue = load_df_from_gcs(bucket, path_rgroup_rev_total)

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


def rgroup_pu_total_table_draw(gameidx:str, path_rgroup_rev_total:str, bucket, **context):

    from google.genai import Client
    genai_client = Client(vertexai=True,project=PROJECT_ID,location=LOCATION)

    query_result5_monthlyRgroupRevenue = load_df_from_gcs(bucket, path_rgroup_rev_total)

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
def merge_rgroup_total_rev_pu_table(gameidx: str, bucket, **context):
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

def cohort_rev_table_draw(gameidx:str, path_rev_cohort_year_pv2:str, bucket, **context):

    from google.genai import Client
    genai_client = Client(vertexai=True,project=PROJECT_ID,location=LOCATION)

    df = load_df_from_gcs(bucket, path_rev_cohort_year_pv2)
    
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
    
    gcs_path = render_table_image(df=df, gameidx=gameidx)
    return gcs_path


########### 장기적 매출 현황 업로드 to 노션
def longterm_rev_upload_notion(joyplegameid: int, gameidx:str, service_sub:str, 
                               path_monthly_day_average_rev:str, 
                               NOTION_TOKEN:str, NOTION_VERSION:str, notion, bucket, headers_json,**context):

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
    headers_json = headers_json
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

    query_result5_dailyAvgRevenue = load_df_from_gcs(bucket, path_monthly_day_average_rev)

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
def longterm_rev_upload_notion(joyplegameid: int, gameidx:str, service_sub:str, 
                               path_rgroup_rev_total:str, path_rgroup_rev_DOD:str,
                               NOTION_TOKEN:str, NOTION_VERSION:str, notion, bucket, headers_json, **context):

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
    headers_json = headers_json
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

    query_result5_monthlyRgroupRevenueALL = load_df_from_gcs(bucket, path_rgroup_rev_total)
    query_result5_monthlyRgroupRevenue = load_df_from_gcs(bucket, path_rgroup_rev_DOD)


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