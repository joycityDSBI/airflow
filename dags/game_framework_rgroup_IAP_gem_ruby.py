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


def rev_group_rev_pu(joyplegameid: int, gameidx: str, bigquery_client, bucket, **context):
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
        and logdatekst<=LAST_DAY(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY), MONTH)
        and joypleGameID = {joyplegameid}
    )
    group by 1,2
    order by 1
    """

    query_result =query_run_method(service_sub='4_detail_sales', bigquery_client=bigquery_client, query=query)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    gcs_path = f"{gameidx}/{timestamp}.parquet"
        
    saved_path = save_df_to_gcs(query_result, bucket, gcs_path)
    
    return saved_path


def rev_group_rev_pu_gemini(service_sub: str, genai_client, MODEL_NAME, SYSTEM_INSTRUCTION:list, rev_group_rev_pu_path, bucket, PROJECT_ID, LOCATION, **context):

    from google.genai import Client
    genai_client = Client(vertexai=True,project=PROJECT_ID,location=LOCATION)
    rev_group_rev_pu_data = load_df_from_gcs(bucket, rev_group_rev_pu_path)

    RUN_ID = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    LABELS = {"datascience_division_service": 'gameinsight_framework',
            "run_id": RUN_ID,
            "datascience_division_service_sub" : service_sub}

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


def iap_gem_ruby(joyplegameid:int, gameidx: str, databaseschema: str, bigquery_client, bucket, **context):
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
    query_result = query_run_method('4_detail_sales', bigquery_client, query)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    gcs_path = f"{gameidx}/{timestamp}.parquet"
        
    saved_path = save_df_to_gcs(query_result, bucket, gcs_path)

    return saved_path


def iap_gem_ruby_history(gameidx: str, bigquery_client, bucket, **context):
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

    query_result = query_run_method('4_detail_sales', bigquery_client=bigquery_client, query=query)
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

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    gcs_path = f"{gameidx}/{timestamp}.parquet"
        
    saved_path = save_df_to_gcs(query_result4_ingameHistory, bucket, gcs_path)

    return saved_path


def iap_gem_ruby_gemini(service_sub: str, genai_client, MODEL_NAME, SYSTEM_INSTRUCTION:list, path_iapgemruby, path_iapgemruby_history, bucket, PROJECT_ID, LOCATION, **context):

    from google.genai import Client
    genai_client = Client(vertexai=True,project=PROJECT_ID,location=LOCATION)

    query_result4_salesByPackage = load_df_from_gcs(bucket, path_iapgemruby)
    query_result4_ingameHistory = load_df_from_gcs(bucket, path_iapgemruby_history)

    RUN_ID = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    LABELS = {"datascience_division_service": 'gameinsight_framework',
            "run_id": RUN_ID,
            "datascience_division_service_sub" : service_sub}

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


def iap_df(gameidx:str, databaseschema: str, bigquery_client, bucket, **context):
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

    query_result =query_run_method('4_detail_sales', bigquery_client, query)
    # 카테고리별로 Pivot

    query_result4_salesByPackage_IAP = query_result.pivot_table(
        index=["month", "week", "logdate_kst"],  # 두 컬럼 기준으로 인덱스 구성
        columns="cat_package_grouped",
        values="rev",
        aggfunc="sum",
        fill_value=0
    ).reset_index()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    gcs_path = f"{gameidx}/{timestamp}.parquet"
        
    saved_path = save_df_to_gcs(query_result4_salesByPackage_IAP, bucket, gcs_path)

    return saved_path


def gem_df(joyplegameid: int, gameidx:str, bigquery_client, bucket, **context):
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

    query_result = query_run_method('4_detail_sales', bigquery_client, query)

    query_result4_salesByPackage_GEM = query_result.pivot_table(
        index=["month", "week", "logdate_kst"],  # 두 컬럼 기준으로 인덱스 구성
        columns="cat_package_grouped",
        values="usegem",
        aggfunc="sum",
        fill_value=0
    ).reset_index()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    gcs_path = f"{gameidx}/{timestamp}.parquet"
        
    saved_path = save_df_to_gcs(query_result4_salesByPackage_GEM, bucket, gcs_path)

    return saved_path


def ruby_df(joyplegameid: int, gameidx:str, bigquery_client, bucket, **context):
    
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

    query_result =query_run_method('4_detail_sales', bigquery_client, query)

    query_result4_salesByPackage_RUBY = query_result.pivot_table(
        index=["month", "week", "logdate_kst"],  # 두 컬럼 기준으로 인덱스 구성
        columns="cat_package_grouped",
        values="useruby",
        aggfunc="sum",
        fill_value=0
    ).reset_index()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    gcs_path = f"{gameidx}/{timestamp}.parquet"
        
    saved_path = save_df_to_gcs(query_result4_salesByPackage_RUBY, bucket, gcs_path)

    return saved_path


def iap_df_gemini(service_sub: str, genai_client, MODEL_NAME, SYSTEM_INSTRUCTION:list, path_iap_df, path_iapgemruby_history, bucket, PROJECT_ID, LOCATION, **context):
    
    from google.genai import Client
    genai_client = Client(vertexai=True,project=PROJECT_ID,location=LOCATION)

    iap_df = load_df_from_gcs(bucket, path_iap_df)
    iap_gem_ruby_history = load_df_from_gcs(bucket, path_iapgemruby_history)
    
    RUN_ID = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    LABELS = {"datascience_division_service": 'gameinsight_framework',
            "run_id": RUN_ID,
            "datascience_division_service_sub" : service_sub}
    
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


def gem_df_gemini(service_sub: str, genai_client, MODEL_NAME, SYSTEM_INSTRUCTION:list, path_gem_df, path_iapgemruby_history, bucket, PROJECT_ID, LOCATION, **context):
    
    from google.genai import Client
    genai_client = Client(vertexai=True,project=PROJECT_ID,location=LOCATION)

    gem_df = load_df_from_gcs(bucket, path_gem_df)
    iap_gem_ruby_history = load_df_from_gcs(bucket, path_iapgemruby_history)

    RUN_ID = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    LABELS = {"datascience_division_service": 'gameinsight_framework',
            "run_id": RUN_ID,
            "datascience_division_service_sub" : service_sub}
    
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


def ruby_df_gemini(service_sub: str, genai_client, MODEL_NAME, SYSTEM_INSTRUCTION:list, path_ruby_df, path_iapgemruby_history, bucket, PROJECT_ID, LOCATION, **context):
    from google.genai import Client
    genai_client = Client(vertexai=True,project=PROJECT_ID,location=LOCATION)

    ruby_df = load_df_from_gcs(bucket, path_ruby_df)
    iap_gem_ruby_history = load_df_from_gcs(bucket, path_iapgemruby_history)

    RUN_ID = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    LABELS = {"datascience_division_service": 'gameinsight_framework',
            "run_id": RUN_ID,
            "datascience_division_service_sub" : service_sub}
    
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


def weekly_iapcategory_rev(joyplegameid: int, gameidx: str, databaseschema:str, bigquery_client, bucket, **context):
    
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

    query_result =query_run_method('4_detail_sales', bigquery_client, query)

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

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    gcs_path_1 = f"{gameidx}/{timestamp}_1.parquet"
    gcs_path_2 = f"{gameidx}/{timestamp}_2.parquet"
        
    saved_path_1 = save_df_to_gcs(query_result4_salesByCategory, bucket, gcs_path_1)

    cols_df = pd.DataFrame({'columns': cols})
    saved_path_2 = save_df_to_gcs(cols_df, bucket, gcs_path_2)

    return saved_path_1, saved_path_2


def iapcategory_rev_df_gemini(service_sub: str, genai_client, MODEL_NAME, SYSTEM_INSTRUCTION:list, path_weekly_iapcategory_rev, bucket, PROJECT_ID, LOCATION, **context):

    from google.genai import Client
    genai_client = Client(vertexai=True,project=PROJECT_ID,location=LOCATION)

    weekly_iapcategory_rev = load_df_from_gcs(bucket, path_weekly_iapcategory_rev)

    RUN_ID = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    LABELS = {"datascience_division_service": 'gameinsight_framework',
            "run_id": RUN_ID,
            "datascience_division_service_sub" : service_sub}

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



def top3_items_by_category(joyplegameid: int, gameidx:str, service_sub: str, databaseschema: str, genai_client, MODEL_NAME, SYSTEM_INSTRUCTION:list, 
                           path_weekly_iapcategory_rev_cols, path_weekly_iapcategory_rev, bigquery_client,
                           bucket, PROJECT_ID, LOCATION, **context):

    from google.genai import Client
    genai_client = Client(vertexai=True,project=PROJECT_ID,location=LOCATION)
    
    weekly_iapcategory_rev_cols = load_df_from_gcs(bucket, path_weekly_iapcategory_rev_cols)
    print(f"top3 items by category 에서 weekly_iapcategory_rev_cols : ", weekly_iapcategory_rev_cols)

    RUN_ID = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    LABELS = {"datascience_division_service": 'gameinsight_framework',
            "run_id": RUN_ID,
            "datascience_division_service_sub" : service_sub}
    

    print(f"top3 items by category 에서 타입 : ", type(service_sub))
    CategoryListUp_SQL, case_when_str, _, _ = iapcategory_rev_df_gemini(service_sub=str(service_sub), 
                                                                        genai_client=genai_client, 
                                                                        MODEL_NAME=MODEL_NAME, 
                                                                        SYSTEM_INSTRUCTION=SYSTEM_INSTRUCTION, 
                                                                        path_weekly_iapcategory_rev=path_weekly_iapcategory_rev,
                                                                        bucket=bucket, 
                                                                        PROJECT_ID=PROJECT_ID, 
                                                                        LOCATION=LOCATION,
                                                                        **context)
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
    from `data-science-division-216308.GW.Sales_iap_hub`
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

    query_result=query_run_method(service_sub, bigquery_client, query)
    query_result['매출'] = query_result['매출'].map(lambda x: f"{int(x)}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    gcs_path = f"{gameidx}/{timestamp}.parquet"
        
    saved_path = save_df_to_gcs(query_result, bucket, gcs_path)

    return saved_path



def top3_items_by_category_gemini(service_sub: str, genai_client, MODEL_NAME, SYSTEM_INSTRUCTION:list, 
                                  path_top3_items_by_category, path_weekly_iapcategory_rev, path_iapgemruby_history,
                                  bucket, PROJECT_ID, LOCATION, **context):

    from google.genai import Client
    genai_client = Client(vertexai=True,project=PROJECT_ID,location=LOCATION)
    query_result4_salesByPackage_ListedCategory = load_df_from_gcs(bucket, path_top3_items_by_category)

    RUN_ID = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    LABELS = {"datascience_division_service": 'gameinsight_framework',
            "run_id": RUN_ID,
            "datascience_division_service_sub" : service_sub}

    _, _, response4_salesByCategory = iapcategory_rev_df_gemini(service_sub, 
                                                                        genai_client, 
                                                                        MODEL_NAME, 
                                                                        SYSTEM_INSTRUCTION, 
                                                                        path_weekly_iapcategory_rev,
                                                                        bucket, 
                                                                        PROJECT_ID, 
                                                                        LOCATION,
                                                                        **context)

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

    iap_gem_ruby_history = load_df_from_gcs(bucket, path_iapgemruby_history)

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


def rgroup_top3_pu(joyplegameid:int, gameidx:str, databaseschema:str, bigquery_client, bucket, **context):
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

    query_result = query_run_method('4_detail_sales', bigquery_client, query)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    gcs_path = f"{gameidx}/{timestamp}.parquet"
        
    saved_path = save_df_to_gcs(query_result, bucket, gcs_path)

    return saved_path


def rgroup_top3_rev(joyplegameid:int, gameidx:str, databaseschema:str, bigquery_client, bucket, **context):
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

    query_result = query_run_method('4_detail_sales', bigquery_client, query)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    gcs_path = f"{gameidx}/{timestamp}.parquet"
        
    saved_path = save_df_to_gcs(query_result, bucket, gcs_path)

    return saved_path


def rgroup_top3_gemini(service_sub: str, genai_client, MODEL_NAME, SYSTEM_INSTRUCTION:list, path_rgroup_top3_rev, path_rgroup_top3_pu, bucket, PROJECT_ID, LOCATION, **context):

    from google.genai import Client
    genai_client = Client(vertexai=True,project=PROJECT_ID,location=LOCATION)

    query_result4_thisWeekSalesTop3 = load_df_from_gcs(bucket, path_rgroup_top3_rev)
    query_result4_thisWeekPUTop3 = load_df_from_gcs(bucket, path_rgroup_top3_pu)

    RUN_ID = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    LABELS = {"datascience_division_service": 'gameinsight_framework',
            "run_id": RUN_ID,
            "datascience_division_service_sub" : service_sub}


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



def category_for_bigquery_sql(service_sub:str, path_weekly_iapcategory_rev:str, 
                              genai_client, MODEL_NAME, SYSTEM_INSTRUCTION, bucket, **context):

    _, _, _, response4_CategoryListUp = iapcategory_rev_df_gemini(service_sub, 
                                                                        genai_client, 
                                                                        MODEL_NAME, 
                                                                        SYSTEM_INSTRUCTION, 
                                                                        path_weekly_iapcategory_rev,
                                                                        bucket, 
                                                                        PROJECT_ID, 
                                                                        LOCATION,
                                                                        **context)

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


def top3_items_rev(joyplegameid:int, gameidx:str, databaseschema:str, service_sub:str, 
                   path_weekly_iapcategory_rev:str, genai_client, MODEL_NAME, SYSTEM_INSTRUCTION:list,
                   bigquery_client, bucket, **context):
    
    CategoryListUp_SQL, case_when_str, _ = category_for_bigquery_sql(service_sub=service_sub,
                                                                    path_weekly_iapcategory_rev=path_weekly_iapcategory_rev,
                                                                    genai_client=genai_client,
                                                                    MODEL_NAME=MODEL_NAME,
                                                                    SYSTEM_INSTRUCTION=SYSTEM_INSTRUCTION,
                                                                    bucket=bucket,
                                                                    **context)

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
    query_result = query_run_method('4_detail_sales', bigquery_client, query)
    query_result['매출'] = query_result['매출'].map(lambda x: f"{int(x)}")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    gcs_path = f"{gameidx}/{timestamp}.parquet"
        
    saved_path = save_df_to_gcs(query_result, bucket, gcs_path)

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

    return dfs, saved_path


def rgroup_rev_draw(gameidx: str, gcs_path:str, bucket, **context):
    ## 해당 데이터프레임에는 매출, PU 둘다 있어서, 매출까지만 필터링
    query_result4_RgroupSales = load_df_from_gcs(bucket, gcs_path)

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


def rgroup_pu_draw(gameidx: str, path_rgroup_pu_rev:str, bucket, **context):
    
    query_result4_RgroupSales =load_df_from_gcs(bucket, path_rgroup_pu_rev)

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


def merge_rgroup_graph(gameidx: str, path_group_rev_pu:str, bucket, **context):
    p1 = rgroup_rev_draw(gameidx, path_group_rev_pu, bucket)
    p2 = rgroup_pu_draw(gameidx, path_group_rev_pu, bucket)

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



def iap_gem_ruby_graph_draw(gameidx: str, path_iap_gem_ruby:str, bucket, **context):
    
    query_result4_salesByPackage = load_df_from_gcs(bucket, path_iap_gem_ruby)
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



def iap_gem_ruby_IAP_graph_draw(gameidx: str, path_iap_df:str, bucket, **context):
    
    query_result4_salesByPackage_IAP = load_df_from_gcs(bucket, path_iap_df)

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


def iap_gem_ruby_GEM_graph_draw(gameidx: str, path_gem_df:str, bucket, **context):

    query_result4_salesByPackage_GEM = load_df_from_gcs(bucket, path_gem_df)
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
    


def iap_gem_ruby_RUBY_graph_draw(gameidx: str, path_ruby_df:str, bucket, **context):

    query_result4_salesByPackage_RUBY = load_df_from_gcs(bucket, path_ruby_df)
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
def top1_graph_draw(joyplegameid: int, gameidx: str, databaseschema: str, service_sub: str, bigquery_client, bucket, **context):

    dfs, _ = top3_items_rev(joyplegameid, gameidx, databaseschema, service_sub, bigquery_client, bucket, **context)

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
def top2_graph_draw(joyplegameid: int, gameidx: str, databaseschema: str, service_sub: str, bigquery_client, bucket, **context):

    dfs, _ = top3_items_rev(joyplegameid, gameidx, databaseschema, service_sub, bigquery_client, bucket, **context)

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
def top3_graph_draw(joyplegameid: int, gameidx: str, databaseschema: str, service_sub: str, bigquery_client, bucket, **context):

    dfs, _ = top3_items_rev(joyplegameid, gameidx, databaseschema, service_sub, bigquery_client, bucket, **context)

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


def rgroup_pu_top3_graph_draw(gameidx: str, path_rgroup_top3_pu:str, bucket, **context):
    
    # rgroup_top3_pu
    query_result4_thisWeekPUTop3 =load_df_from_gcs(bucket, path_rgroup_top3_pu)

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


def rgroup_rev_top3_graph_draw(gameidx: str, path_rgroup_top3_rev:str, bucket, **context):

    query_result4_thisWeekRevTop3 = load_df_from_gcs(bucket, path_rgroup_top3_rev)

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


def rgroup_rev_upload_notion(gameidx: str, path_rev_group_rev_pu, rev_group_rev_pu_path, service_sub, genai_client, MODEL_NAME, SYSTEM_INSTRUCTION, notion, bucket, headers_json, **context):

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
    headers_json = headers_json
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

        # ✅ 로컬 파일 대신 BytesIO 사용
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


    query_result4_RgroupSales = load_df_from_gcs(bucket, path_rev_group_rev_pu)

    resp = df_to_notion_table_under_toggle(
        notion=notion,
        page_id=PAGE_INFO['id'],
        df=query_result4_RgroupSales,
        toggle_title="📊 로데이터 - R그룹 ",
        max_first_batch_rows=90,
        batch_size=100,
    )

    ########### (3) 제미나이 해석

    blocks = md_to_notion_blocks(rev_group_rev_pu_gemini(service_sub,
                                                         genai_client,
                                                         MODEL_NAME,
                                                         SYSTEM_INSTRUCTION,
                                                         rev_group_rev_pu_path,
                                                         bucket,
                                                         PROJECT_ID,
                                                         LOCATION,
                                                         **context
                                                        )
                                                    )

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



def iap_gem_ruby_upload_notion(gameidx: str, joyplegameid: int, databaseschema: str,
                               path_iapgemruby, path_iapgemruby_history, 
                               path_top3_items_by_category,path_weekly_iapcategory_rev,
                               service_sub, genai_client, MODEL_NAME, SYSTEM_INSTRUCTION, bigquery_client, notion, bucket, headers_json, **context):

    PAGE_INFO=context['task_instance'].xcom_pull(
        task_ids = 'make_gameframework_notion_page',
        key='page_info'
    )

    try:
        gcs_path = iap_gem_ruby_graph_draw(gameidx, path_iap_gem_ruby=path_iapgemruby, bucket=bucket, **context)
        blob = bucket.blob(gcs_path)
        image_bytes = blob.download_as_bytes()
        filename = 'graph4_salesByPackage.png'
        print(f"✓ GCS 이미지 다운로드 성공 : {gcs_path}")
    except Exception as e:
        print(f"❌ GCS 다운로드 실패: {str(e)}")
        raise


    # 공통 헤더
    headers_json = headers_json
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
            "Authorization": headers_json.get("Authorization"),
            "Notion-Version": headers_json.get("Notion-Version")
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

    query_result4_salesByPackage = load_df_from_gcs(bucket, path_iapgemruby)

    resp = df_to_notion_table_under_toggle(
        notion=notion,
        page_id=PAGE_INFO['id'],
        df=query_result4_salesByPackage,
        toggle_title="📊 로데이터 - 상품군별 매출 ",
        max_first_batch_rows=90,
        batch_size=100,
        )
    
    blocks = md_to_notion_blocks(iap_gem_ruby_gemini(
        service_sub=service_sub,
        genai_client=genai_client,
        MODEL_NAME=MODEL_NAME,
        SYSTEM_INSTRUCTION=SYSTEM_INSTRUCTION,
        path_iapgemruby=path_iapgemruby,
        path_iapgemruby_history=path_iapgemruby_history,
        bucket=bucket,
        PROJECT_ID=PROJECT_ID,
        LOCATION=LOCATION))

    notion.blocks.children.append(
        block_id=PAGE_INFO['id'],
        children=blocks
    )

    # 프롬프트 결과 중간에 그래프 삽입을 위한 결과 텍스트 5분할

    text = top3_items_by_category_gemini(
        service_sub=service_sub,
        genai_client=genai_client,
        MODEL_NAME=MODEL_NAME,
        SYSTEM_INSTRUCTION=SYSTEM_INSTRUCTION,
        path_top3_items_by_category=path_top3_items_by_category,
        path_weekly_iapcategory_rev=path_weekly_iapcategory_rev,
        bucket=bucket,
        PROJECT_ID=PROJECT_ID,
        LOCATION=LOCATION,
        **context)

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
        gcs_path = top1_graph_draw(joyplegameid, gameidx, databaseschema, service_sub, bigquery_client, bucket, **context)
        blob = bucket.blob(gcs_path)
        image_bytes = blob.download_as_bytes()
        filename = 'graph4_salesByPackage_Category1.png'
        print(f"✓ GCS 이미지 다운로드 성공 : {gcs_path}")
    except Exception as e:
        print(f"❌ GCS 다운로드 실패: {str(e)}")
        raise

    # 공통 헤더
    headers_json = headers_json

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
            "Authorization": headers_json.get("Authorization"),
            "Notion-Version": headers_json.get("Notion-Version")
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
        gcs_path = top2_graph_draw(joyplegameid, gameidx, databaseschema, service_sub, bigquery_client, bucket, **context)
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
            "Authorization": headers_json.get("Authorization"),
            "Notion-Version": headers_json.get("Notion-Version")
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
        gcs_path = top3_graph_draw(joyplegameid, gameidx, databaseschema, service_sub, bigquery_client, bucket, **context)
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
            "Authorization": headers_json.get("Authorization"),
            "Notion-Version": headers_json.get("Notion-Version")
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



def iap_toggle_add(gameidx: str, service_sub:str, MODEL_NAME:str, SYSTEM_INSTRUCTION:list, 
                   path_iap_df:str, path_iapgemruby_history:str, PROJECT_ID: str, LOCATION:str, bucket, notion, **context):
    
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
    headers_json = headers_json

    try:
        gcs_path = iap_gem_ruby_IAP_graph_draw(gameidx, path_iap_df, bucket, **context)
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
            "Authorization": headers_json.get("Authorization"),
            "Notion-Version": headers_json.get("Notion-Version")
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

    from google.genai import Client
    genai_client = Client(vertexai=True,project=PROJECT_ID,location=LOCATION)

    blocks = md_to_notion_blocks(iap_df_gemini(service_sub=service_sub,
                                               genai_client=genai_client,
                                               MODEL_NAME=MODEL_NAME,
                                               SYSTEM_INSTRUCTION=SYSTEM_INSTRUCTION,
                                               path_iap_df=path_iap_df,
                                               path_iapgemruby_history=path_iapgemruby_history,
                                               bucket=bucket,
                                               PROJECT_ID=PROJECT_ID,
                                               LOCATION=LOCATION,
                                               **context))

    notion.blocks.children.append(
        block_id=toggle_id,
        children=blocks
    )

    return True



def gem_toggle_add(gameidx: str, service_sub:str, MODEL_NAME:str, SYSTEM_INSTRUCTION:list, 
                   path_gem_df:str, path_iapgemruby_history:str, PROJECT_ID: str, LOCATION:str, bucket, notion, **context):
    
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
    headers_json = headers_json
    try:
        gcs_path = iap_gem_ruby_GEM_graph_draw(gameidx, path_gem_df, bucket, **context)
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
            "Authorization": headers_json.get("Authorization"),
            "Notion-Version": headers_json.get("Notion-Version")
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

###########
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

    from google.genai import Client
    genai_client = Client(vertexai=True,project=PROJECT_ID,location=LOCATION)

    blocks = md_to_notion_blocks(gem_df_gemini(service_sub=service_sub,
                                               genai_client=genai_client,
                                               MODEL_NAME=MODEL_NAME,
                                               SYSTEM_INSTRUCTION=SYSTEM_INSTRUCTION,
                                               path_gem_df=path_gem_df,
                                               path_iapgemruby_history=path_iapgemruby_history,
                                               bucket=bucket,
                                               PROJECT_ID=PROJECT_ID,
                                               LOCATION=LOCATION,
                                               **context))

    notion.blocks.children.append(
        block_id=toggle_id,
        children=blocks
    )

    return True


def ruby_toggle_add(gameidx: str, service_sub:str, MODEL_NAME:str, SYSTEM_INSTRUCTION:list, 
                   path_ruby_df:str, path_iapgemruby_history:str, PROJECT_ID: str, LOCATION:str, bucket, notion, **context):

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
    headers_json = headers_json

    try:
        gcs_path = iap_gem_ruby_RUBY_graph_draw(gameidx, path_ruby_df, bucket, **context)
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
            "Authorization": headers_json.get("Authorization"),
            "Notion-Version": headers_json.get("Notion-Version")
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

    from google.genai import Client
    genai_client = Client(vertexai=True,project=PROJECT_ID,location=LOCATION)

    blocks = md_to_notion_blocks(ruby_df_gemini(service_sub=service_sub,
                                               genai_client=genai_client,
                                               MODEL_NAME=MODEL_NAME,
                                               SYSTEM_INSTRUCTION=SYSTEM_INSTRUCTION,
                                               path_ruby_df=path_ruby_df,
                                               path_iapgemruby_history=path_iapgemruby_history,
                                               bucket=bucket,
                                               PROJECT_ID=PROJECT_ID,
                                               LOCATION=LOCATION,
                                               **context))

    notion.blocks.children.append(
        block_id=toggle_id,
        children=blocks
    )

    return True


def rgroup_top3_upload_notion(gameidx: str, service_sub:str, MODEL_NAME:str, SYSTEM_INSTRUCTION:list, 
                   path_rgroup_top3_pu:str, path_rgroup_top3_rev:str, PROJECT_ID: str, LOCATION:str, bucket, notion, **context):

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
    headers_json = headers_json
    try:
        gcs_path = rgroup_pu_top3_graph_draw(gameidx, path_rgroup_top3_pu, bucket, **context)
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
            "Authorization": headers_json.get("Authorization"),
            "Notion-Version": headers_json.get("Notion-Version")
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
            "Authorization": headers_json.get("Authorization"),
            "Notion-Version": headers_json.get("Notion-Version")
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

    from google.genai import Client
    genai_client = Client(vertexai=True,project=PROJECT_ID,location=LOCATION)

    blocks = md_to_notion_blocks(rgroup_top3_gemini(service_sub=service_sub,
                                               genai_client=genai_client,
                                               MODEL_NAME=MODEL_NAME,
                                               SYSTEM_INSTRUCTION=SYSTEM_INSTRUCTION,
                                               path_rgroup_top3_pu=path_rgroup_top3_pu,
                                               path_rgroup_top3_rev=path_rgroup_top3_rev,
                                               bucket=bucket,
                                               PROJECT_ID=PROJECT_ID,
                                               LOCATION=LOCATION,
                                               **context))


    notion.blocks.children.append(
        block_id=PAGE_INFO['id'],
        children=blocks
    )

