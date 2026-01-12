# 폰트 캐시 재구축
import matplotlib.font_manager as fm

# 라이브러리 import
import os
import re
import json
import time
import hashlib
import math
import random
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path

# 데이터 처리 및 시각화
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib as mpl
from matplotlib.ticker import PercentFormatter
import dataframe_image as dfi
from PIL import Image

# Google Cloud 관련
from google.cloud import bigquery
from google.cloud import aiplatform
import pandas_gbq

# Gemini AI 관련
import vertexai
from vertexai.generative_models import GenerativeModel
import google.generativeai as genai
from google.generativeai import GenerativeModel as GeminiModel # 이름 충돌 방지를 위해 별칭 사용
from google.oauth2 import service_account

# Notion API
from notion_client import Client

# 웹 관련 (HTML, CSS 렌더링 등)
import nest_asyncio
from jinja2 import Template
from playwright.async_api import async_playwright
import asyncio

# IPython 디스플레이
from IPython.display import display

#Gemini 3.0 관련
# !pip install --upgrade google-genai
from google.genai import Client as GeminiClient
from google.genai.types import GenerateContentConfig
from google.genai import types

# Airflow 관련
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python import PythonOperator


# 날짜 관련
from datetime import datetime, timezone, timedelta


def get_var(key: str, default: str = None) -> str:
    """환경 변수 또는 Airflow Variable 조회"""
    return os.environ.get(key) or Variable.get(key, default_var=default)

# gemini 설정
os.environ['GOOGLE_CLOUD_PROJECT'] = 'data-science-division-216308'
os.environ['GOOGLE_CLOUD_LOCATION'] = 'us-central1'  #global


RUN_ID = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
LABELS = {"datascience_division_service": "daily_mkt_mailing",
          "run_id": RUN_ID,
          "datascience_division_service_sub" : "mkt_daily_mailing_total"} ## 딕셔너리 형태로 붙일 수 있음.
print("RUN_ID=", RUN_ID, "LABEL_ID=", LABELS)

## 변경 코드
## 1> cohort별 전체 지표
client = bigquery.Client()
query = """WITH ua_perfo AS (

-- ============================================================
-- 1. UA 퍼포먼스 원천 (최근 14일)
-- ============================================================
      SELECT
        a.JoypleGameID,
        a.RegdateAuthAccountDateKST,
        a.APPID,
        a.MediaSource,
        a.Campaign,
        b.UptdtCampaign,

        -- gcat 정리
        CASE
            WHEN a.MediaSource IN ('Unknown') THEN 'Unknown'
            WHEN a.campaign LIKE '%Pirates of the Caribbean Android AU%' THEN 'UA'
            WHEN a.campaign LIKE '%Pirates of the Caribbean Android KR%' THEN 'UA'
            WHEN a.campaign LIKE '%Pirates of the Caribbean Android US%' THEN 'UA'
            WHEN a.campaign LIKE '%Pirates of the Caribbean Android GB%' THEN 'UA'
            WHEN a.campaign = 'POTC_検索' THEN 'UA'
            WHEN b.gcat IS NULL AND a.JoypleGameID = 131 THEN d.gcat
            ELSE b.gcat
        END AS gcat,

        a.CountryCode,
        a.MarketName,
        a.OS,
        a.AdsetName,
        a.AdName,

        a.TrackerInstallCount,
        a.RU,

        a.rev_d0,
        a.rev_d1,
        a.rev_d3,
        a.rev_d7,
        a.rev_dcum,

        a.ru_d1,
        a.ru_d3,
        a.ru_d7,

        -- Media Category
        CASE
            WHEN a.campaign LIKE '%Pirates of the Caribbean Android AU%' THEN 'ADNW'
            WHEN a.campaign LIKE '%Pirates of the Caribbean Android KR%' THEN 'ADNW'
            WHEN a.campaign LIKE '%Pirates of the Caribbean Android US%' THEN 'ADNW'
            WHEN a.campaign LIKE '%Pirates of the Caribbean Android GB%' THEN 'ADNW'
            WHEN a.campaign = 'POTC_検索' THEN 'ADNW'
            WHEN b.gcat IS NULL AND a.JoypleGameID = 131 THEN d.media_category
            ELSE b.mediacategory
        END AS mediacategory,

        b.productcategory,
        b.media,
        b.mediadetail,

        -- Optim
        CASE
            WHEN b.optim = 'NONE' AND a.AdsetName LIKE '%MAIA%' THEN 'MAIA'
            WHEN b.optim = 'NONE' AND a.AdsetName LIKE '%AEO%' THEN 'AEO'
            WHEN b.optim = 'NONE' AND a.AdsetName LIKE '%VO%' THEN 'VO'
            ELSE b.optim
        END AS optim,

        b.etccategory,
        b.OSCAM,
        b.GEOCAM,
        b.class,

        CASE
            WHEN a.MediaSource = 'Unknown' THEN '5.Organic'
            ELSE b.targetgroup
        END AS target_group,

        -- GEO 그룹
        CASE
            WHEN CountryCode = 'KR' THEN '4.KR'
            WHEN CountryCode = 'US' THEN '1.US'
            WHEN CountryCode = 'JP' THEN '2.JP'
            WHEN CountryCode IN ('UK','FR','DE','GB') THEN '3.WEU'
            ELSE '5.ETC'
        END AS geo_user_group,
    FROM `dataplatform-reporting.DataService.T_0420_0000_UAPerformanceRaw_V1` a
    LEFT JOIN `dataplatform-reporting.DataService.V_0261_0000_AFCampaignRule_V` b
        ON a.appID = b.appID
       AND a.MediaSource = b.MediaSource
       AND a.Campaign = b.initCampaign
    LEFT JOIN `data-science-division-216308.POTC.before_mas_campaign` d
        ON a.campaign = d.campaign
    WHERE
        a.JoypleGameID IN (1590,159,131,30001,30003,133)
        AND a.RegdateAuthAccountDateKST >= DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 14 DAY)
        AND a.RegdateAuthAccountDateKST < CURRENT_DATE('Asia/Seoul')
),

-- ============================================================
-- 2. Cost 원천
-- ============================================================
cost_raw AS (
    SELECT
        joyplegameid,
        gameid,
        cmpgndate,
        gcat,
        mediacategory,
        os,
        geo_user_group,
        TargetGroup,
        SUM(costcurrency) AS cost,
        SUM(costcurrencyuptdt) AS cost_exclude_credit
    FROM (
        SELECT
            *,
            CASE
                WHEN CountryCode = 'KR' THEN '1.KR'
                WHEN CountryCode = 'US' THEN '2.US'
                WHEN CountryCode = 'JP' THEN '3.JP'
                WHEN CountryCode IN ('UK','FR','DE','GB') THEN '4.WEU'
                ELSE '5.ETC'
            END AS geo_user_group
        FROM `dataplatform-reporting.DataService.V_0410_0000_CostCampaignRule_V`
        WHERE
            joyplegameid IN (1590,159,131,30001,30003,133)
            AND cmpgndate >= DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 14 DAY)
            AND cmpgndate < CURRENT_DATE('Asia/Seoul')
    )
    GROUP BY
        joyplegameid, gameid, cmpgndate, gcat, mediacategory, os, geo_user_group, TargetGroup
),

-- ============================================================
-- 3. UA + Cost 결합
-- ============================================================
final AS (
    SELECT
        IFNULL(a.joyplegameid, b.joyplegameid) AS joyplegameid,
        IFNULL(a.RegdateAuthAccountDateKST, b.cmpgndate) AS RegdateAuthAccountDateKST,
        IFNULL(a.gcat, b.gcat) AS gcat,
        IFNULL(a.mediacategory, b.mediacategory) AS mediacategory,
        IFNULL(a.osuser, b.os) AS osuser,
        IFNULL(a.geo_user_group, b.geo_user_group) AS geo_user_group,
        IFNULL(a.target_group, b.TargetGroup) AS target_group,

        a.install,
        a.ru,
        a.rev_D0,
        a.rev_D1,
        a.rev_D3,
        a.rev_D7,
        a.rev_dcum,
        a.ru_d1,
        a.ru_d3,
        a.ru_d7,

        b.cost,
        b.cost_exclude_credit,

        DATE_DIFF(
            CURRENT_DATE('Asia/Seoul'),
            CASE
                WHEN a.RegdateAuthAccountDateKST IS NULL THEN b.cmpgndate
                ELSE a.RegdateAuthAccountDateKST
            END,
            DAY
        ) AS daydiff
    FROM (
        SELECT
            joyplegameid,
            RegdateAuthAccountDateKST,
            gcat,
            mediacategory,
            geo_user_group,
            CASE
                WHEN OS = 'android' THEN 'And'
                WHEN OS = 'ios' THEN 'IOS'
                ELSE OS
            END AS osuser,
            target_group,
            SUM(TrackerInstallCount) AS install,
            SUM(ru) AS ru,
            SUM(rev_D0) AS rev_D0,
            SUM(rev_D1) AS rev_D1,
            SUM(rev_D3) AS rev_D3,
            SUM(rev_D7) AS rev_D7,
            SUM(rev_dcum) AS rev_dcum,
            SUM(ru_d1) AS ru_d1,
            SUM(ru_d3) AS ru_d3,
            SUM(ru_d7) AS ru_d7
        FROM ua_perfo
        GROUP BY
            joyplegameid,
            RegdateAuthAccountDateKST,
            gcat,
            mediacategory,
            geo_user_group,
            osuser,
            target_group
    ) a
    FULL JOIN cost_raw b
        ON a.joyplegameid = b.joyplegameid
       AND a.RegdateAuthAccountDateKST = b.cmpgndate
       AND a.gcat = b.gcat
       AND a.mediacategory = b.mediacategory
       AND a.geo_user_group = b.geo_user_group
       AND a.osuser = b.os
       AND a.target_group = b.TargetGroup
),

-- ============================================================
-- 4. lag 컷 적용
-- ============================================================
final2 AS (
    SELECT
        joyplegameid,
        RegdateAuthAccountDateKST AS regdate_joyple_kst,
        gcat,
        target_group,
        mediacategory AS media_category,
        geo_user_group,
        osuser,
        install,
        ru,
        rev_d0,
        CASE WHEN daydiff <= 1 THEN NULL ELSE rev_d1 END AS rev_d1,
        CASE WHEN daydiff <= 3 THEN NULL ELSE rev_d3 END AS rev_d3,
        CASE WHEN daydiff <= 7 THEN NULL ELSE rev_d7 END AS rev_d7,
        rev_dcum,
        CASE WHEN daydiff <= 1 THEN NULL ELSE ru_d1 END AS ru_d1,
        CASE WHEN daydiff <= 3 THEN NULL ELSE ru_d3 END AS ru_d3,
        CASE WHEN daydiff <= 7 THEN NULL ELSE ru_d7 END AS ru_d7,
        cost,
        cost_exclude_credit,
        daydiff
    FROM final
)

-- ============================================================
-- 5. 최종 출력
-- ============================================================
SELECT
    regdate_joyple_kst,
    CASE
        WHEN regdate_joyple_kst BETWEEN DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 7 DAY)
             AND DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY)
            THEN 'current_7d'
        WHEN regdate_joyple_kst BETWEEN DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 14 DAY)
             AND DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 8 DAY)
            THEN 'prev_7d'
    END AS period_7d,

    JoypleGameID,
    CASE
        WHEN JoypleGameID = 131 THEN '1.POTC'
        WHEN JoypleGameID = 133 THEN '2.GBTW'
        WHEN JoypleGameID = 30001 THEN '3.WWMC'
        WHEN JoypleGameID = 30003 THEN '4.DRSG'
        ELSE '5.RESU'
    END AS game_name,

    geo_user_group,
    osuser,
    CASE
        WHEN target_group IN ('1.UA-HVU','2.UA-VU','3.UA-Install','4.UA-Restricted') THEN 'Paid'
        WHEN target_group IN ('5.Organic') THEN 'Organic'
        ELSE 'ETC'
    END AS organic_paid,

    install,
    ru,
    cost_exclude_credit AS cost,
    rev_d0,
    rev_d1,
    rev_d3,
    rev_d7,
    rev_dcum,
    ru_d1,
    ru_d3,
    ru_d7
FROM final2
WHERE
    regdate_joyple_kst >= DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 14 DAY)
    AND regdate_joyple_kst < CURRENT_DATE('Asia/Seoul')

"""

query_result_base = client.query(query, job_config=bigquery.QueryJobConfig(labels=LABELS)).to_dataframe()

df = query_result_base.copy()

def aggregate_kpi(df, group_cols):

    g = df.groupby(group_cols, as_index=False)

    # --------------------
    # 1) 전체 합계 (참고용)
    # --------------------
    agg = g.agg(
        cost=("cost", "sum"),
        install=("install", "sum"),
        ru=("ru", "sum"),

        rev_d0=("rev_d0", "sum"),
        rev_d1=("rev_d1", "sum"),
        rev_d3=("rev_d3", "sum"),
        rev_d7=("rev_d7", "sum"),
        rev_dcum=("rev_dcum", "sum"),

        ru_d1=("ru_d1", "sum"),
        ru_d3=("ru_d3", "sum"),
        ru_d7=("ru_d7", "sum"),
    )

    # --------------------
    # 2) 지표별 Mature Base 계산
    # --------------------
    def mature_base(col):
        return (
            df[df[col].notna()]
            .groupby(group_cols)[["ru", "cost"]]
            .sum()
            .rename(columns={
                "ru": f"ru_{col}_base",
                "cost": f"cost_{col}_base"
            })
        )

    base_d0 = mature_base("rev_d0")
    base_d1 = mature_base("rev_d1")
    base_d3 = mature_base("rev_d3")
  #  base_d7 = mature_base("rev_d7")

    # merge
    for base in [base_d0, base_d1, base_d3]: #, base_d7
        agg = agg.merge(base, on=group_cols, how="left")

    # --------------------
    # 3) 단가 KPI
    # --------------------
    agg["CPI"]  = agg["cost"] / agg["install"]
    agg["CPRU"] = agg["cost"] / agg["ru"]

    # --------------------
    # 4) LTV (RU mature 기준)
    # --------------------
    agg["D0LTV"] = agg["rev_d0"] / agg["ru_rev_d0_base"]
    agg["D1LTV"] = agg["rev_d1"] / agg["ru_rev_d1_base"]
    agg["D3LTV"] = agg["rev_d3"] / agg["ru_rev_d3_base"]
    #agg["D7LTV"] = agg["rev_d7"] / agg["ru_rev_d7_base"]
    agg["DcumLTV"] = agg["rev_dcum"] / agg["ru"]

    # --------------------
    # 5) ROAS (Cost mature 기준) ⭐ 핵심
    # --------------------
    agg["D0ROAS"] = agg["rev_d0"] / agg["cost_rev_d0_base"]
    agg["D1ROAS"] = agg["rev_d1"] / agg["cost_rev_d1_base"]
    agg["D3ROAS"] = agg["rev_d3"] / agg["cost_rev_d3_base"]
    #agg["D7ROAS"] = agg["rev_d7"] / agg["cost_rev_d7_base"]
    agg["DcumROAS"] = agg["rev_dcum"] / agg["cost"]

    # --------------------
    # 6) Retention (RU mature 기준)
    # --------------------
    agg["D1RET"] = agg["ru_d1"] / agg["ru_rev_d1_base"]
    agg["D3RET"] = agg["ru_d3"] / agg["ru_rev_d3_base"]
    #agg["D7RET"] = agg["ru_d7"] / agg["ru_rev_d7_base"]


    return agg

def fmt_int(x):
    return f"{x:,.0f}"

def fmt_pct(x):
    return f"{x:.2f}%"

def fmt_delta(x):
    sign = "+" if x >= 0 else ""
    return f"{sign}{x:.2f}%"

def build_wow_table(
    df,
    index_cols=("game_name",),
    period_col="period_7d",
    current_label="current_7d",
    prev_label="prev_7d",
    metrics=("cost","install","ru","CPI","CPRU","D0LTV","D0ROAS"),
    roas_cols=("D0ROAS",),
):

    # --------------------
    # 1) wide 형태로 변환
    # --------------------
    df_wide = (
        df
        .set_index(list(index_cols) + [period_col])
        .unstack(period_col)
    )

    # --------------------
    # 2) 증감률 계산
    # --------------------
    def pct_change(cur, prev):
        return (cur / prev - 1) * 100

    delta = pd.DataFrame(index=df_wide.index)

    for col in metrics:
        cur = df_wide[(col, current_label)]
        prev = df_wide[(col, prev_label)]
        delta[col] = pct_change(cur, prev)

    # --------------------
    # 3) 3줄 구조 생성
    # --------------------
    rows = []

    for idx in df_wide.index:
        row_cur = {period_col: current_label}
        row_prev = {period_col: prev_label}
        row_delta = {period_col: ""}

        # index 컬럼 채우기
        if isinstance(idx, tuple):
            for k, v in zip(index_cols, idx):
                row_cur[k] = v
                row_prev[k] = v
                row_delta[k] = ""
        else:
            row_cur[index_cols[0]] = idx
            row_prev[index_cols[0]] = idx
            row_delta[index_cols[0]] = ""

        for col in metrics:
            row_cur[col] = df_wide.loc[idx, (col, current_label)]
            row_prev[col] = df_wide.loc[idx, (col, prev_label)]
            row_delta[col] = delta.loc[idx, col]

        rows.extend([row_cur, row_prev, row_delta])

    df_final = pd.DataFrame(rows)

    # --------------------
    # 4) 포맷 적용
    # --------------------
    df_fmt = df_final.copy()

    # delta row 판별: period_7d가 빈 값이면 증감 row
    is_delta_row = df_final["period_7d"].eq("")

    for c in metrics:
        if c in roas_cols:
            # ROAS: current/prev는 % 변환, delta는 그대로
            df_fmt[c] = [
                fmt_delta(v) if d and isinstance(v, float)
                else fmt_pct(v * 100) if isinstance(v, float)
                else v
                for v, d in zip(df_final[c], is_delta_row)
            ]
        else:
            # 일반 수치: current/prev는 숫자, delta는 % 변화
            df_fmt[c] = [
                fmt_delta(v) if d and isinstance(v, float)
                else fmt_int(v) if isinstance(v, float)
                else v
                for v, d in zip(df_final[c], is_delta_row)
            ]

    return df_fmt


def build_group_pivot(
    df,
    index_cols=("game_name",),          # 행 그룹 (1차 그룹)
    pivot_col="geo_user_group",          # 열 그룹 (2차 그룹)
    pivot_order=None,                    # 열 그룹 순서
    metrics=("cost", "ru", "CPRU", "D0ROAS"),
    aggfunc="first",
    output_pivot_first=True,             # True: [그룹 → 지표]
):

    # --------------------
    # 1) pivot
    # --------------------
    df_pivot = (
        df
        .pivot_table(
            index=list(index_cols),
            columns=pivot_col,
            values=list(metrics),
            aggfunc=aggfunc
        )
    )
    df_pivot = df_pivot.swaplevel(0, 1, axis=1)

    # --------------------
    # 2) 컬럼 정렬 (metric → pivot)
    # --------------------
    if pivot_order is not None:
      df_pivot = df_pivot.reindex(
        columns=pd.MultiIndex.from_product(
            [pivot_order, metrics]
        )
      )


    return df_pivot

current = df[df["period_7d"] == "current_7d"]

group_cols = ["game_name"]
df_current = aggregate_kpi(current, group_cols)
col = ['game_name', 'cost', 'install', 'ru','CPI', 'CPRU', 'D1LTV', 'D3LTV', 'DcumLTV', 'D1RET','D3RET', 'D1ROAS', 'D3ROAS', 'DcumROAS']
df_current = df_current[col]
# df_current

# 2) 전주대비 금주 Paid 성과 비교
Paid = df[(df["organic_paid"] == "Paid") & (df["osuser"] == "And")]
group_cols = ["period_7d", "game_name"]
df_paid_period = aggregate_kpi(Paid, group_cols)
df_wow_paid = build_wow_table(
    df=df_paid_period,
    index_cols=("game_name",),
    metrics=('cost', 'install', 'ru','CPI', 'CPRU', 'D0LTV', 'D1LTV', 'D3LTV', 'D1RET','D3RET', 'D0ROAS', 'D1ROAS', 'D3ROAS', ),
    roas_cols=('D1RET','D3RET', 'D0ROAS', 'D1ROAS', 'D3ROAS', 'DcumROAS',)
)
# df_wow_paid

# 3) 국가별 주요 성과 비교

country_order = ("1.US", "2.JP", "3.WEU",  "4.KR", "5.ETC")
group_cols = ["game_name", "geo_user_group"]
df_geo_current = aggregate_kpi(current, group_cols)
df_country = build_group_pivot(
    df=df_geo_current,
    index_cols=("game_name",),
    pivot_col="geo_user_group",
    pivot_order=country_order,
    metrics=("cost", "ru", "CPRU", "D0ROAS")
)
# df_country

# 4) OS별 주요 성과 비교

os_order = ("And", "IOS")
group_cols = ["game_name", "osuser"]
df_os_current = aggregate_kpi(current, group_cols)
df_os = build_group_pivot(
    df=df_os_current,
    index_cols=("game_name",),
    pivot_col="osuser",
    pivot_order= os_order,
    metrics=("cost", "ru", "CPRU", "D0ROAS")
)
# df_os

# OS Cost 비중 계산
df_os[("Total", "And Cost 비중")] = (
    df_os[("And", "cost")]
    / (df_os[("IOS", "cost")] + df_os[("And", "cost")])
)

# % 포맷
df_os[("Total", "And Cost 비중")] = df_os[("Total", "And Cost 비중")].apply(
    lambda x: f"{x*100:.1f}%" if pd.notna(x) else ""
)
# 위치 이동
cols = list(df_os.columns)
meta_col = ("Total", "And Cost 비중")
cols.insert(0, cols.pop(cols.index(meta_col)))
df_os = df_os[cols]
# df_os

# 5) Paid/Organic 별 주요 성과 비교

organic_paid = ("Organic", "Paid")
group_cols = ["game_name", "organic_paid"]
df_organic_paid_current = aggregate_kpi(current, group_cols)
df_organic_paid = build_group_pivot(
    df=df_organic_paid_current,
    index_cols=("game_name",),
    pivot_col="organic_paid",
    pivot_order= organic_paid,
    metrics=("ru","D0LTV", "D1LTV", "D3LTV", 'D1RET','D3RET')
)
# df_organic_paid

# 5) Paid/Organic 별 주요 성과 비교

organic_paid = ("Organic", "Paid")
group_cols = ["game_name", "organic_paid"]
df_organic_paid_current = aggregate_kpi(current, group_cols)
df_organic_paid = build_group_pivot(
    df=df_organic_paid_current,
    index_cols=("game_name",),
    pivot_col="organic_paid",
    pivot_order= organic_paid,
    metrics=("ru","D0LTV", "D1LTV", "D3LTV", 'D1RET','D3RET')
)
# df_organic_paid

# Paid/Organic 별 RU 비중 계산
df_organic_paid[("Total", "RU Organic 비중")] = (
    df_organic_paid[("Organic", "ru")]
    / (df_organic_paid[("Paid", "ru")] + df_organic_paid[("Organic", "ru")])
)

# % 포맷
df_organic_paid[("Total", "RU Organic 비중")] = df_organic_paid[("Total", "RU Organic 비중")].apply(
    lambda x: f"{x*100:.1f}%" if pd.notna(x) else ""
)
# 위치 이동
cols = list(df_organic_paid.columns)
meta_col = ("Total", "RU Organic 비중")
cols.insert(0, cols.pop(cols.index(meta_col)))
df_organic_paid = df_organic_paid[cols]
# df_organic_paid

# AI 가 보기 편한 형태로 컬럼 정리
def df_to_md(df):
    # --------------------
    # 1) 컬럼 헤더 처리
    # --------------------
    if isinstance(df.columns, pd.MultiIndex):
        # MultiIndex 컬럼 (예: Total / Organic / Paid 구조)
        levels = df.columns.nlevels

        header_rows = []
        for lvl in range(levels):
            row = []
            for col in df.columns:
                val = col[lvl]
                row.append("" if val is None else str(val))
            header_rows.append("| " + " | ".join(row) + " |")

        sep = "| " + " | ".join(["---"] * len(df.columns)) + " |"
        header = "\n".join(header_rows + [sep])

    else:
        # 단일 컬럼
        header = "| " + " | ".join(map(str, df.columns)) + " |"
        sep = "| " + " | ".join(["---"] * len(df.columns)) + " |"
        header = "\n".join([header, sep])

    # --------------------
    # 2) 데이터 행
    # --------------------
    rows = "\n".join(
        "| " + " | ".join(map(str, row)) + " |"
        for row in df.values
    )

    return "\n".join([header, rows])

SYSTEM_INSTRUCTION = """
    너는 전문 마케팅 데이터 분석가야.
    주어진 ROAS 데이터와 퍼포먼스팀의 원문 리포트를 **절대 오류 없이 분석**하고, 요청된 **모든 출력 형식 규칙**을 엄격하게 준수하여 리포트를 작성해야해.

    [데이터 정합성 최우선 규칙]
    1. 모든 수치 비교 (cost, install ru, CPI, cpru, 증감률 계산)는 오직 제공된 테이블 데이터만을 기반으로 수행하고 ** 값이 존재하는 것만 언급해 **
    2. 테이블에 없는 데이터나 추론은 엄금하며, 비교 대상 서로 다른 게임이야 각 게임의 성과 차이를 설명해줘.

    [표기법 규칙]
    - cost, install ru, CPI, cpru는 천단위 쉼표(,)를 사용
    - ROAS, RET 관련 지표는 소수점 두번째 자리까지 표기하고 '%' 단위를 사용
    - 증감률을 이야기할 때는 +- 기호 대신 🔺(상승) 또는 🔻(하락) 기호를 숫자앞에 사용해줘

    [출력형식 규칙]
    - 리포트 작성 완료했다는 내용은 별도로 언급하지마
    - 마크다운 포맷: 노션 마크다운 포맷을 사용해
    - 한 문장마다 시작은 # 로 시작해줘.
    - 습니다. 체로 써줘
    - 명확하고 간결하게 작성해줘
    """

df_current_md = df_to_md(df_current)

# 1) 최근 7일 게임 성과
prompt_current_description = f"""
    ## 데이터 설명
    최근 1주일간 게임별 성과를 종합 비교한 데이터야.
    ### [기본 유입 지표]
    - game_name : 게임 이름(1.POTC, 2.GBTW, 3.WWMC, 4.DRSG, 5.RESU)
    - **Cost**: 해당 날짜의 마케팅 집행 비용
    - **Install**: 해당 날짜의 전체 신규 유입 수
    - **RU : 신규 유저수

    ### [단가 지표]
    - **CPI **: Install 1건당 비용
    - **CPRU **: 신규 유저(RU) 1명 데려오는 비용

    ### [LTV 지표]
    - **dnLTV(D0LTV / D1LTV / D3LTV)**: 해당 시점의 LTV

    ### [Retention 지표]
    - **dnRET(D1RET / D3RET)**: 각각 1·3일차 잔존율(리텐션)

    ### [ROAS 지표]
    - **dnROAS(D0ROAS / D1ROAS / D3ROAS)**: 해당 시점의 ROAS

    ---- 통합 게임별 성과 분석
    {df_current_md}
    """
prompt_current_final = [
    prompt_current_description,
    """
    ### 마케팅 분석 리포트 작성 요청

    주어진 데이터를 참고하여 아래 4가지 항목에 대한 게임별 분석 리포트를 작성해줘.

    # 1. 최근 7일간 게임별 종합 성과 분석
      # 각 게임의 RU, Cost, CPRU, D0ROAS, D0LTV를 중심으로 성과를 비교해 설명해줘.
      # 수치는 반드시 테이블에 존재하는 값만 사용하고, 값이 없는 지표는 언급하지마.
      # 성과가 우수한 게임부터 순서대로 나열해줘.

    # 2. 유입 규모 관점 분석
      # RU 기준으로 게임 간 유입 규모 차이를 비교해 설명해줘.
      # 예시처럼 서술해줘:
      # "RU 유입은 RESU > GBTW > … > DRSG 순으로 확인됩니다."

    # 3. 효율 관점 분석
      # CPRU와 D0ROAS를 중심으로 비용 효율이 좋은 게임과 비효율적인 게임을 구분해 설명해줘.
      # ROAS는 % 단위로 비교하고, 비용 대비 성과 차이가 큰 게임 위주로 비교해줘.

    # 4. 표현 규칙
      # 모든 문장은 '#'으로 시작해줘
      # '~습니다.' 체로 작성해줘
      # 불필요한 감정 표현이나 추측은 하지마
      # 데이터 기반 사실만 간결하게 정리해줘
    # 5. 출력 형태
      # 줄글형태로 5줄 미만으로 작성해줘
      # 설명 시 반드시 게임명을 언급해줘
      # # Paid 전체 게임별 종합 성과 분석 등 제목을 넣지 말고 작성해줘

      """]

genai_client = GeminiClient(
    vertexai=True,
    location="global"      # genai 호출용location 변경
)

config_current_optimized = GenerateContentConfig(
    temperature=1.0,
    thinking_config=types.ThinkingConfig(thinking_level="high"),
    system_instruction= SYSTEM_INSTRUCTION,
    labels=LABELS
)

response_current = genai_client.models.generate_content(
    model="gemini-3-pro-preview",   # API 호출
    contents = prompt_current_final
    ,config=config_current_optimized
)

df_organic_paid_md = df_to_md(df_organic_paid)

prompt_organic_paid_description = f"""
    ## 데이터 설명
    최근 1주일간 게임별 성과를 종합 비교한 데이터야.
    ### [기본 유입 지표]
    - game_name : 게임 이름(1.POTC, 2.GBTW, 3.WWMC, 4.DRSG, 5.RESU)
    - **RU Organic 비중**: 해당 날짜의 Organic RU 비중
    - **RU : 신규 유저수

    ### [단가 지표]
    - **CPI **: Install 1건당 비용
    - **CPRU **: 신규 유저(RU) 1명 데려오는 비용

    ### [LTV 지표]
    - **dnLTV(D0LTV / D1LTV / D3LTV)**: 해당 시점의 LTV

    ### [Retention 지표]
    - **dnRET(D1RET / D3RET)**: 각각 1·3일차 잔존율(리텐션)

    ---- Organic / Paid 유입 및 매출 성과
    {df_organic_paid_md}
    """
prompt_organic_paid_final = [
    prompt_organic_paid_description,
    """
    ### 마케팅 분석 리포트 작성 요청

    아래 테이블은 게임별 Organic / Paid 유입 및 매출 성과를 비교한 데이터야

    # 게임별 Organic / Paid 성과 분석
      # 각 게임의 RU Organic 비중이 가장 큰 게임과 낮은 게에 대해서 비교 언급해줘.
      # 게임별 LTV, RET 기준 Organic / Paid 성과 차이 비교해줘. Organic의 성과가 더 우수한 게임과 Paid의 성과가 더 우수한 게임을 비교 언급해줘
      # 특정 게임에서 LTV, ROAS 차이가 크다면 명확히 지적해
    # 출력형태
      # 줄글형태로 5줄 미만으로 작성해줘
      # 설명 시 반드시 게임명을 언급해줘
      # #게임별 Organic / Paid 성과 분석 등 제목을 넣지 말고 작성해줘

      """]

config_organic_paid_optimized = GenerateContentConfig(
    temperature=1.0,
    thinking_config=types.ThinkingConfig(thinking_level="high"),
    system_instruction= SYSTEM_INSTRUCTION,
    labels=LABELS
)

response_organic_paid = genai_client.models.generate_content(
    model="gemini-3-pro-preview",   # Vertex AI 모델명
    contents = prompt_organic_paid_final
    ,config=config_organic_paid_optimized
)

df_wow_paid_md = df_to_md(df_wow_paid)
#3) 전주대비 금주 Paid 성과 비교
prompt_wow_paid_description = f"""
    ## 데이터 설명
    최근 1주일간 게임별 성과를 전주 대비 금주로 종합 비교한 데이터야.

    ### [기본 유입 지표]
    - **Period_7d**: 전주와 금주를 구별하는 컬럼
    - **Cost**: 해당 날짜의 마케팅 집행 비용
    - **Install**: 해당 날짜의 전체 신규 유입 수
    - **RU : 신규 유저수

    ### [단가 지표]
    - **CPI **: Install 1건당 비용
    - **CPRU **: 신규 유저(RU) 1명 데려오는 비용

    ### [LTV 지표]
    - **dnLTV(D0LTV / D1LTV / D3LTV)**: 해당 시점의 LTV

    ### [Retention 지표]
    - **dnRET(D1RET / D3RET)**: 각각 1·3일차 잔존율(리텐션)

    ### [ROAS 지표]
    - **dnROAS(D0ROAS / D1ROAS / D3ROAS)**: 해당 시점의 ROAS

    ---- Paid 게임별 성과 트렌드 분석
    {df_wow_paid_md}
    """
prompt_wow_paid_final = [
    prompt_wow_paid_description,
    """
    ### 마케팅 분석 리포트 작성 요청

    아래 테이블은 **Android Paid 유저만을 기준**으로, 최근 7일(current_7d)과 전주 7일(prev_7d)의 게임별 성과를 비교한 데이터야
    각 게임은 current_7d / prev_7d / 증감률(%) 순서로 정리되어 있어
    Paid 성과 트렌드 비교가 목적이야.

    # Paid 전체 게임별 종합 성과 분석
      # 각 게임의 RU, Cost, CPRU, D0ROAS, D0LTV의 전주 대비 변화를 간략하게 언급해줘.
      # Cost가 증가한 게임과 감소한 게임을 명확히 구분하여 설명해
      # 전주 대비 CPRU가 개선된 게임과 악화된 게임을 구분하여 설명해줘. 특히 RU 증가에도 CPRU가 개선된 케이스가 있다면 반드시 언급해줘
      # 특정 게임에서 LTV, ROAS 하락 폭이 크다면 명확히 지적해
    # 출력형태
      # 줄글형태로 5줄 미만으로 작성해줘
      # 설명 시 반드시 게임명을 언급해줘
      # # Paid 전체 게임별 종합 성과 분석 등 제목을 넣지 말고 작성해줘

      """]

config_wow_paid_optimized = GenerateContentConfig(
    temperature=1.0,
    thinking_config=types.ThinkingConfig(thinking_level="high"),
    system_instruction= SYSTEM_INSTRUCTION,
    labels=LABELS
)

response_wow_paid = genai_client.models.generate_content(
    model="gemini-3-pro-preview",   # Vertex AI 모델명
    contents = prompt_wow_paid_final
    ,config=config_wow_paid_optimized
)

df_country_md = df_to_md(df_country)
#4) 국가별 주요 성과 비교
prompt_country_description = f"""
    ## 데이터 설명
    최근 1주일간 게임별 성과를 종합 비교한 데이터야.
    ### [기본 유입 지표]
    - game_name : 게임 이름(1.POTC, 2.GBTW, 3.WWMC, 4.DRSG, 5.RESU)
    - **RU : 신규 유저수

    ### [단가 지표]
    - **CPI **: Install 1건당 비용
    - **CPRU **: 신규 유저(RU) 1명 데려오는 비용

    ### [ROAS 지표]
    - **D0ROAS**: D0 시점의 ROAS

    ---- 국가별 유입 및 매출 성과
    {df_country_md}
    """
prompt_country_final = [
    prompt_country_description,
    """
    ### 마케팅 분석 리포트 작성 요청

    아래 테이블은 게임별 국가별 유입 및 매출 성과를 비교한 데이터야

    # 게임별 국가별 성과 분석
      # 게임별로 RU 숫자가 가장 큰 국가를 언급해줘
      # 게임별 LTV, RET 기준 국가별 성과 차이 비교해줘. 게임별로 성과가 가장 우수한 국가와 저조한 국가를 언급해줘.
      # 특정 게임에서 특정국가의 COST는 높은데, 타국가 대비 D0ROAS가 저조하다면 명확히 지적해
    # 출력형태
      # 줄글형태로 5줄 미만으로 작성해줘
      # 설명 시 반드시 게임명을 언급해줘
      # # Paid 전체 게임별 종합 성과 분석 등 제목을 넣지 말고 작성해줘

      """]

config_country_optimized = GenerateContentConfig(
    temperature=1.0,
    thinking_config=types.ThinkingConfig(thinking_level="high"),
    system_instruction= SYSTEM_INSTRUCTION,
    labels=LABELS
)

response_country = genai_client.models.generate_content(
    model="gemini-3-pro-preview",   # Vertex AI 모델명
    contents = prompt_country_final
    ,config=config_country_optimized
)

df_os_md = df_to_md(df_os)
#5) OS 별 주요 성과 비
prompt_os_description = f"""
    ## 데이터 설명
    최근 1주일간 게임별 성과를 종합 비교한 데이터야.

    ### [기본 유입 지표]
    - game_name : 게임 이름(1.POTC, 2.GBTW, 3.WWMC, 4.DRSG, 5.RESU)
    - **And Cost 비중 : And Cost 비중
    - **RU : 신규 유저수


    ### [단가 지표]
    - **CPI **: Install 1건당 비용
    - **CPRU **: 신규 유저(RU) 1명 데려오는 비용

    ### [ROAS 지표]
    - **D0ROAS**: D0 시점의 ROAS

    ---- OS별 유입 및 매출 성과
    {df_os_md}
    """
prompt_os_final = [
    prompt_os_description,
    """
    ### 마케팅 분석 리포트 작성 요청

    아래 테이블은 게임별 OS별 유입 및 매출 성과를 비교한 데이터야

    # 게임별 OS별 성과 분석
      # 각 게임의 And Cost 비중을 확인하여, 가장 높은 게임과 낮은 게임을 언급해줘
      # 게임별 LTV, RET 기준 OS별 성과 차이 비교해줘. 게임별로 성과가 가장 우수한 OS와 저조한 OS를 언급해줘.
      # 특정 게임에서 특정 OS의 COST는 높은데, 타OS 대비 D0ROAS가 저조하다면 명확히 지적해
    # 출력형태
      # 줄글형태로 5줄 미만으로 작성해줘
      # 설명 시 반드시 game_name을 언급해줘
      # # Paid 전체 게임별 종합 성과 분석 등 제목을 넣지 말고 작성해줘

      """]

config_os_optimized = GenerateContentConfig(
    temperature=1.0,
    thinking_config=types.ThinkingConfig(thinking_level="high"),
    system_instruction= SYSTEM_INSTRUCTION,
    labels=LABELS
)

response5 = genai_client.models.generate_content(
    model="gemini-3-pro-preview",   # Vertex AI 모델명
    contents = prompt_os_final
    ,config=config_os_optimized
)