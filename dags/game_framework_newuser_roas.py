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
from airflow.sdk import get_current_context
from game_framework_util import *

PROJECT_ID = "data-science-division-216308"
LOCATION = "us-central1"

## 신규 유저 회수 현황
## 6_newuser_roas

def result6_monthlyROAS(joyplegameid:int, gameidx:str, bigquery_client, bucket, **context):
    
    query= f"""
    WITH revraw AS(
    select JoypleGameID, Month
    , concat("D_",date_diff(date_sub(current_date('Asia/Seoul'), interval 1 day), date_sub(cast(concat(Month, '-01') as date), interval 1 day), day)) as matured_daydiff
    ,sum(RU) as RU,
    sum(Sales_D1) as sales_D1,
    sum(Sales_D3) as sales_D3,
    sum(Sales_D7) as sales_D7,
    CASE WHEN COUNTIF(sales_D14 IS NULL) >= 1 THEN null ELSE sum(Sales_D14) END as Sales_D14,
    CASE WHEN COUNTIF(Sales_D30 IS NULL) >= 1 THEN null ELSE sum(Sales_D30) END as Sales_D30,
    CASE WHEN COUNTIF(Sales_D60 IS NULL) >= 1 THEN null ELSE sum(Sales_D60) END as Sales_D60,
    CASE WHEN COUNTIF(Sales_D90 IS NULL) >= 1 THEN null ELSE sum(Sales_D90) END as Sales_D90,
    CASE WHEN COUNTIF(Sales_D120 IS NULL) >= 1 THEN null ELSE sum(Sales_D120) END as Sales_D120,
    CASE WHEN COUNTIF(Sales_D150 IS NULL) >= 1 THEN null ELSE sum(Sales_D150) END as Sales_D150,
    CASE WHEN COUNTIF(Sales_D180 IS NULL) >= 1 THEN null ELSE sum(Sales_D180) END as Sales_D180,
    CASE WHEN COUNTIF(Sales_D210 IS NULL) >= 1 THEN null ELSE sum(Sales_D210) END as Sales_D210,
    CASE WHEN COUNTIF(Sales_D240 IS NULL) >= 1 THEN null ELSE sum(Sales_D240) END as Sales_D240,
    CASE WHEN COUNTIF(Sales_D270 IS NULL) >= 1 THEN null ELSE sum(Sales_D270) END as Sales_D270,
    CASE WHEN COUNTIF(Sales_D300 IS NULL) >= 1 THEN null ELSE sum(Sales_D300) END as Sales_D300,
    CASE WHEN COUNTIF(Sales_D330 IS NULL) >= 1 THEN null ELSE sum(Sales_D330) END as Sales_D330,
    CASE WHEN COUNTIF(Sales_D360 IS NULL) >= 1 THEN null ELSE sum(Sales_D360) END as Sales_D360,
    from(
    select JoypleGameID, RegdateAuthAccountDateKST,
    FORMAT_DATE('%Y-%m' ,RegdateAuthAccountDateKST) as Month,
    sum(RU) as RU,
    IFNULL(sum(rev_D1),0) as Sales_D1,
    IFNULL(sum(rev_D3),0) as Sales_D3,
    IFNULL(sum(rev_D7),0) as Sales_D7,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -15 DAY) AS Date)  THEN  IFNULL(sum(rev_D14),0) ELSE  null END as Sales_D14,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -31 DAY) AS Date)  THEN  IFNULL(sum(rev_D30),0) ELSE  null END as Sales_D30,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -61 DAY) AS Date)  THEN  IFNULL(sum(rev_D60),0) ELSE  null END as Sales_D60,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -91 DAY) AS Date)  THEN  IFNULL(sum(rev_D90),0) ELSE  null END as Sales_D90,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -121 DAY) AS Date)  THEN  IFNULL(sum(rev_D120),0) ELSE  null END as Sales_D120,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -151 DAY) AS Date)  THEN  IFNULL(sum(rev_D150),0) ELSE  null END as Sales_D150,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -181 DAY) AS Date)  THEN  IFNULL(sum(rev_D180),0) ELSE  null END as Sales_D180,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -211 DAY) AS Date)  THEN  IFNULL(sum(rev_D210),0) ELSE  null END as Sales_D210,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -241 DAY) AS Date)  THEN  IFNULL(sum(rev_D240),0) ELSE  null END as Sales_D240,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -271 DAY) AS Date)  THEN  IFNULL(sum(rev_D270),0) ELSE  null END as Sales_D270,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -301 DAY) AS Date)  THEN  IFNULL(sum(rev_D300),0) ELSE  null END as Sales_D300,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -331 DAY) AS Date)  THEN  IFNULL(sum(rev_D330),0) ELSE  null END as Sales_D330,
    CASE WHEN RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -361 DAY) AS Date)  THEN  IFNULL(sum(rev_D360),0) ELSE  null END as Sales_D360
    from `dataplatform-reporting.DataService.T_0420_0000_UAPerformanceRaw_V1`
        where JoypleGameID = {joyplegameid}
        and RegdateAuthAccountDateKST >= DATE_SUB(DATE(CONCAT(FORMAT_DATE('%Y-%m', DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY)),'-01')), INTERVAL 24 MONTH)
        and RegdateAuthAccountDateKST <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -8 DAY) AS Date)
    group by JoypleGameID, RegdateAuthAccountDateKST
    ) group by JoypleGameID, month
    )


    , final AS(
    select  JoypleGameID,  Month, matured_daydiff, RU,
    Sales_D1/RU as D1_LTV,
    Sales_D3/RU as D3_LTV,
    Sales_D7/RU as D7_LTV,
    Sales_D14/RU as D14_LTV,
    Sales_D30/RU as D30_LTV,
    Sales_D60/RU as D60_LTV,
    Sales_D90/RU as D90_LTV,
    Sales_D120/RU as D120_LTV,
    Sales_D150/RU as D150_LTV,
    Sales_D180/RU as D180_LTV,
    Sales_D210/RU as D210_LTV,
    Sales_D240/RU as D240_LTV,
    Sales_D270/RU as D270_LTV,
    Sales_D300/RU as D300_LTV,
    Sales_D330/RU as D330_LTV,
    Sales_D360/RU as D360_LTV,
    Sales_D14_p/RU as D14_LTV_p,
    Sales_D30_p/RU as D30_LTV_p,
    Sales_D60_p/RU as D60_LTV_p,
    Sales_D90_p/RU as D90_LTV_p,
    Sales_D120_p/RU as D120_LTV_p,
    Sales_D150_p/RU as D150_LTV_p,
    Sales_D180_p/RU as D180_LTV_p,
    Sales_D210_p/RU as D210_LTV_p,
    Sales_D240_p/RU as D240_LTV_p,
    Sales_D270_p/RU as D270_LTV_p,
    Sales_D300_p/RU as D300_LTV_p,
    Sales_D330_p/RU as D330_LTV_p,
    Sales_D360_p/RU as D360_LTV_p,
    D1D3_avg,  D3D7_avg , Sales_D7/RU as kpi_d7
    from(
    select *,
    CASE WHEN Sales_D14 is not null then null  ELSE  Sales_D7*D7D14_avg END as Sales_D14_p,
    CASE WHEN Sales_D30 is not null then null
    WHEN Sales_D30 is null and Sales_D14 is not null THEN Sales_D14*D14D30_avg
    ELSE Sales_D7*D7D14_avg*D14D30_avg END as Sales_D30_p,
    CASE WHEN Sales_D60 is not null then null
    WHEN Sales_D30 is not null THEN Sales_D30*D30D60_avg
    WHEN Sales_D14 is not null  THEN Sales_D14*D14D30_avg*D30D60_avg
    ELSE Sales_D7*D7D14_avg*D14D30_avg*D30D60_avg END as Sales_D60_p,
    CASE WHEN Sales_D90 is not null then null
    WHEN Sales_D60 is not null THEN Sales_D60*D60D90_avg
    WHEN Sales_D30 is not null THEN Sales_D30*D30D60_avg*D60D90_avg
    WHEN Sales_D14 is not null  THEN Sales_D14*D14D30_avg*D30D60_avg*D60D90_avg
    ELSE Sales_D7*D7D14_avg*D14D30_avg*D30D60_avg*D60D90_avg END as Sales_D90_p,
    CASE WHEN Sales_D120 is not null then null
    WHEN Sales_D90 is not null THEN Sales_D90*D90D120_avg
    WHEN Sales_D60 is not null THEN Sales_D60*D60D90_avg*D90D120_avg
    WHEN Sales_D30 is not null THEN Sales_D30*D30D60_avg*D60D90_avg*D90D120_avg
    WHEN Sales_D14 is not null  THEN Sales_D14*D14D30_avg*D30D60_avg*D60D90_avg*D90D120_avg
    ELSE Sales_D7*D7D14_avg*D14D30_avg*D30D60_avg*D60D90_avg*D90D120_avg  END as Sales_D120_p,
    CASE WHEN Sales_D150 is not null then null
    WHEN Sales_D120 is not null THEN Sales_D120*D120D150_avg
    WHEN Sales_D90 is not null THEN Sales_D90*D90D120_avg*D120D150_avg
    WHEN Sales_D60 is not null THEN Sales_D60*D60D90_avg*D90D120_avg*D120D150_avg
    WHEN Sales_D30 is not null THEN Sales_D30*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg
    WHEN Sales_D14 is not null  THEN Sales_D14*D14D30_avg*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg
    ELSE Sales_D7*D7D14_avg*D14D30_avg*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg   END as Sales_D150_p,
    CASE WHEN Sales_D180 is not null then null
    WHEN Sales_D150 is not null THEN Sales_D150*D150D180_avg
    WHEN Sales_D120 is not null THEN Sales_D120*D120D150_avg*D150D180_avg
    WHEN Sales_D90 is not null THEN Sales_D90*D90D120_avg*D120D150_avg*D150D180_avg
    WHEN Sales_D60 is not null THEN Sales_D60*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg
    WHEN Sales_D30 is not null THEN Sales_D30*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg
    WHEN Sales_D14 is not null  THEN Sales_D14*D14D30_avg*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg
    ELSE Sales_D7*D7D14_avg*D14D30_avg*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg    END as Sales_D180_p,
    CASE WHEN Sales_D210 is not null then null
    WHEN Sales_D180 is not null THEN Sales_D180*D180D210_avg
    WHEN Sales_D150 is not null THEN Sales_D150*D150D180_avg*D180D210_avg
    WHEN Sales_D120 is not null THEN Sales_D120*D120D150_avg*D150D180_avg*D180D210_avg
    WHEN Sales_D90 is not null THEN Sales_D90*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg
    WHEN Sales_D60 is not null THEN Sales_D60*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg
    WHEN Sales_D30 is not null THEN Sales_D30*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg
    WHEN Sales_D14 is not null  THEN Sales_D14*D14D30_avg*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg
    ELSE Sales_D7*D7D14_avg*D14D30_avg*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg     END as Sales_D210_p,
    CASE WHEN Sales_D240 is not null then null
    WHEN Sales_D210 is not null THEN Sales_D210*D210D240_avg
    WHEN Sales_D180 is not null THEN Sales_D180*D180D210_avg*D210D240_avg
    WHEN Sales_D150 is not null THEN Sales_D150*D150D180_avg*D180D210_avg*D210D240_avg
    WHEN Sales_D120 is not null THEN Sales_D120*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg
    WHEN Sales_D90 is not null THEN Sales_D90*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg
    WHEN Sales_D60 is not null THEN Sales_D60*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg
    WHEN Sales_D30 is not null THEN Sales_D30*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg
    WHEN Sales_D14 is not null  THEN Sales_D14*D14D30_avg*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg
    ELSE Sales_D7*D7D14_avg*D14D30_avg*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg      END as Sales_D240_p,
    CASE WHEN Sales_D270 is not null then null
    WHEN Sales_D240 is not null THEN Sales_D240*D240D270_avg
    WHEN Sales_D210 is not null THEN Sales_D210*D210D240_avg*D240D270_avg
    WHEN Sales_D180 is not null THEN Sales_D180*D180D210_avg*D210D240_avg*D240D270_avg
    WHEN Sales_D150 is not null THEN Sales_D150*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg
    WHEN Sales_D120 is not null THEN Sales_D120*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg
    WHEN Sales_D90 is not null THEN Sales_D90*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg
    WHEN Sales_D60 is not null THEN Sales_D60*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg
    WHEN Sales_D30 is not null THEN Sales_D30*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg
    WHEN Sales_D14 is not null  THEN Sales_D14*D14D30_avg*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg
    ELSE Sales_D7*D7D14_avg*D14D30_avg*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg       END as Sales_D270_p,
    CASE WHEN Sales_D300 is not null then null
    WHEN Sales_D270 is not null THEN Sales_D270*D270D300_avg
    WHEN Sales_D240 is not null THEN Sales_D240*D240D270_avg*D270D300_avg
    WHEN Sales_D210 is not null THEN Sales_D210*D210D240_avg*D240D270_avg*D270D300_avg
    WHEN Sales_D180 is not null THEN Sales_D180*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg
    WHEN Sales_D150 is not null THEN Sales_D150*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg
    WHEN Sales_D120 is not null THEN Sales_D120*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg
    WHEN Sales_D90 is not null THEN Sales_D90*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg
    WHEN Sales_D60 is not null THEN Sales_D60*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg
    WHEN Sales_D30 is not null THEN Sales_D30*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg
    WHEN Sales_D14 is not null  THEN Sales_D14*D14D30_avg*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg
    ELSE Sales_D7*D7D14_avg*D14D30_avg*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg        END as Sales_D300_p,
    CASE WHEN Sales_D330 is not null then null
    WHEN Sales_D300 is not null THEN Sales_D300*D300D330_avg
    WHEN Sales_D270 is not null THEN Sales_D270*D270D300_avg*D300D330_avg
    WHEN Sales_D240 is not null THEN Sales_D240*D240D270_avg*D270D300_avg*D300D330_avg
    WHEN Sales_D210 is not null THEN Sales_D210*D210D240_avg*D240D270_avg*D270D300_avg*D300D330_avg
    WHEN Sales_D180 is not null THEN Sales_D180*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg*D300D330_avg
    WHEN Sales_D150 is not null THEN Sales_D150*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg*D300D330_avg
    WHEN Sales_D120 is not null THEN Sales_D120*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg*D300D330_avg
    WHEN Sales_D90 is not null THEN Sales_D90*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg*D300D330_avg
    WHEN Sales_D60 is not null THEN Sales_D60*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg*D300D330_avg
    WHEN Sales_D30 is not null THEN Sales_D30*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg*D300D330_avg
    WHEN Sales_D14 is not null  THEN Sales_D14*D14D30_avg*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg*D300D330_avg
    ELSE Sales_D7*D7D14_avg*D14D30_avg*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg*D300D330_avg         END as Sales_D330_p,
    CASE WHEN Sales_D360 is not null then null
    WHEN Sales_D330 is not null THEN Sales_D330*D330D360_avg
    WHEN Sales_D300 is not null THEN Sales_D300*D300D330_avg *D330D360_avg
    WHEN Sales_D270 is not null THEN Sales_D270*D270D300_avg*D300D330_avg  *D330D360_avg
    WHEN Sales_D240 is not null THEN Sales_D240*D240D270_avg*D270D300_avg*D300D330_avg   *D330D360_avg
    WHEN Sales_D210 is not null THEN Sales_D210*D210D240_avg*D240D270_avg*D270D300_avg*D300D330_avg    *D330D360_avg
    WHEN Sales_D180 is not null THEN Sales_D180*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg*D300D330_avg *D330D360_avg
    WHEN Sales_D150 is not null THEN Sales_D150*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg*D300D330_avg    *D330D360_avg
    WHEN Sales_D120 is not null THEN Sales_D120*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg*D300D330_avg  *D330D360_avg
    WHEN Sales_D90 is not null THEN Sales_D90*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg*D300D330_avg *D330D360_avg
    WHEN Sales_D60 is not null THEN Sales_D60*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg*D300D330_avg   *D330D360_avg
    WHEN Sales_D30 is not null THEN Sales_D30*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg*D300D330_avg     *D330D360_avg
    WHEN Sales_D14 is not null  THEN Sales_D14*D14D30_avg*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg*D300D330_avg     *D330D360_avg
    ELSE Sales_D7*D7D14_avg*D14D30_avg*D30D60_avg*D60D90_avg*D90D120_avg*D120D150_avg*D150D180_avg*D180D210_avg*D210D240_avg*D240D270_avg*D270D300_avg*D300D330_avg*D330D360_avg          END as Sales_D360_p
    from(

    select *
    ,  LAST_VALUE(d1d3_avg3 IGNORE NULLS ) over(partition by joyplegameid ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  as D1D3_avg
    ,  LAST_VALUE(d3d7_avg3 IGNORE NULLS ) over(partition by joyplegameid ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  as D3D7_avg
    ,  LAST_VALUE(d7d14_avg3 IGNORE NULLS ) over(partition by joyplegameid ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  as D7D14_avg
    ,  LAST_VALUE(d14d30_avg3 IGNORE NULLS ) over(partition by joyplegameid ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as D14D30_avg
    ,  LAST_VALUE(d30d60_avg3 IGNORE NULLS ) over(partition by joyplegameid ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as D30D60_avg
    ,  LAST_VALUE(d60d90_avg3 IGNORE NULLS ) over(partition by joyplegameid ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as D60D90_avg
    ,  LAST_VALUE(d90d120_avg3 IGNORE NULLS ) over(partition by joyplegameid ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as d90d120_avg
    ,  LAST_VALUE(d120d150_avg3 IGNORE NULLS ) over(partition by joyplegameid ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as d120d150_avg
    ,  LAST_VALUE(d150d180_avg3 IGNORE NULLS ) over(partition by joyplegameid ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as d150d180_avg
    ,  LAST_VALUE(d180d210_avg3 IGNORE NULLS ) over(partition by joyplegameid ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as d180d210_avg
    ,  LAST_VALUE(d210d240_avg3 IGNORE NULLS ) over(partition by joyplegameid ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as d210d240_avg
    ,  LAST_VALUE(d240d270_avg3 IGNORE NULLS ) over(partition by joyplegameid ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as d240d270_avg
    ,  LAST_VALUE(d270d300_avg3 IGNORE NULLS ) over(partition by joyplegameid ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as d270d300_avg
    ,  LAST_VALUE(d300d330_avg3 IGNORE NULLS ) over(partition by joyplegameid ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as d300d330_avg
    ,  LAST_VALUE(d330d360_avg3 IGNORE NULLS ) over(partition by joyplegameid ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as d330d360_avg

    from(
    select *,
    CASE WHEN Sales_D3 is null THEN null ELSE AVG(Sales_D3/Sales_D1) OVER (partition by joyplegameid ORDER BY month ROWS BETWEEN 3 PRECEDING AND  1 PRECEDING ) END AS d1d3_avg3, -- 현재월제외 kpi계산용
    CASE WHEN Sales_D7 is null THEN null ELSE AVG(Sales_D7/Sales_D3) OVER (partition by joyplegameid ORDER BY month ROWS BETWEEN 3 PRECEDING AND  1 PRECEDING) END AS d3d7_avg3, -- 현재월제외 kpi계산용

    CASE WHEN Sales_D14 is null THEN null ELSE AVG(Sales_D14/Sales_D7) OVER (partition by joyplegameid ORDER BY month ROWS BETWEEN 2 PRECEDING AND  CURRENT ROW ) END AS d7d14_avg3,
    CASE WHEN Sales_D30 is null THEN null ELSE AVG(Sales_D30/Sales_D14) OVER (partition by joyplegameid ORDER BY month ROWS BETWEEN 2 PRECEDING AND  CURRENT ROW ) END AS d14d30_avg3,
    CASE WHEN Sales_D60 is null THEN null ELSE AVG(Sales_D60/Sales_D30) OVER (partition by joyplegameid ORDER BY month ROWS BETWEEN 2 PRECEDING AND  CURRENT ROW ) END AS d30d60_avg3,
    CASE WHEN Sales_D90 is null THEN null ELSE AVG(Sales_D90/Sales_D60) OVER (partition by joyplegameid ORDER BY month ROWS BETWEEN 2 PRECEDING AND  CURRENT ROW ) END AS d60d90_avg3,
    CASE WHEN Sales_D120 is null THEN null ELSE AVG(Sales_D120/Sales_D90) OVER (partition by joyplegameid ORDER BY month ROWS BETWEEN 2 PRECEDING AND  CURRENT ROW ) END AS d90d120_avg3,
    CASE WHEN Sales_D150 is null THEN null ELSE AVG(Sales_D150/Sales_D120) OVER (partition by joyplegameid ORDER BY month ROWS BETWEEN 2 PRECEDING AND  CURRENT ROW ) END AS d120d150_avg3,
    CASE WHEN Sales_D180 is null THEN null ELSE AVG(Sales_D180/Sales_D150) OVER (partition by joyplegameid ORDER BY month ROWS BETWEEN 2 PRECEDING AND  CURRENT ROW ) END AS d150d180_avg3,
    CASE WHEN Sales_D210 is null THEN null ELSE AVG(Sales_D210/Sales_D180) OVER  (partition by joyplegameid ORDER BY month ROWS BETWEEN 2 PRECEDING AND  CURRENT ROW ) END AS d180d210_avg3,
    CASE WHEN Sales_D240 is null THEN null ELSE AVG(Sales_D240/Sales_D210) OVER  (partition by joyplegameid ORDER BY month ROWS BETWEEN 2 PRECEDING AND  CURRENT ROW )END AS d210d240_avg3,
    CASE WHEN Sales_D270 is null THEN null ELSE AVG(Sales_D270/Sales_D240) OVER  (partition by joyplegameid ORDER BY month ROWS BETWEEN 2 PRECEDING AND  CURRENT ROW ) END AS d240d270_avg3,
    CASE WHEN Sales_D300 is null THEN null ELSE AVG(Sales_D300/Sales_D270) OVER  (partition by joyplegameid ORDER BY month ROWS BETWEEN 2 PRECEDING AND  CURRENT ROW ) END AS d270d300_avg3,
    CASE WHEN Sales_D330 is null THEN null ELSE AVG(Sales_D330/Sales_D300) OVER  (partition by joyplegameid ORDER BY month ROWS BETWEEN 2 PRECEDING AND  CURRENT ROW ) END AS d300d330_avg3,
    CASE WHEN Sales_D360 is null THEN null ELSE AVG(Sales_D360/Sales_D330) OVER  (partition by joyplegameid ORDER BY month ROWS BETWEEN 2 PRECEDING AND  CURRENT ROW )END AS d330d360_avg3
    from revraw
    )
    )
    )
    )

    ,final2 AS(
    select a.*, b.cost, b.cost_exclude_credit
    from final as a
    left join (
    select joyplegameid,  format_date('%Y-%m', cmpgndate) as month
    , sum(costcurrency) as cost, sum(costcurrencyuptdt) as cost_exclude_credit
    from  `dataplatform-reporting.DataService.V_0410_0000_CostCampaignRule_V`
    where joyplegameid = {joyplegameid}
    and cmpgndate >= DATE_SUB(DATE(CONCAT(FORMAT_DATE('%Y-%m', DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY)),'-01')), INTERVAL 24 MONTH)
    and cmpgndate <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -8 DAY) AS Date)
    group by joyplegameid,  format_date('%Y-%m', cmpgndate)

    ) as b
    on a.joyplegameid = b.joyplegameid
    and a.month = b.month
    )

    select month as `가입월`,
    matured_daydiff as `지표확정 최대기간`,
    cost_exclude_credit as `마케팅 비용`,
    ru*d1_ltv/cost_exclude_credit as `ROAS D1`,
    ru*d3_ltv/cost_exclude_credit as `ROAS D3`,
    ru*d7_ltv/cost_exclude_credit as `ROAS D7`,
    ru*d14_ltv/cost_exclude_credit as `ROAS D14`,
    ru*d30_ltv/cost_exclude_credit as `ROAS D30`,
    ru*d60_ltv/cost_exclude_credit as `ROAS D60`,
    ru*d90_ltv/cost_exclude_credit as `ROAS D90`,
    ru*d120_ltv/cost_exclude_credit as `ROAS D120`,
    ru*d150_ltv/cost_exclude_credit as `ROAS D150`,
    ru*d180_ltv/cost_exclude_credit as `ROAS D180`,
    ru*d210_ltv/cost_exclude_credit as `ROAS D210`,
    ru*d240_ltv/cost_exclude_credit as `ROAS D240`,
    ru*d270_ltv/cost_exclude_credit as `ROAS D270`,
    ru*d300_ltv/cost_exclude_credit as `ROAS D300`,
    ru*d330_ltv/cost_exclude_credit as `ROAS D330`,
    ru*d360_ltv/cost_exclude_credit as `ROAS D360`,
    ru*d14_ltv_p/cost_exclude_credit as `ROAS D14 예측치`,
    ru*d30_ltv_p/cost_exclude_credit as `ROAS D30 예측치`,
    ru*d60_ltv_p/cost_exclude_credit as `ROAS D60 예측치`,
    ru*d90_ltv_p/cost_exclude_credit as `ROAS D90 예측치`,
    ru*d120_ltv_p/cost_exclude_credit as `ROAS D120 예측치`,
    ru*d150_ltv_p/cost_exclude_credit as `ROAS D150 예측치`,
    ru*d180_ltv_p/cost_exclude_credit as `ROAS D180 예측치`,
    ru*d210_ltv_p/cost_exclude_credit as `ROAS D210 예측치`,
    ru*d240_ltv_p/cost_exclude_credit as `ROAS D240 예측치`,
    ru*d270_ltv_p/cost_exclude_credit as `ROAS D270 예측치`,
    ru*d300_ltv_p/cost_exclude_credit as `ROAS D300 예측치`,
    ru*d330_ltv_p/cost_exclude_credit as `ROAS D330 예측치`,
    ru*d360_ltv_p/cost_exclude_credit as `ROAS D360 예측치`
    from final2
    order by `가입월`
    """

    query_result6_monthlyROAS = query_run_method('6_newuser_roas', bigquery_client, query)
    query_result6_monthlyROAS['지표확정 최대기간'] = (
    query_result6_monthlyROAS['지표확정 최대기간'].astype(str).str.replace('_', '', regex=False)
    )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    gcs_path = f"{gameidx}/{timestamp}.parquet"
        
    saved_path = save_df_to_gcs(query_result6_monthlyROAS, bucket, gcs_path)

    return saved_path



def result6_pLTV(joyplegameid:int, gameidx:str, bigquery_client, bucket, **context):

    ## pLTV D360
    query = f"""

    with perfo_raw AS(
    select a.*
    , b.countrycode, b.os
    , b.gcat, b.mediacategory, b.class, b.media, b.adsetname, b.adname, b.optim, b.oscam, b.geocam, b.targetgroup
    from(
    select *,
    case when logdatekst < current_date('Asia/Seoul') then pricekrw else daypred_low end as combined_rev_low,
    case when logdatekst < current_date('Asia/Seoul') then pricekrw else daypred_upp end as combined_rev_upp,
    FROM `data-science-division-216308.VU.Performance_pLTV`
    where authaccountregdatekst >= DATE_SUB(DATE(CONCAT(FORMAT_DATE('%Y-%m', DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY)),'-01')), INTERVAL 24 MONTH)
    and authaccountregdatekst <= CAST(DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -8 DAY) AS Date)
    and JoypleGameID = {joyplegameid}
    ) as a
    left join (select  *
    from `dataplatform-reporting.DataService.V_0316_0000_AuthAccountInfo_V`
    where JoypleGameID = {joyplegameid}
    ) as b
    on a.authaccountname = b.authaccountname
    and a.joyplegameid = b.joyplegameid
    )

    select format_date('%Y-%m',AuthAccountRegDateKST) as `가입월`
        ,count(distinct if(daysfromregisterdate = 0, authaccountname, null)) as RU
        ,sum(if(daysfromregisterdate <= 360, combined_rev, null)) as `매출 D360 예측치`
        , max(authaccountregdatekst) as `최대 가입일자`
    from perfo_raw
    group by joyplegameid, format_date('%Y-%m',AuthAccountRegDateKST)

    """
    query_result6_pLTV = query_run_method('6_newuser_roas', bigquery_client, query)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    gcs_path = f"{gameidx}/{timestamp}.parquet"
        
    saved_path = save_df_to_gcs(query_result6_pLTV, bucket, gcs_path)

    return saved_path



##### 복귀 유저 데이터
def result6_return(joyplegameid:int, gameidx:str, bigquery_client, bucket, **context):

    query = f"""
    with raw AS(
    select *
    , sum(d90diff) over(partition by joyplegameid, authaccountname order by logdatekst) as cum_d90diff
    from(
    select *
    , date_diff(logdatekst,AuthAccountLastAccessBeforeDateKST, day ) as daydiff_beforeaccess   -- authaccountlastaccessbeforedatekst : Access 기준으로 로깅
    , case when  date_diff(logdatekst,AuthAccountLastAccessBeforeDateKST, day )  >= 90 then 1 else 0  end as d90diff
    FROM `dataplatform-reporting.DataService.T_0317_0000_AuthAccountPerformance_V`
    WHERE joyplegameid = {joyplegameid}
    and logdatekst >= '2023-01-01'
    and DaysFromRegisterDate >= 0 -- 가입일이 이후에 찍힌 case제외
    )
    )

    , raw2 AS(
    select *, date_diff(logdatekst, returndate, day) as daydiff_re -- 복귀일 cohort
    -- , if(returndate = AuthAccountRegDateKST, 0,1) as return_yn -- 가입일이 먼저 찍힌 case 포함
    , if(cum_d90diff = 0, 0,1) as return_yn -- 가입일이 먼저 찍힌 case 포함
    from(
    select *
    , first_value(logdatekst) over(partition by joyplegameid, authaccountname, cum_d90diff order by logdatekst) as returndate
    from raw
    )
    )

    , ru_raw AS(
    -- 신규 유저 기준
    select joyplegameid,  format_date('%Y-%m',authaccountregdatekst) as regmonth
    , count(distinct authaccountname) as ru
    , sum(if(DaysFromRegisterDate<=360, pricekrw, null)) as d360rev
    from raw2
    where  AuthAccountRegDateKST  >= '2023-01-01'
    and AuthAccountRegDateKST <= DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 8 DAY)
    group by joyplegameid,    format_date('%Y-%m',authaccountregdatekst)
    )

    , return_raw AS(
    -- 복귀유저
    select joyplegameid,  format_date('%Y-%m', returndate) as regmonth
    , count(distinct if(daydiff_re = 0 , authaccountname, null)) as ru
    , sum(if(daydiff_re<=360, pricekrw, null)) as d360rev_all
    from raw2
    where  returndate  >= '2023-01-01'
    and returndate <= DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 8 DAY)
    group by joyplegameid,   format_date('%Y-%m', returndate)
    )


    ,final AS(
    select
    ifnull(ifnull(a.joyplegameid , b.joyplegameid) , c.joyplegameid)  as joyplegameid
    ,ifnull(ifnull(a.regmonth , b.regmonth)  , c.regmonth) as regmonth
    , a.ru ,b.ru as ru_all,
    d360rev  AS rev_D360,
    d360rev_all  AS rev_D360_all
    ,  cost
    ,  cost_exclude_credit
    , d360rev_all - d360rev as rev_D360_return ,
    case WHEN DATE_DIFF(
            DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY),
            LAST_DAY(DATE(CONCAT(ifnull(ifnull(a.regmonth , b.regmonth)  , c.regmonth) , '-01'))),
            DAY
            ) >= 360 THEN 'mature'
        ELSE 'notmature'
    END AS status
    from ru_raw  as a
    full join return_raw as b
    on a.joyplegameid = b.joyplegameid
    and a.regmonth = b.regmonth
    full join (
            select joyplegameid,  format_date('%Y-%m',cmpgndate) as regmonth, sum(costcurrency) as cost, sum(costcurrencyuptdt) as cost_exclude_credit
            from  `dataplatform-reporting.DataService.V_0410_0000_CostCampaignRule_V`
            where joyplegameid = 133
            and cmpgndate >='2023-01-01'
            and cmpgndate <= DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 8 DAY)
            group by  joyplegameid,  format_date('%Y-%m',cmpgndate)
            ) as c
            on a.joyplegameid = c.joyplegameid
    and a.regmonth = c.regmonth
    )

    #notmature 구간 최근 6개월 평균
    #POTC 2024/4 ~ 6월 로그인 이슈로 복귀유저 기여도 낮아 제외
    , return_user_proas AS(
    select joyplegameid, avg(rev_D360_return/cost_exclude_credit) as d360_return_roas
    , approx_quantiles(rev_D360_return/cost_exclude_credit, 2)[OFFSET(1)] as d360_return_roas_med
    from(
    select *, row_number() over (partition by joyplegameid order by regmonth desc) as rownum
    from final
    where status = 'mature'
    and (
        -- (joyplegameid = 131 and regmonth not in ('2024-04','2024-05','2024-06')) or joyplegameid in (133,30001,30003)
        joyplegameid = {joyplegameid}
        )
    )
    where rownum <= 6 -- 최근 6개월
    group by joyplegameid
    )

    select a.regmonth as `가입월`
        , a.RU
        , a.RU_all
        , a.cost_exclude_credit as `마케팅 비용`
        , rev_D360_return as `복귀유저 매출 D360`
        , status as `데이터 완성 여부`
    , rev_D360_return/cost_exclude_credit as `복귀유저 ROAS D360`
    , case when status = 'mature' then rev_D360_return/cost_exclude_credit
        else b.d360_return_roas_med end as `복귀유저 ROAS D360 예측치`
    from final  as a
    left join return_user_proas as b
    on a.joyplegameid = b.joyplegameid
    """

    query_result6_return =query_run_method('6_newuser_roas', bigquery_client, query)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    gcs_path = f"{gameidx}/{timestamp}.parquet"
        
    saved_path = save_df_to_gcs(query_result6_return, bucket, gcs_path)

    return saved_path



### 수수료 적용 BEP 계산
def result6_BEP(joyplegameid:int, gameidx:str, bigquery_client, bucket, **context):

    query = f"""
    with raw AS(
    select a.*, b.value
    , case when b.value is not null then b.value
    when b.value is null and a.PGName = 'Google' then 0.3
    when b.value is null and a.PGName = 'Apple' and a.joyplegameid = 131 then 0.33
    when b.value is null and a.PGName = 'Apple' and a.joyplegameid = 133 then 0.32
    when b.value is null and a.PGName = 'Apple' and a.joyplegameid = 30001 then 0.32
    when b.value is null and a.PGName = 'Apple' and a.joyplegameid = 30003 then 0.31
    when b.value is null and a.PGName = 'Xsolla' and a.joyplegameid = 131 then 0.15
    when b.value is null and  a.PGName = 'Xsolla' and a.joyplegameid = 133 then 0.10
    when b.value is null and  a.PGName = 'Xsolla' and a.joyplegameid = 30001 then 0.09
    when b.value is null and  a.PGName = 'Xsolla' and a.joyplegameid = 30003 then 0.08
    when b.value is null and  a.PGName = 'Danal' then 0.03
    when b.value is null and  a.PGName = 'One Store' and a.joyplegameid = 131 then 0.3
    when b.value is null and  a.PGName = 'One Store' and a.joyplegameid = 133 then 0.24
    when b.value is null and  a.PGName = 'One Store' and a.joyplegameid = 30001 then 0.24
    when b.value is null and  a.PGName = 'One Store' and a.joyplegameid = 30003 then 0.24
    when b.value is null and  a.PGName = 'Facebook Gaming' then 0.3
    when b.value is null and a.PGName = 'Steam' then 0.3
    else 0.3
    end as commission_rate
    from(
    select JoypleGameID, format_date('%Y-%m', authaccountregdatekst) as regmonth , t2.PGName, sum(t2.PGPriceKRW) as sales
    from  dataplatform-reporting.DataService.V_0317_0000_AuthAccountPerformance_V AS t1,
    UNNEST(t1.PaymentDetailArrayStruct) AS t2
    where joyplegameid = {joyplegameid}
    and authaccountregdatekst >= DATE_SUB(DATE(CONCAT(FORMAT_DATE('%Y-%m', DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY)),'-01')), INTERVAL 24 MONTH)
    group by JoypleGameID, format_date('%Y-%m', authaccountregdatekst) , t2.PGName
    ) as a
    left join (
    select joyplegameid, regmonth, PGName,  value
    , case when PGName = 'One_Store' then 'One Store'
    when PGName = 'Facebook_Gaming' then 'Facebook Gaming'
    else PGName end as pgname2
    from `data-science-division-216308.Common.pg_commission_rate_copy2`
    unpivot (
    value for PGName in (Google,Apple, Xsolla	,Danal,	One_Store	,Facebook_Gaming,	Steam)
    )
    ) as b
    on a.joyplegameid = b.joyplegameid
    and a.regmonth = b.regmonth
    and a.pgname = b.pgname2
    )

    -- BEP 계산

    select distinct regmonth as `가입월`, bep_commission as `수수료 적용후 BEP`
    from (
        select * , sum(sales) over(partition by joyplegameid , regmonth) as cumsales
                , sales /  sum(sales) over(partition by joyplegameid , regmonth) as sales_p
                , sum(commission) over(partition by joyplegameid , regmonth) as cumcommission
                , sum(commission) over(partition by joyplegameid , regmonth) /  sum(sales) over(partition by joyplegameid , regmonth) as total_commssion_rate
                , case when joyplegameid in (131,133) then 1/(1- sum(commission) over(partition by joyplegameid , regmonth) /  sum(sales) over(partition by joyplegameid , regmonth))
                        when joyplegameid in (30001,30003) then 1.08/(1- sum(commission) over(partition by joyplegameid , regmonth) /  sum(sales) over(partition by joyplegameid , regmonth))
                    end as bep_commission
        from(
            select  *, sales*commission_rate as commission
            from raw
            )
        )
    order by `가입월`
    """

    query_result6_BEP =query_run_method('6_newuser_roas', bigquery_client, query)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    gcs_path = f"{gameidx}/{timestamp}.parquet"
        
    saved_path = save_df_to_gcs(query_result6_BEP, bucket, gcs_path)

    return saved_path


### ROAS KPI
def result6_roaskpi(gameidx:str, bigquery_client, bucket, **context):

    query = f"""
    select kpi_d1, kpi_d3, kpi_d7, kpi_d14, kpi_d30, kpi_d60, kpi_d90, kpi_d120, kpi_d150, kpi_d180, kpi_d210, kpi_d240, kpi_d270, kpi_d300, kpi_d330, kpi_d360
    from
    (select * ,row_number() OVER (partition by project ORDER BY updateDate desc ) AS row_
    from `data-science-division-216308.MetaData.roas_kpi`
    where project='{gameidx}'
    and operationStatus = '운영 중')
    where row_=1

    """

    query_result6_roaskpi = query_run_method('6_newuser_roas', bigquery_client, query)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    gcs_path = f"{gameidx}/{timestamp}.parquet"
        
    saved_path = save_df_to_gcs(query_result6_roaskpi, bucket, gcs_path)

    return saved_path


def roas_kpi(gameidx:str, path_result6_roaskpi:str, bucket, **context):

    query_result6_roaskpi = load_df_from_gcs(bucket=bucket, path=path_result6_roaskpi)

    query_result6_roaskpi = query_result6_roaskpi * 100
    data = query_result6_roaskpi.rename(columns={
            'kpi_d1' : 'ROAS D1',
            'kpi_d3' : 'ROAS D3',
            'kpi_d7' : 'ROAS D7',
            'kpi_d14' : 'ROAS D14',
            'kpi_d30' : 'ROAS D30',
            'kpi_d60' : 'ROAS D60',
            'kpi_d90' : 'ROAS D90',
            'kpi_d120' : 'ROAS D120',
            'kpi_d150' : 'ROAS D150',
            'kpi_d180' : 'ROAS D180',
            'kpi_d210' : 'ROAS D210',
            'kpi_d240' : 'ROAS D240',
            'kpi_d270' : 'ROAS D270',
            'kpi_d300' : 'ROAS D300',
            'kpi_d330' : 'ROAS D330',
            'kpi_d360' : 'ROAS D360'
            })
    # 데이터프레임 생성
    roas_kpi = pd.DataFrame(data)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    gcs_path = f"{gameidx}/{timestamp}.parquet"
        
    saved_path = save_df_to_gcs(roas_kpi, bucket, gcs_path)

    return saved_path


###
def roas_dataframe_preprocessing(gameidx:str, path_result6_monthlyROAS:str, path_result6_pLTV:str, path_result6_return:str, path_result6_BEP:str, bucket, **context):

    query_result6_monthlyROAS = load_df_from_gcs(bucket, path_result6_monthlyROAS)
    query_result6_pLTV = load_df_from_gcs(bucket, path_result6_pLTV)
    query_result6_return = load_df_from_gcs(bucket, path_result6_return)
    query_result6_BEP = load_df_from_gcs(bucket, path_result6_BEP)

    query6_monthlyROAS = pd.merge(query_result6_monthlyROAS, query_result6_pLTV[['가입월', '매출 D360 예측치']], on = ['가입월'], how = "left")
    query6_monthlyROAS = pd.merge(query6_monthlyROAS
                                , query_result6_return[['가입월', '복귀유저 ROAS D360', '복귀유저 ROAS D360 예측치', '데이터 완성 여부']]
                                , on = ['가입월'], how = 'left')
    query6_monthlyROAS = pd.merge(query6_monthlyROAS, query_result6_BEP, on = ['가입월'], how = 'left')

    target_columns = ["ROAS D14","ROAS D30","ROAS D60","ROAS D90","ROAS D120",
            "ROAS D150","ROAS D180","ROAS D210","ROAS D240","ROAS D270",
            "ROAS D300","ROAS D330","ROAS D360"]

    ## 실측치 컬럼 빈칸에 예측치 컬럼값으로 채우기
    for col in target_columns:
        pred = f"{col} 예측치"
        if col in query6_monthlyROAS.columns and pred in query6_monthlyROAS.columns:
            query6_monthlyROAS[col] = query6_monthlyROAS[col].fillna(query6_monthlyROAS[pred])

    ## 예측치 컬럼 제외(복귀유저 예측치는 그대로 두기)
    cols_to_drop = [
        c for c in query6_monthlyROAS.columns
        if ("예측치" in c) and ("복귀유저" not in c)
    ]
    query6_monthlyROAS = query6_monthlyROAS.drop(columns=cols_to_drop)

    ## 복귀유저 포함 D360 ROAS 계산 -> mature 안된 경우 예측치로 계산
    mature_mask = query6_monthlyROAS['데이터 완성 여부'].eq('mature')

    query6_monthlyROAS['복귀유저 포함 ROAS D360'] = np.where(
        mature_mask,
        query6_monthlyROAS['ROAS D360'] + query6_monthlyROAS['복귀유저 ROAS D360'],
        query6_monthlyROAS['ROAS D360'] + query6_monthlyROAS['복귀유저 ROAS D360 예측치']
    )

    query6_monthlyROAS['기본 BEP'] = 1.429

    ## 2) 컬럼을 'ROAS D360' 다음으로 이동
    cols = query6_monthlyROAS.columns.tolist()
    cols.remove('복귀유저 포함 ROAS D360')
    cols.remove('기본 BEP')
    insert_at = cols.index('ROAS D360') + 1
    cols.insert(insert_at, '복귀유저 포함 ROAS D360')
    insert_at = cols.index('데이터 완성 여부') + 1
    cols.insert(insert_at, '기본 BEP')
    cols.remove('데이터 완성 여부')
    insert_at = cols.index('수수료 적용후 BEP') + 1
    cols.insert(insert_at, '데이터 완성 여부')
    query6_monthlyROAS = query6_monthlyROAS[cols]

    roas_days = [1, 3, 7, 14, 30, 60, 90, 120, 150, 180, 210, 240, 270, 300, 330, 360]

    # 성장세 컬럼 생성
    for i in range(1, len(roas_days)):
        prev_day = roas_days[i - 1]
        curr_day = roas_days[i]

        prev_col = f"ROAS D{prev_day}"
        curr_col = f"ROAS D{curr_day}"
        new_col = f"LTV 성장세 D{curr_day}"

        query6_monthlyROAS[new_col] = query6_monthlyROAS[curr_col] / query6_monthlyROAS[prev_col]

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    gcs_path = f"{gameidx}/{timestamp}.parquet"
        
    saved_path = save_df_to_gcs(query6_monthlyROAS, bucket, gcs_path)

    return saved_path


########## ROAS 프롬프트
def result6_ROAS_gemini(gameidx:str, service_sub:str, path_monthlyBEP_ROAS:str, path_roas_kpi:str, bucket, genai_client, MODEL_NAME, SYSTEM_INSTRUCTION, **context):

    RUN_ID = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    LABELS = {"datascience_division_service": 'gameinsight_framework',
            "run_id": RUN_ID,
            "datascience_division_service_sub" : service_sub}
    
    # KST 타임존 정의 (UTC+9)
    kst = timezone(timedelta(hours=9))

    # 어제 날짜 (KST 기준)
    yesterday_kst = datetime.now(kst) - timedelta(days=1)

    # 어제 날짜의 연도
    year = yesterday_kst.year

    #print("어제 날짜(KST):", yesterday_kst.date())
    #print("어제 연도:", year)

    query6_monthlyROAS = load_df_from_gcs(bucket=bucket, path=path_monthlyBEP_ROAS)
    roas_kpi = load_df_from_gcs(bucket=bucket, path=path_roas_kpi)

    response6_monthlyROAS = genai_client.models.generate_content(
        model=MODEL_NAME,
        contents = f"""
    < 월별 마케팅비용과 ROAS>
    {query6_monthlyROAS.to_csv(index=False)}
    다음은 가입월별 ROAS 야. "지표확정 최대기간" 이후의 ROAS는 예측치이니, "지표확정  최대기간" 이후의 ROAS를 언급할 때에는 예측치라고 말해줘.
    어느 가입월이 "복귀유저 포함 ROAS D360"이 KPI를 달성했는지, 또는 달성하지 못했는지를 서두에 Bold 체로 한줄로 언급해줘.
    {year} 연도만 적어줘.

    KPI 는 수수료 적용후 BEP 로 판단하면돼.

    그리고 "복귀유저 포함 ROAS D360" 이 KPI 달성하지 못한 올해 월들은
    아래 ROAS KPI 와 비교해서 어떤 코호트부터 미달하여 달성하지 못했다고 간단히 알려줘.
    달성하지 못한 월들만 언급해줘.
    한 문장마다 노션의 마크다운 리스트 문법을 사용해줘. e.g. * 당월 매출은 이렇습니다.
    ROAS와 KPI 수치는 소수점 첫째자리까지 %로 표시해줘.

    <ROAS KPI>
    {roas_kpi}

    """,
        config=types.GenerateContentConfig(
            system_instruction=SYSTEM_INSTRUCTION
            #,tools=[RAG]
            ,temperature=0.1
            ,labels=LABELS
            # max_output_tokens=2048
        )
    )

    # GCS에 업로드
    print("📤 GCS에 제미나이 코멘트 업로드 중...")
    gcs_response_path = f"{gameidx}/response6_monthlyROAS.text"
    blob = bucket.blob(gcs_response_path)
    blob.upload_from_string(
        response6_monthlyROAS.text,
        content_type='text/markdown; charset=utf-8'
    )
    
    
    return response6_monthlyROAS.text



########## LTV 성장세 부분 프롬프트
def monthlyLTVgrowth_gemini(service_sub:str, path_monthlyBEP_ROAS:str, bucket, genai_client, MODEL_NAME, SYSTEM_INSTRUCTION, **context):

    RUN_ID = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    LABELS = {"datascience_division_service": 'gameinsight_framework',
            "run_id": RUN_ID,
            "datascience_division_service_sub" : service_sub}
    
    query6_monthlyROAS = load_df_from_gcs(bucket=bucket, path=path_monthlyBEP_ROAS)

    response6_monthlyLTVgrowth = genai_client.models.generate_content(
    model=MODEL_NAME,
    contents = f"""
    < 월별 마케팅비용과 ROAS>
    {query6_monthlyROAS.to_csv(index=False)}
    다음은 가입월별 ROAS와 LTV 성장세야. "지표확정 최대기간" 이후의 LTV 성장세는 예측치이니, "지표확정  최대기간" 이후의 LTV 성장세를 언급할 때에는 예측치라고 말해줘.
    가입월에 따라서 어느 구간의 성장세가 증가하거나, 하락하는지 트렌드만 언급해줘. e.g. D7 LTV 성장세가 2025년 1월부터 하락했습니다.
    구간은 D3부터 D30까지 초반, D60부터 D180까지 중반, D180부터 D360까지는 후반으로 나눠서 각 구간에 대한 트렌드로 언급해줘.
    가입월별로 하지말고 가입월에 따른 각 구간의 트렌드를 요약해서 언급하되, 특정 가입월에서 크게 상승하거나 크게 하락했다면 그 가입월에 대해서는 수치와 함께 언급해줘.
    LTV 성장세는 소수점 첫째자리까지 %로 표시해줘.
    10줄 이내로 작성해줘.
    한 문장마다 노션의 마크다운 리스트 문법을 사용해줘. e.g. * 당월 매출은 이렇습니다.
    """,
        config=types.GenerateContentConfig(
            system_instruction=SYSTEM_INSTRUCTION,
            # tools=[RAG],
            temperature=0.1
            ,labels=LABELS
            # max_output_tokens=2048
        )
    )
    return response6_monthlyLTVgrowth.text


### ROAS 현황 및 KPI 표 이미지 생성
def roas_table_draw(gameidx:str, path_roas_dataframe_preprocessing:str, path_result6_monthlyROAS:str, bucket, gcs_bucket:str, **context):

    query6_monthlyROAS = load_df_from_gcs(bucket=bucket, path=path_roas_dataframe_preprocessing)
    query_result6_monthlyROAS = load_df_from_gcs(bucket=bucket, path=path_result6_monthlyROAS)

    df_numeric = query6_monthlyROAS.drop(columns=['데이터 완성 여부']).copy()
    df_numeric = df_numeric.reset_index(drop=True)

    nest_asyncio.apply()

    def infer_cohort_dn_map(df):
        cohort_map = {}
        for idx, row in df.iterrows():
            regmonth = idx[1] if isinstance(idx, tuple) else row.get('가입월', None)
            for col in df.columns:
                if re.search(r"예측치$", col):
                    if pd.notna(row[col]):
                        dn = int(re.findall(r'\d+', col)[0])
                        # 예측치가 존재하는 가장 작은 dn 값을 기준으로 설정
                        if regmonth not in cohort_map or dn < cohort_map[regmonth]:
                            cohort_map[regmonth] = dn
        return cohort_map
    
    cohort_dn_map = infer_cohort_dn_map(query_result6_monthlyROAS)

    ## 컬럼 이름에서 dn 값 추출
    def extract_dn(col):
        match = re.match(r'(ROAS|LTV 성장세) D(\d+)', col)
        return int(match.group(2)) if match else None

    # dn_values는 <br> 없는 clean 컬럼명 기준으로 생성
    dn_values = {col: extract_dn(col) for col in query6_monthlyROAS.columns if col.startswith("ROAS D") or col.startswith("LTV 성장세 D")}

    # 개행할 컬럼 지정 및 개행 입력한 컬럼명으로 변경
    custom_colnames = {
        "지표확정 최대기간": "지표확정<br>최대기간",
        "복귀유저 포함 ROAS D360": "복귀유저 포함<br>ROAS D360",
        "복귀유저 ROAS D360": "복귀유저<br>ROAS D360",
        "복귀유저 ROAS D360 예측치": "복귀유저<br>ROAS D360<br>예측치",
        "기본 BEP": "기본<br>BEP",
        "수수료 적용후 BEP": "수수료 적용후<br>BEP",
        "LTV 성장세 D3": "LTV<br>성장세<br>D1 D3",
        "LTV 성장세 D7": "LTV<br>성장세<br>D3 D7",
        "LTV 성장세 D14": "LTV<br>성장세<br>D7 D14",
        "LTV 성장세 D30": "LTV<br>성장세<br>D14 D30",
        "LTV 성장세 D60": "LTV<br>성장세<br>D30 D60",
        "LTV 성장세 D90": "LTV<br>성장세<br>D60 D90",
        "LTV 성장세 D120": "LTV<br>성장세<br>D90 D120",
        "LTV 성장세 D150": "LTV<br>성장세<br>D120 D150",
        "LTV 성장세 D180": "LTV<br>성장세<br>D150 D180",
        "LTV 성장세 D210": "LTV<br>성장세<br>D180 D210",
        "LTV 성장세 D240": "LTV<br>성장세<br>D210 D240",
        "LTV 성장세 D270": "LTV<br>성장세<br>D240 D270",
        "LTV 성장세 D300": "LTV<br>성장세<br>D270 D300",
        "LTV 성장세 D330": "LTV<br>성장세<br>D300 D330",
        "LTV 성장세 D360": "LTV<br>성장세<br>D330 D360"
    }
    df_numeric = df_numeric.rename(columns=custom_colnames)

    ## 스타일 함수 정의 - mautred 되지 않은 구간 회색처리
    def highlight_based_on_dn(row):
        regmonth = row['가입월']
        cohort_dn = cohort_dn_map.get(regmonth, np.inf)

        styles = []
        for col in row.index:
            clean_col = col.replace("<br>", " ").strip()

            # 두 숫자가 있으면 마지막 숫자를 dn으로
            match = re.findall(r'D(\d+)', clean_col)
            dn_val = int(match[-1]) if match else None

            if (
                (clean_col.startswith('ROAS D') or clean_col.startswith('LTV 성장세'))
                and dn_val is not None
                and dn_val >= cohort_dn
                and pd.notna(row[col])
            ):
                styles.append('background-color: lightgray')
            else:
                styles.append('')
        return styles

    #### roas 달성 구간 빨간색 음영
    def highlight_roas_vs_bep(row):
        styles = []
        for col in row.index:
            style = ""
            try:
                # 값 변환
                if isinstance(row[col], str) and row[col].endswith('%'):
                    roas_val = float(row[col].replace('%', '')) / 100
                elif isinstance(row[col], (int, float)):
                    roas_val = row[col]
                else:
                    roas_val = None

                # 기준 bep_base 비교
                if col.startswith("ROAS D") and pd.notnull(row.get("기본<br>BEP")):
                    bep_val = row["기본<br>BEP"]
                    if pd.notnull(roas_val) and roas_val > bep_val:
                        style = "background-color: #fbe4e6"

                # d360 plus 비교 vs bep_commission
                elif col == "복귀유저 포함<br>ROAS D360" and pd.notnull(row.get("수수료 적용후<br>BEP")):
                    bep_comm = row["수수료 적용후<br>BEP"]
                    if pd.notnull(roas_val) and roas_val > bep_comm:
                        style = "background-color: #fbe4e6"

            except Exception as e:
                print(f"[DEBUG] {col} 처리 중 오류: {e}")
                style = ""

            styles.append(style)
        return styles
    
    #### ROAS 수치의 바 서식을 컬럼별이 아닌 전체 수치 기준으로 서식적용을 위한 파라미터값 설정

    roas_cols = [c for c in df_numeric.columns if c.startswith("ROAS D")] + ["복귀유저 포함<br>ROAS D360"]

    # 전체 최소/최대 구하기
    roas_global_min = df_numeric[roas_cols].min().min()
    roas_global_max = df_numeric[roas_cols].max().max()

    #### 성장세 수치의 바 서식을 컬럼별이 아닌 전체 수치 기준으로 서식적용을 위한 파라미터값 설정

    growth_cols = [c for c in df_numeric.columns if c.startswith("LTV<br>성장세<br>D")]

    # 전체 최소/최대 구하기
    growth_global_min = df_numeric[growth_cols].min().min()
    growth_global_max = df_numeric[growth_cols].max().max()

    styled = (
        df_numeric.style
        .hide(axis="index")
        .format({
            "마케팅 비용": "{:,.0f}",
            **{
                col: "{:.1%}"
                for col in df_numeric.columns
                if col.startswith("ROAS D")
                or col.startswith("LTV<br>성장세<br>D")
                or col.startswith("복귀유저")
                or col.endswith("BEP")
            }
        })
        .bar(subset=["마케팅 비용"], color="#f4cccc")
        .bar(subset=roas_cols, color="#c9daf8", vmin=roas_global_min, vmax=roas_global_max)
        .bar(subset=["복귀유저<br>ROAS D360", "복귀유저<br>ROAS D360<br>예측치"], color="#ffe599")
        .bar(subset=growth_cols, color="#b5f7a3", vmin=growth_global_min, vmax=growth_global_max)
        .set_table_styles(
            [
                {"selector": "th", "props": [("background-color", "#f0f0f0"), ("font-weight", "bold"), ("border", "1px solid black")]},
                {"selector": "td", "props": [("border", "1px solid black")]}
            ]
        )
        # 강조 함수 적용
        .apply(highlight_based_on_dn, axis=1)
        .apply(highlight_roas_vs_bep, axis=1)
    )

    # HTML 템플릿 정의
    html_template = """
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <style>
            body { font-family: Arial, sans-serif; padding: 20px; }
            table { border-collapse: collapse; font-size: 13px; }
            th, td {
                border: 1px solid #999;
                padding: 6px 10px;
                text-align: center;
            }
            th { background-color: #f0f0f0; }

            /* 특정 컬럼별 스타일 - 컬럼순서대로*/
            th:nth-child(1), td:nth-child(1) { min-width: 50px; max-width: 60px; } /* 가입월 */
            th:nth-child(2), td:nth-child(2) { min-width: 60px; max-width: 65px; white-space: normal; } /* 지표확정 최대기간 */
            th:nth-child(3), td:nth-child(3) { min-width: 70px; max-width: 95px; } /* 마케팅 비용 */
            th:nth-child(20), td:nth-child(20) { min-width: 80px; white-space: normal; } /* 복귀유저 포함 ROAS D360 */
            th:nth-child(21), td:nth-child(21) { min-width: 75px; white-space: normal; } /* 복귀유저 ROAS D360 */
            th:nth-child(22), td:nth-child(22) { min-width: 85px; white-space: normal; } /* 복귀유저 포함 ROAS D360 예측치 */
            th:nth-child(24), td:nth-child(24) { min-width: 80px; white-space: normal; } /* 수수료 적용후 BEP */
            th:nth-child(28), td:nth-child(28) { min-width: 55px; white-space: normal; } /* LTV 성장세 D14 D30 */
            th:nth-child(29), td:nth-child(29) { min-width: 55px; white-space: normal; } /* LTV 성장세 D30 D60 */
            th:nth-child(30), td:nth-child(30) { min-width: 55px; white-space: normal; } /* LTV 성장세 D60 D90 */
            th:nth-child(31), td:nth-child(31) { min-width: 60px; white-space: normal; } /* LTV 성장세 D90 D120 */
            th:nth-child(32), td:nth-child(32) { min-width: 70px; white-space: normal; } /* LTV 성장세 D120 D150 */
            th:nth-child(33), td:nth-child(33) { min-width: 70px; white-space: normal; } /* LTV 성장세 D150 D180 */
            th:nth-child(34), td:nth-child(34) { min-width: 70px; white-space: normal; } /* LTV 성장세 D180 D210 */
            th:nth-child(35), td:nth-child(35) { min-width: 70px; white-space: normal; } /* LTV 성장세 D210 D240 */
            th:nth-child(36), td:nth-child(36) { min-width: 70px; white-space: normal; } /* LTV 성장세 D240 D270 */
            th:nth-child(37), td:nth-child(37) { min-width: 70px; white-space: normal; } /* LTV 성장세 D270 D300 */
            th:nth-child(38), td:nth-child(38) { min-width: 70px; white-space: normal; } /* LTV 성장세 D300 D330 */
            th:nth-child(39), td:nth-child(39) { min-width: 70px; white-space: normal; } /* LTV 성장세 D330 D360 */

        </style>
    </head>
    <body>
        <h2>{{ game_name }} GBTW 신규유저 회수 현황</h2>
        {{ table | safe }}
    </body>
    </html>
    """

    # html 저장
    table_html = styled.to_html()
    rendered_html = Template(html_template).render(table=table_html)
    html_path = "table6_monthlyROAS.html"
    with open(html_path, "w", encoding="utf-8") as f:
            f.write(rendered_html)

    # 이미지 캡처 비동기 함수
    async def capture_html_to_image(html_path, output_image_path):
        async with async_playwright() as p:
            # 브라우저 열기
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page(viewport={"width": 1800, "height": 800})

            # HTML 파일 로드
            await page.goto("file://" + os.path.abspath(html_path))

            # 이미지 캡처
            await page.screenshot(path=output_image_path, full_page=True)

            # 브라우저 닫기
            await browser.close()

    def capture_image_task(html_path, gcs_bucket, gcs_path, project_id=None):
        """이미지를 캡처하고 GCS에 업로드"""
        import tempfile

        # 임시 디렉토리에서 작업
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_image_path = os.path.join(temp_dir, 'screenshot.png')
            
            # 1. 임시 파일에 이미지 캡처
            print(f"Capturing HTML to image: {html_path}")
            asyncio.run(capture_html_to_image(html_path, temp_image_path))
            print(f"Image saved temporarily to: {temp_image_path}")
            
            # 2. 파일이 제대로 생성되었는지 확인
            if not os.path.exists(temp_image_path):
                raise FileNotFoundError(f"Screenshot file not created: {temp_image_path}")
            
            file_size = os.path.getsize(temp_image_path)
            print(f"File size: {file_size} bytes")
            
            # 3. GCS에 업로드
            print(f"Uploading to GCS: gs://{gcs_bucket}/{gcs_path}")
            client = storage.Client(project=project_id)
            bucket = client.bucket(gcs_bucket)
            blob = bucket.blob(gcs_path)
            blob.upload_from_filename(temp_image_path, content_type='image/png')
            
            gcs_uri = f"gs://{gcs_bucket}/{gcs_path}"
            print(f"Image successfully uploaded to: {gcs_uri}")
            
            return True

    html_path = "table6_monthlyROAS.html"  # HTML 파일 경로
    output_image_path = "graph6_monthlyROAS.png"  # 저장될 이미지 경로
    gcs_path = f'{gameidx}/graph6_monthlyROAS.png'  # GCS에 저장될 경로

    #함수 호출 
    capture_image_task(html_path=html_path, gcs_bucket=gcs_bucket, gcs_path=gcs_path, project_id=PROJECT_ID)

    return gcs_path


############# KPI 테이블 이미지 생성
def kpi_table_draw(gameidx:str, path_roas_kpi:str, bucket, gcs_bucket:str, **context):

    roas_kpi = load_df_from_gcs(bucket=bucket, path=path_roas_kpi)

    nest_asyncio.apply()

    df_numeric = roas_kpi.copy()
    df_numeric = df_numeric.reset_index(drop=True)

    # 1) ROAS % → 비율 변환
    def to_ratio_series(s: pd.Series) -> pd.Series:
        s_str = s.astype(str)
        s_num = pd.to_numeric(s_str.str.replace('%', '', regex=False), errors='coerce')
        return s_num / 100.0

    for c in df_numeric.columns:
        if c.startswith("ROAS "):
            df_numeric[c] = to_ratio_series(df_numeric[c])

    # 2) suffixes 추출
    suffixes = []
    for c in df_numeric.columns:
        m = re.search(r'\b(D\d+)\b$', str(c))
        if m and m.group(1) not in suffixes:
            suffixes.append(m.group(1))
    suffixes_tuple = tuple(suffixes)

    # 3) Styler 기본 포맷
    styled = (
        df_numeric.style
        .hide(axis="index")
        .format({col: "{:.1%}" for col in df_numeric.columns if col.startswith("ROAS ")})
        .set_table_attributes('style="table-layout:fixed; width:600px;"')
    )

    # 4) HTML 템플릿
    html_template = """
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <style>
            body { font-family: Arial, sans-serif; padding: 20px; }
            h2 { margin: 0 0 10px 0; font-size: 18px; }
            table { border-collapse: collapse; font-size: 12px; border: 1px solid black; }
            th, td {
                border: 1px solid black;
                padding: 6px 8px;
                text-align: center;
                white-space: nowrap;
            }
            th { background-color: #f0f0f0; font-weight: bold; }
        </style>
    </head>
    <body>
        <h2>GBTW ROAS KPI (신규유저 기준)</h2>
        {{ table | safe }}
    </body>
    </html>
    """

    # 5) Styler → HTML
    soup = BeautifulSoup(styled.to_html(), "html.parser")
    table = soup.find("table")

    # 6) colgroup & width 적용
    ncols = len(df_numeric.columns)
    for cg in table.find_all("colgroup"):
        cg.decompose()

    colgroup = soup.new_tag("colgroup")
    width_map = {col: (80 if col.startswith("ROAS ") else 110) for col in df_numeric.columns}
    for col_name in df_numeric.columns:
        col = soup.new_tag("col", style=f"width: {width_map[col_name]}px !important;")
        colgroup.append(col)
    table.insert(0, colgroup)

    # 7) 헤더 줄바꿈 (ROAS → ROAS<br>…)
    for th in table.find_all("th"):
        text = th.get_text(strip=True)
        if text.startswith("ROAS "):
            th.string = ""
            th.append(BeautifulSoup(text.replace("ROAS ", "ROAS<br>"), "html.parser"))

    # 8) 최종 HTML 저장
    rendered_html = Template(html_template).render(table=str(table))
    html_path = "table6_ROAS_KPI.html"
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(rendered_html)

    import tempfile
    async def capture_html_to_image_async(html_path, gcs_bucket, gcs_path):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_image_path = os.path.join(temp_dir, "graph6_ROAS_KPI.png")
            
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page(viewport={"width": 600, "height": 160})
                await page.goto("file://" + os.path.abspath(html_path))
                await page.screenshot(path=temp_image_path, full_page=True)
                await browser.close()
            
            print(f"✓ Screenshot captured")
            
            # GCS 업로드
            client = storage.Client()
            bucket = client.bucket(gcs_bucket)
            blob = bucket.blob(gcs_path)
            blob.upload_from_filename(temp_image_path, content_type='image/png')
            
            gcs_uri = f"gs://{gcs_bucket}/{gcs_path}"
            print(f"✓ Uploaded to: {gcs_uri}")
            
            return gcs_uri

    def capture_and_upload_task(html_path, gcs_bucket, gcs_path, **context):
        """Airflow용 동기 래퍼 함수"""
        result = asyncio.run(
            capture_html_to_image_async(html_path, gcs_bucket, gcs_path)
        )
        
        # XCom에 GCS 경로 저장
        return result
    
    html_path = "table6_ROAS_KPI.html"
    gcs_path = f'{gameidx}/graph6_ROAS_KPI.png' 

    result = capture_and_upload_task(html_path, gcs_bucket, gcs_path, **context)

    return gcs_path



def roas_kpi_table_merge(gameidx:str, path_roas_dataframe_preprocessing:str, path_result6_monthlyROAS:str, path_roas_kpi:str, bucket, gcs_bucket:str, **context):

    p1 = roas_table_draw(gameidx, path_roas_dataframe_preprocessing, path_result6_monthlyROAS, bucket, gcs_bucket, **context)
    p2 = kpi_table_draw(gameidx, path_roas_kpi, bucket, gcs_bucket, **context)

    # 2) 이미지 열기 (투명 보존 위해 RGBA)
    blob1 = bucket.blob(p1)
    blob2 = bucket.blob(p2)

    im1 = blob1.download_as_bytes()
    im2 = blob2.download_as_bytes()

    img1 = Image.open(BytesIO(im1)).convert("RGBA")
    img2 = Image.open(BytesIO(im2)).convert("RGBA") 

    # 두 이미지의 크기 가져오기
    w1, h1 = img1.size
    w2, h2 = img2.size

    # 최종 캔버스 크기 (너비는 두 이미지 중 큰 값, 높이는 합계)
    final_width = max(w1, w2)
    final_height = h1 + h2

    # 흰색 배경의 새 캔버스 생성
    combined = Image.new("RGB", (final_width, final_height), (255, 255, 255))

    # 위에 roas_pc, 아래에 roaskpi_pc 붙이기 (왼쪽 정렬)
    combined.paste(img1, (0, 0))
    combined.paste(img2, (0, h1))

    # 저장
    combined.save("graph6_monthlyROAS_and_KPI.png", dpi=(180,180))

    # 3) GCS에 저장
    output_buffer = BytesIO()
    combined.save(output_buffer, format='PNG')
    output_buffer.seek(0)

    # GCS 경로
    gcs_path = f'{gameidx}/graph6_monthlyROAS_and_KPI.png'
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(output_buffer.getvalue(), content_type='image/png')
    print(f"✓✓✓✓✓ GCS에 최종 이미지 업로드 완료: {gcs_path}")

    return gcs_path


def retrieve_new_user_upload_notion(gameidx:str, service_sub:str, path_monthlyBEP_ROAS:str, path_roas_kpi:str, 
                                    MODEL_NAME:str, SYSTEM_INSTRUCTION:list, NOTION_TOKEN:str, NOTION_VERSION:str, notion, bucket, headers_json, **context):

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
                "rich_text": [{"type": "text", "text": {"content": "6. 신규유저 회수 현황" }}]
                },
            }
        ],
    )

    # 공통 헤더
    headers_json = headers_json

    try:
        gcs_path = f'{gameidx}/graph6_monthlyROAS_and_KPI.png'
        blob = bucket.blob(gcs_path)
        image_bytes = blob.download_as_bytes()
        filename = 'graph6_monthlyROAS_and_KPI.png'
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

    print("■■■■■■■■■■■ monthlyBEP_ROAS 데이터프레임 가져오기 시작 ■■■■■■■■■■■")
    query6_monthlyROAS = load_df_from_gcs(bucket=bucket, path=path_monthlyBEP_ROAS)

    resp = df_to_notion_table_under_toggle(
        notion=notion,
        page_id=PAGE_INFO['id'],
        df=query6_monthlyROAS,
        toggle_title="📊 로데이터 - 신규유저 회수현황",
        max_first_batch_rows=90,
        batch_size=100,
    )

    print("■■■■■■■■■■■ monthlyBEP_ROAS 데이터프레임 가져오기 완료 ■■■■■■■■■■■")
    from google.genai import Client
    genai_client = Client(vertexai=True,project=PROJECT_ID,location=LOCATION)

    text = result6_ROAS_gemini(gameidx=gameidx, service_sub=service_sub, 
                               path_monthlyBEP_ROAS=path_monthlyBEP_ROAS,
                               path_roas_kpi=path_roas_kpi,
                               bucket=bucket,
                               genai_client=genai_client,
                               MODEL_NAME=MODEL_NAME,
                               SYSTEM_INSTRUCTION=SYSTEM_INSTRUCTION,
                               **context)
    
    blocks = md_to_notion_blocks(text, 1)

    notion.blocks.children.append(
        block_id=PAGE_INFO['id'],
        children=blocks
    )

    text = monthlyLTVgrowth_gemini(service_sub=service_sub, 
                               path_monthlyBEP_ROAS=path_monthlyBEP_ROAS,
                               bucket=bucket,
                               genai_client=genai_client,
                               MODEL_NAME=MODEL_NAME,
                               SYSTEM_INSTRUCTION=SYSTEM_INSTRUCTION,
                               **context)
    blocks = md_to_notion_blocks(text, 1)

    notion.blocks.children.append(
        block_id=PAGE_INFO['id'],
        children=blocks
    )

    print("■■■■■■■■■■■ NOTION 업로드 완료 ■■■■■■■■■■■")
    return True
