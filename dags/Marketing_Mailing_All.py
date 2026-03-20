# 폰트 캐시 재구축
import matplotlib.font_manager as fm

# 라이브러리 import
import os
import requests
import json
import logging
import html
import pandas as pd

# 날짜 관련
from datetime import datetime, timezone, timedelta

# 데이터 처리 및 시각화
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Google Cloud 관련
from google.cloud import bigquery

# Gemini AI 관련
from google.oauth2 import service_account
from google.genai import Client as GeminiClient
from google.genai.types import GenerateContentConfig
from google.genai import types

# Airflow 관련
from airflow.models import Variable
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

logger = logging.getLogger(__name__)


def get_var(key: str, default: str | None= None) -> str:
    """환경 변수 또는 Airflow Variable 조회"""
    return os.environ.get(key) or Variable.get(key, default_var=default)


# 이메일 설정
SMTP_SERVER = "61.43.45.137"
SMTP_PORT = 25
SENDER_EMAIL = "ds_bi@joycity.com"
SENDER_PASSWORD = get_var("SMTP_PASSWORD")

# gemini 설정
GOOGLE_CLOUD_PROJECT = "datahub-478802"
GOOGLE_CLOUD_LOCATION = "us-central1"  # global
CREDENTIALS_JSON = get_var("GOOGLE_CREDENTIAL_JSON")

# Notion 설정
NOTION_TOKEN = get_var("NOTION_TOKEN")

RUN_ID = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
LABELS = {
    "datascience_division_service": "daily_mkt_mailing",
    "run_id": RUN_ID,
    "datascience_division_service_sub": "mkt_daily_mailing_total",
}  ## 딕셔너리 형태로 붙일 수 있음.
print("RUN_ID=", RUN_ID, "LABEL_ID=", LABELS)


def get_email_list_from_notion(NOTION_TOKEN: str) -> list:
    """Notion 데이터베이스에서 이메일 리스트를 조회합니다."""

    # 수신자 설정 (notion에서 불러오기)
    database_id = "2cbea67a56818058b9c1c5bf0cb3f3a4"
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }

    # 쿼리 전송
    response = requests.post(
        f"https://api.notion.com/v1/databases/{database_id}/query",
        headers=headers,
        json={
            "filter": {
                "or": [
                    {"property": "Project", "select": {"equals": "ALL"}},
                    {"property": "Project", "select": {"equals": "DRSG"}},
                ]
            }
        },
    )

    data = response.json()
    # email 값 추출
    emails = []
    for item in data.get("results", []):
        if "Email" in item["properties"]:
            email_prop = item["properties"]["Email"]
            # email이 rich_text 타입인 경우
            if email_prop.get("rich_text"):
                email_value = "".join(
                    [text["plain_text"] for text in email_prop["rich_text"]]
                )
                emails.append(email_value)

    return emails


RECIPIENT_EMAILS = get_email_list_from_notion(NOTION_TOKEN)
# RECIPIENT_EMAILS = ["seongin@joycity.com", "nayoonkim@joycity.com"]


# client 설정 함수
def get_gcp_credentials():
    """GCP 서비스 계정 인증 정보를 생성합니다."""
    cred_dict = json.loads(CREDENTIALS_JSON)
    # 2. private_key 줄바꿈 문자 처리 (필수 체크)
    if "private_key" in cred_dict:
        # 만약 키 값에 \\n 문자가 그대로 들어있다면 실제 줄바꿈으로 변경
        if "\\n" in cred_dict["private_key"]:
            cred_dict["private_key"] = cred_dict["private_key"].replace("\\n", "\n")

    # 3. 명시적으로 Service Account Credentials 생성 (google.auth.default 아님!)
    credentials = service_account.Credentials.from_service_account_info(
        cred_dict,
        scopes=[
            "https://www.googleapis.com/auth/cloud-platform",  # [필수] Vertex AI 및 대부분의 GCP 서비스 만능 키
            "https://www.googleapis.com/auth/devstorage.read_write",
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return credentials


# 전처리 함수 list
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
            .rename(columns={"ru": f"ru_{col}_base", "cost": f"cost_{col}_base"})
        )

    base_d0 = mature_base("rev_d0")
    base_d1 = mature_base("rev_d1")
    base_d3 = mature_base("rev_d3")
    #  base_d7 = mature_base("rev_d7")

    # merge
    for base in [base_d0, base_d1, base_d3]:  # , base_d7
        agg = agg.merge(base, on=group_cols, how="left")

    # --------------------
    # 3) 단가 KPI
    # --------------------
    agg["CPI"] = agg["cost"] / agg["install"]
    agg["CPRU"] = agg["cost"] / agg["ru"]

    # --------------------
    # 4) LTV (RU mature 기준)
    # --------------------
    agg["D0LTV"] = agg["rev_d0"] / agg["ru_rev_d0_base"]
    agg["D1LTV"] = agg["rev_d1"] / agg["ru_rev_d1_base"]
    agg["D3LTV"] = agg["rev_d3"] / agg["ru_rev_d3_base"]
    # agg["D7LTV"] = agg["rev_d7"] / agg["ru_rev_d7_base"]
    agg["DcumLTV"] = agg["rev_dcum"] / agg["ru"]

    # --------------------
    # 5) ROAS (Cost mature 기준) ⭐ 핵심
    # --------------------
    agg["D0ROAS"] = agg["rev_d0"] / agg["cost_rev_d0_base"]
    agg["D1ROAS"] = agg["rev_d1"] / agg["cost_rev_d1_base"]
    agg["D3ROAS"] = agg["rev_d3"] / agg["cost_rev_d3_base"]
    # agg["D7ROAS"] = agg["rev_d7"] / agg["cost_rev_d7_base"]
    agg["DcumROAS"] = agg["rev_dcum"] / agg["cost"]

    # --------------------
    # 6) Retention (RU mature 기준)
    # --------------------
    agg["D1RET"] = agg["ru_d1"] / agg["ru_rev_d1_base"]
    agg["D3RET"] = agg["ru_d3"] / agg["ru_rev_d3_base"]
    # agg["D7RET"] = agg["ru_d7"] / agg["ru_rev_d7_base"]

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
    metrics=("cost", "install", "ru", "CPI", "CPRU", "D0LTV", "D0ROAS"),
    roas_cols=("D0ROAS",),
):

    # --------------------
    # 1) wide 형태로 변환
    # --------------------
    df_wide = df.set_index(list(index_cols) + [period_col]).unstack(period_col)

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
            row_delta[col] = str(delta.loc[idx, col])

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
                (
                    fmt_delta(v)
                    if d and isinstance(v, float)
                    else fmt_pct(v * 100) if isinstance(v, float) else v
                )
                for v, d in zip(df_final[c], is_delta_row)
            ]
        else:
            # 일반 수치: current/prev는 숫자, delta는 % 변화
            df_fmt[c] = [
                (
                    fmt_delta(v)
                    if d and isinstance(v, float)
                    else fmt_int(v) if isinstance(v, float) else v
                )
                for v, d in zip(df_final[c], is_delta_row)
            ]

    return df_fmt


def build_group_pivot(
    df,
    index_cols=("game_name",),  # 행 그룹 (1차 그룹)
    pivot_col="geo_user_group",  # 열 그룹 (2차 그룹)
    pivot_order=None,  # 열 그룹 순서
    metrics=("cost", "ru", "CPRU", "D0ROAS"),
    aggfunc="first",
    output_pivot_first=True,  # True: [그룹 → 지표]
):

    # --------------------
    # 1) pivot
    # --------------------
    df_pivot = df.pivot_table(
        index=list(index_cols), columns=pivot_col, values=list(metrics), aggfunc=aggfunc
    )
    df_pivot = df_pivot.swaplevel(0, 1, axis=1)

    # --------------------
    # 2) 컬럼 정렬 (metric → pivot)
    # --------------------
    if pivot_order is not None:
        df_pivot = df_pivot.reindex(
            columns=pd.MultiIndex.from_product([pivot_order, metrics])
        )
    return df_pivot


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
    rows = "\n".join("| " + " | ".join(map(str, row)) + " |" for row in df.values)

    return "\n".join([header, rows])


# 숫자 포맷팅 함수
def format_number(value):
    """숫자에 1000단위 쉼표 추가 + HTML 이스케이프 + % 기호 방어"""
    if pd.isna(value):
        return ""

    try:
        # 숫자 타입 확인
        num = float(value)
        # 정수인 경우
        if num == int(num):
            formatted = f"{int(num):,}"
        # 소수점이 있는 경우
        else:
            formatted = f"{num:,.2f}"
    except (ValueError, TypeError, OverflowError):
        formatted = str(value)

    # [핵심 수정 1] 데이터 안에 있는 '%' 문자를 '%%'로 바꿔줍니다.
    # 이렇게 하면 나중에 혹시 % 포맷팅을 쓰더라도 에러가 나지 않습니다.
    formatted = formatted.replace("%", "%%")

    # HTML 엔티티로 변환
    formatted = formatted.replace(".", ".\u200b")
    return html.escape(formatted)


# HTML 테이블 생성 함수 : 튜플 처리 추가
def format_table(df):
    html_table_header = ""

    # [핵심] MultiIndex(2줄 이상 헤더) 처리
    if isinstance(df.columns, pd.MultiIndex):
        levels = df.columns.levels
        codes = df.columns.codes
        n_levels = len(levels)  # 헤더 줄 수 (보통 2줄)

        for level_i in range(n_levels):
            html_table_header += '<tr class="data-title">'

            # 현재 레벨의 컬럼들을 순회
            col_i = 0
            while col_i < len(df.columns):
                # 현재 컬럼의 이름 가져오기
                col_name = df.columns[col_i][level_i]

                # 병합(colspan)할 칸 수 계산
                colspan = 1
                for next_i in range(col_i + 1, len(df.columns)):
                    # 다음 컬럼이 현재 컬럼과 같은 그룹인지 확인
                    # (상위 레벨들이 모두 같고, 현재 레벨 이름도 같아야 함)
                    is_same_group = (
                        df.columns[next_i][: level_i + 1]
                        == df.columns[col_i][: level_i + 1]
                    )
                    if is_same_group:
                        colspan += 1
                    else:
                        break

                # rowspan 계산 (하위 레벨이 비어있거나 동일한 경우 등 - 여기서는 간단히 빈칸 처리)
                # 보통 MultiIndex에서 상위 레벨 이름 출력 후 colspan 적용

                # 안전한 문자열 변환 (% 기호 방어)
                safe_col_name = str(col_name).replace("%", "%%")

                # HTML 생성 (colspan 적용)
                if colspan > 1:
                    html_table_header += f'<td colspan="{colspan}" style="text-align:center; font-weight:bold;">{safe_col_name}</td>'
                else:
                    # 상위 레벨이 비어있지 않거나, 하위 레벨인 경우
                    html_table_header += f'<td style="text-align:center; font-weight:bold;">{safe_col_name}</td>'

                col_i += colspan  # 처리한 만큼 인덱스 점프

            html_table_header += "</tr>"

    # [기존] SingleIndex(1줄 헤더) 처리
    else:
        html_table_header = '<tr class="data-title">'
        for col in df.columns:
            safe_col = str(col).replace("%", "%%")
            html_table_header += f"<td>{safe_col}</td>"
        html_table_header += "</tr>"

    # ---------------------------------------------------------
    # 데이터 행(Body) 생성 부분 (기존과 동일하지만 안전성 강화)
    html_table_rows = ""
    for i, (idx, row) in enumerate(df.iterrows()):
        row_class = "data1" if i % 2 == 0 else "data2"
        html_table_rows += f'<tr class="{row_class}">'
        for cell in row:
            # format_number 함수가 있다고 가정
            cell_value = format_number(cell)
            html_table_rows += f"<td>{cell_value}</td>"
        html_table_rows += "</tr>"

    return html_table_header, html_table_rows


# 텍스트 파싱 함수
def parse_response_text(response_data):
    text = response_data.text
    first_hash_removed = text.replace("#", "", 1)

    return first_hash_removed.replace("#", "<br>\n*")


# 메일 생성 및 발송 함수
def create_graph_send_email(**kwargs):
    """마케팅 프레임워크 Total 정보 생성 및 이메일 발송"""

    # 1. GCP 인증 정보 가져오기
    credentials = get_gcp_credentials()
    client = bigquery.Client(
        project=GOOGLE_CLOUD_PROJECT
        #  , location='US'
        ,
        credentials=credentials,
    )

    ## 1> cohort별 전체 지표
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
                WHEN CountryCode = 'KR' THEN '4.KR'
                WHEN CountryCode = 'US' THEN '1.US'
                WHEN CountryCode = 'JP' THEN '2.JP'
                WHEN CountryCode IN ('UK','FR','DE','GB') THEN '3.WEU'
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

    query_result_base = client.query(
        query, job_config=bigquery.QueryJobConfig(labels=LABELS)
    ).to_dataframe()
    df = query_result_base.copy()

    # 1) 최근 7일 게임 성과
    current = df[df["period_7d"] == "current_7d"]

    group_cols = ["game_name"]
    df_current = aggregate_kpi(current, group_cols)
    col = [
        "game_name",
        "cost",
        "install",
        "ru",
        "CPI",
        "CPRU",
        "D1LTV",
        "D3LTV",
        "DcumLTV",
        "D1RET",
        "D3RET",
        "D1ROAS",
        "D3ROAS",
        "DcumROAS",
    ]
    df_current = df_current[col]

    # 2) 전주대비 금주 Paid 성과 비교
    Paid = df[(df["organic_paid"] == "Paid") & (df["osuser"] == "And")]
    group_cols = ["game_name", "period_7d"]
    df_paid_period = aggregate_kpi(Paid, group_cols)
    df_wow_paid = build_wow_table(
        df=df_paid_period,
        index_cols=("game_name",),
        metrics=(
            "cost",
            "install",
            "ru",
            "CPI",
            "CPRU",
            "D0LTV",
            "D1LTV",
            "D3LTV",
            "D1RET",
            "D3RET",
            "D0ROAS",
            "D1ROAS",
            "D3ROAS",
        ),
        roas_cols=(
            "D1RET",
            "D3RET",
            "D0ROAS",
            "D1ROAS",
            "D3ROAS",
            "DcumROAS",
        ),
    )

    # 3) 국가별 주요 성과 비교
    country_order = ("1.US", "2.JP", "3.WEU", "4.KR", "5.ETC")
    group_cols = ["game_name", "geo_user_group"]
    df_geo_current = aggregate_kpi(current, group_cols)
    df_country = build_group_pivot(
        df=df_geo_current,
        index_cols=("game_name",),
        pivot_col="geo_user_group",
        pivot_order=country_order,
        metrics=("cost", "ru", "CPRU", "D0ROAS"),
    )

    # 4) OS별 주요 성과 비교
    os_order = ("And", "IOS")
    group_cols = ["game_name", "osuser"]
    df_os_current = aggregate_kpi(current, group_cols)
    df_os = build_group_pivot(
        df=df_os_current,
        index_cols=("game_name",),
        pivot_col="osuser",
        pivot_order=os_order,
        metrics=("cost", "ru", "CPRU", "D0ROAS"),
    )

    # OS Cost 비중 계산
    df_os[("Total", "And Cost 비중")] = df_os[("And", "cost")] / (
        df_os[("IOS", "cost")] + df_os[("And", "cost")]
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

    # 5) Paid/Organic 별 주요 성과 비교

    organic_paid = ("Organic", "Paid")
    group_cols = ["game_name", "organic_paid"]
    df_organic_paid_current = aggregate_kpi(current, group_cols)
    df_organic_paid = build_group_pivot(
        df=df_organic_paid_current,
        index_cols=("game_name",),
        pivot_col="organic_paid",
        pivot_order=organic_paid,
        metrics=("ru", "D0LTV", "D1LTV", "D3LTV", "D1RET", "D3RET"),
    )

    # 5) Paid/Organic 별 주요 성과 비교
    organic_paid = ("Organic", "Paid")
    group_cols = ["game_name", "organic_paid"]
    df_organic_paid_current = aggregate_kpi(current, group_cols)
    df_organic_paid = build_group_pivot(
        df=df_organic_paid_current,
        index_cols=("game_name",),
        pivot_col="organic_paid",
        pivot_order=organic_paid,
        metrics=("ru", "D0LTV", "D1LTV", "D3LTV", "D1RET", "D3RET"),
    )
    # df_organic_paid

    # Paid/Organic 별 RU 비중 계산
    df_organic_paid[("Total", "RU Organic 비중")] = df_organic_paid[
        ("Organic", "ru")
    ] / (df_organic_paid[("Paid", "ru")] + df_organic_paid[("Organic", "ru")])

    # % 포맷
    df_organic_paid[("Total", "RU Organic 비중")] = df_organic_paid[
        ("Total", "RU Organic 비중")
    ].apply(lambda x: f"{x*100:.1f}%" if pd.notna(x) else "")
    # 위치 이동
    cols = list(df_organic_paid.columns)
    meta_col = ("Total", "RU Organic 비중")
    cols.insert(0, cols.pop(cols.index(meta_col)))
    df_organic_paid = df_organic_paid[cols]
    # df_organic_paid

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

        """,
    ]

    genai_client = GeminiClient(
        vertexai=True,
        location="global",  # genai 호출용location 변경
        credentials=credentials,
    )

    config_current_optimized = GenerateContentConfig(
        temperature=1.0,
        thinking_config=types.ThinkingConfig(include_thoughts=True),
        system_instruction=SYSTEM_INSTRUCTION,
        labels=LABELS,
    )

    response_current = genai_client.models.generate_content(
        model="gemini-3-pro-preview",  # API 호출
        contents="\n\n".join(prompt_current_final),
        config=config_current_optimized,
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

        """,
    ]

    config_organic_paid_optimized = GenerateContentConfig(
        temperature=1.0,
        thinking_config=types.ThinkingConfig(include_thoughts=True),
        system_instruction=SYSTEM_INSTRUCTION,
        labels=LABELS,
    )

    response_organic_paid = genai_client.models.generate_content(
        model="gemini-3-pro-preview",  # Vertex AI 모델명
        contents="\n\n".join(prompt_organic_paid_final),
        config=config_organic_paid_optimized,
    )

    df_wow_paid_md = df_to_md(df_wow_paid)
    # 3) 전주대비 금주 Paid 성과 비교
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

        """,
    ]

    config_wow_paid_optimized = GenerateContentConfig(
        temperature=1.0,
        thinking_config=types.ThinkingConfig(include_thoughts=True),
        system_instruction=SYSTEM_INSTRUCTION,
        labels=LABELS,
    )

    response_wow_paid = genai_client.models.generate_content(
        model="gemini-3-pro-preview",  # Vertex AI 모델명
        contents="\n\n".join(prompt_wow_paid_final),
        config=config_wow_paid_optimized,
    )

    df_country_md = df_to_md(df_country)
    # 4) 국가별 주요 성과 비교
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

        """,
    ]

    config_country_optimized = GenerateContentConfig(
        temperature=1.0,
        thinking_config=types.ThinkingConfig(include_thoughts=True),
        system_instruction=SYSTEM_INSTRUCTION,
        labels=LABELS,
    )

    response_country = genai_client.models.generate_content(
        model="gemini-3-pro-preview",  # Vertex AI 모델명
        contents="\n\n".join(prompt_country_final),
        config=config_country_optimized,
    )

    df_os_md = df_to_md(df_os)
    # 5) OS 별 주요 성과 비
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

        """,
    ]

    config_os_optimized = GenerateContentConfig(
        temperature=1.0,
        thinking_config=types.ThinkingConfig(include_thoughts=True),
        system_instruction=SYSTEM_INSTRUCTION,
        labels=LABELS,
    )

    response5 = genai_client.models.generate_content(
        model="gemini-3-pro-preview",  # Vertex AI 모델명
        contents="\n\n".join(prompt_os_final),
        config=config_os_optimized,
    )

    # 날짜 가져오기
    kst = timezone(timedelta(hours=9))
    today = datetime.now(kst).date()

    # 표 변환
    html_table_header_current, html_table_rows_current = format_table(
        df_current.reset_index()
    )
    html_table_header_organic_paid, html_table_rows_organic_paid = format_table(
        df_organic_paid.reset_index()
    )
    html_table_header_wow_paid, html_table_rows_wow_paid = format_table(
        df_wow_paid.reset_index()
    )
    html_table_header_country, html_table_rows_country = format_table(
        df_country.reset_index()
    )
    html_table_header_os, html_table_rows_os = format_table(df_os.reset_index())

    response_current_text = parse_response_text(response_current)
    response_organic_paid_text = parse_response_text(response_organic_paid)
    response_wow_paid_text = parse_response_text(response_wow_paid)
    response_country_text = parse_response_text(response_country)
    response5_text = parse_response_text(response5)

    try:
        # 이메일 HTML 본문 생성 (메일 클라이언트 호환성을 위해 인라인 스타일 사용)
        current_time = datetime.now(timezone(timedelta(hours=9))).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        html_body = f"""<!DOCTYPE html>
                    <html lang="ko">
                    <head>
                        <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
                        <meta http-equiv="Content-Script-Type" content="text/javascript">
                        <meta http-equiv="Content-Style-Type" content="text/css">
                        <meta http-equiv="X-UA-Compatible" content="IE=edge">
                        <meta name="robots" content="noindex, nofollow">
                        <title>Joyple UA Performance & Cost Report</title>
                        <style>
                            body {{
                                padding: 10px;
                                margin: 0;
                                width: 100%;
                                font-family: Arial, Verdana, Gulim;
                                font-size: 8pt;
                            }}
                            table {{
                                width: 100%;
                                display: table;
                                border-collapse: collapse;
                            }}
                            tr {{
                                display: table-row;
                                vertical-align: inherit;
                                border-color: inherit;
                            }}
                            tr:nth-child(odd) {{
                                background: #f2f2f2;
                                text-align: right;
                                color: #555555;
                            }}
                            tr:nth-child(even) {{
                                background: white;
                                text-align: right;
                                color: #555555;
                            }}
                            td {{
                                padding: 3px;
                                border: 1px #d6d6d6 solid;
                                text-align: center;
                                color: black;
                                white-space: nowrap;
                            }}
                            tr.data1 td {{
                                background: white;
                                text-align: right;
                                color: #555555;
                            }}
                            tr.data2 td {{
                                background: #f2f2f2;
                                text-align: right;
                                color: #555555;
                            }}
                            tr.data-title td {{
                                background: #eaeaec;
                                text-align: center;
                                color: black;
                                font-weight: bold;
                                border: 1px #d6d6d6 solid;
                            }}
                            .tableTitleNew1 {{
                                padding: 5px;
                                text-align: left;
                                font-weight: bold;
                                font-size: 8pt;
                                background: #707070;
                                color: white;
                                border: 1px #2e2e2e solid !important;
                            }}
                            .tableTitleNewMain {{
                                padding: 5px;
                                text-align: left;
                                font-weight: bold;
                                font-size: 9pt;
                                background: #424242;
                                color: white;
                                border: 1px #2e2e2e solid !important;
                            }}
                            .tableTitleNewgenai {{
                                padding: 5px;
                                text-align: left;
                                font-size: 10pt;
                                background: #E5E5E5;
                                color: black;
                                border: 1px #2e2e2e solid !important;
                            }}
                            .pcenter {{
                                text-align: center !important;
                            }}
                            .pleft {{
                                text-align: left !important;
                            }}
                        </style>
                    </head>
                    <body>
                        <table border="1" width="100%">
                            <tbody>
                                <tr>
                                    <td style="white-space: nowrap" class="tableTitleNewMain">
                                        📊 게임 별 메일링 통합 성과 리포트 :: {current_time} (KST)
                                    </td>
                                </tr>
                            </tbody>
                        </table>

                        <table border="1" width="100%">
                            <tbody>
                                <tr>
                                    <td style="white-space:nowrap" class="tableTitleNewMain">1. 최근 7일간 게임별 성과</td>
                                </tr>
                            </tbody>
                        </table>

                        <table border="1" width="100%">
                            <tbody>
                                {html_table_header_current}
                                {html_table_rows_current}
                            </tbody>
                        </table>

                        <table border="1" width="100%">
                            <tbody>
                                <tr>
                                    <td style="white-space:nowrap" class="tableTitleNewgenai">
                                    {response_current_text}
                                    </td>
                                </tr>
                            </tbody>
                        </table>                            

                        <table border="1" width="100%">
                            <tbody>
                                <tr>
                                    <td style="white-space:nowrap" class="tableTitleNew1">2. Android Paid / Orgainic 성과 비교</td>
                                </tr>
                            </tbody>
                        </table>

                        <table border="1" width="100%">
                            <tbody>
                                {html_table_header_organic_paid}
                                {html_table_rows_organic_paid}
                            </tbody>
                        </table>

                        <table border="1" width="100%">
                            <tbody>
                                <tr>
                                    <td style="white-space:nowrap" class="tableTitleNewgenai">
                                    {response_organic_paid_text}
                                    </td>
                                </tr>
                            </tbody>
                        </table>

                        <table border="1" width="100%">
                            <tbody>
                                <tr>
                                    <td style="white-space:nowrap" class="tableTitleNew1">3. Android Paid 성과 비교 (전주대비 상승 / 하락 트렌드)</td>
                                </tr>
                            </tbody>
                        </table>

                        <table border="1" width="100%">
                            <tbody>
                                {html_table_header_wow_paid}
                                {html_table_rows_wow_paid}
                            </tbody>
                        </table>

                        <table border="1" width="100%">
                            <tbody>
                                <tr>
                                    <td style="white-space:nowrap" class="tableTitleNewgenai">
                                    {response_wow_paid_text}
                                    </td>
                                </tr>
                            </tbody>
                        </table>

                        <table border="1" width="100%">
                            <tbody>
                                <tr>
                                    <td style="white-space:nowrap" class="tableTitleNew1">4. 국가별 성과 비교</td>
                                </tr>
                            </tbody>
                        </table>

                        <table border="1" width="100%">
                            <tbody>
                                {html_table_header_country}
                                {html_table_rows_country}
                            </tbody>
                        </table>
                        <table border="1" width="100%">
                            <tbody>
                                <tr>
                                    <td style="white-space:nowrap" class="tableTitleNewgenai">
                                    {response_country_text}
                                    </td>
                                </tr>
                            </tbody>
                        </table>

                        <table border="1" width="100%">
                            <tbody>
                                <tr>
                                    <td style="white-space:nowrap" class="tableTitleNew1">5. OS 별 성과 비교</td>
                                </tr>
                            </tbody>
                        </table>

                        <table border="1" width="100%">
                            <tbody>
                                {html_table_header_os}
                                {html_table_rows_os}
                            </tbody>
                        </table>

                        <table border="1" width="100%">
                            <tbody>
                                <tr>
                                    <td style="white-space:nowrap" class="tableTitleNewgenai">
                                    {response5_text}
                                    </td>
                                </tr>
                            </tbody>
                        </table>

                        <div style="text-align: center; margin-top: 20px; padding-top: 10px; border-top: 1px solid #ddd; color: #999; font-size: 8pt;">
                            <p>자동 생성된 이메일입니다. 회신하지 마세요.</p>
                        </div>
                    </body>
                    </html>
                    """

        # [수정] 디버깅 로그 추가 및 수신자 확인
        logger.info("📧 이메일 발송 준비 시작...")
        logger.info(f"📋 수신자 목록: {RECIPIENT_EMAILS}")

        if not RECIPIENT_EMAILS:
            logger.error("❌ 수신자가 없습니다. 이메일을 발송하지 않습니다.")
            return

        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=60)
        server.set_debuglevel(1)  # 👈 상세 로그 켜기

        # # 인증이 필요하면
        # if SENDER_PASSWORD:
        #     server.login(SENDER_EMAIL, SENDER_PASSWORD)

        msg = MIMEMultipart()
        msg["From"] = SENDER_EMAIL
        msg["To"] = ", ".join(RECIPIENT_EMAILS)
        msg["Subject"] = f"게임 별 메일링 통합 성과 리포트 {today}"
        msg.attach(MIMEText(html_body, "html"))

        server.sendmail(SENDER_EMAIL, RECIPIENT_EMAILS, msg.as_string())
        server.quit()
        print("메일 발송 성공")

        # msg = MIMEMultipart()
        # msg['From'] = SENDER_EMAIL
        # msg['To'] = ', '.join(RECIPIENT_EMAILS)
        # msg['Subject'] = f"[RESU] UA Performance & Cost Report {today}"
        # msg.attach(MIMEText(html_body, 'html'))

        # with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        #     server.starttls()
        #     # server.login(SENDER_EMAIL, SENDER_PASSWORD)
        #     server.sendmail(SENDER_EMAIL, RECIPIENT_EMAILS, msg.as_string())

        # logger.info(f"✅ 이메일 발송 완료: {RECIPIENT_EMAILS}")
        # return True

    except Exception as e:
        logger.error(f"❌ 에러 발생: {str(e)}", exc_info=True)
        raise

    # 이메일 본문 조합


# DAG 기본 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=15),
}

with DAG(
    dag_id="Marketing_Mailing_ALL_Project",
    default_args=default_args,
    description="전체 프로젝트 마케팅 메일링",
    schedule="10 5 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["marketing", "mailing", "Total"],
) as dag:

    create_graph_send_email_task = PythonOperator(
        task_id="create_graph_send_email",
        python_callable=create_graph_send_email,
        dag=dag,
    )
