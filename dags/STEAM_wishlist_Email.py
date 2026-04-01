from datetime import datetime, timedelta, timezone
import requests
import pandas as pd
import io
import urllib.parse
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models import Variable
import os

import logging
from google.cloud import bigquery
import json
from google.oauth2 import service_account
import time
from typing import Any, cast
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import re
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

def get_var(key: str, default: str = 'default') -> str:
    """환경 변수 또는 Airflow Variable 조회"""
    return os.environ.get(key) or Variable.get(key, default_var=default)

logger = logging.getLogger(__name__)


PROJECT_ID = 'datahub-478802'
LOCATION = 'US'

BQ_DATASET_ID = 'external_data'
BQ_TABLE_ID = 'steam_wishlist_region'
CREDENTIALS_JSON = get_var('GOOGLE_CREDENTIAL_JSON')

def init_clients():
    """Task 내부에서 실행되어 필요한 클라이언트들을 생성하여 반환합니다."""
    creds = get_gcp_credentials()
    
    # 1. GCP Clients
    bq_client = bigquery.Client(project=PROJECT_ID, credentials=creds)
    
    return {
        "bq_client": bq_client
    }

def get_gcp_credentials():
    """Airflow Variable에서 GCP 자격 증명을 로드합니다."""
    credentials_json = Variable.get('GOOGLE_CREDENTIAL_JSON')
    cred_dict = json.loads(credentials_json)
    if 'private_key' in cred_dict:
        cred_dict['private_key'] = cred_dict['private_key'].replace('\\n', '\n')
    
    # [수정] 스코프(Scopes)를 명시적으로 여러 개 추가합니다.
    SCOPES = [
        "https://www.googleapis.com/auth/cloud-platform",       # 기본 전체 권한
        "https://www.googleapis.com/auth/bigquery"             # BigQuery 권한
    ]
    
    return service_account.Credentials.from_service_account_info(
        cred_dict,
        scopes=SCOPES
    )

def extract_bigquery_data():
    client = init_clients()["bq_client"]

    query = """
        select a.datekey, b.`일자별 위시리스트 수`, a.`누적 위시리스트 수`
        from
        (
        SELECT datekey, SUM(sum(adds)) OVER(order by datekey asc) AS `누적 위시리스트 수`
        FROM `datahub-478802.external_data.steam_wishlist_region` 
        group by datekey
        ) a
        inner join 
        (
        SELECT datekey, sum(adds) AS `일자별 위시리스트 수`
        FROM `datahub-478802.external_data.steam_wishlist_region` 
        where datekey >= DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 14 DAY)
        group by datekey
        ) b
        on a.datekey = b.datekey
        order by a.datekey asc
    """

    df = client.query(
        query, job_config=bigquery.QueryJobConfig()
    ).to_dataframe()

    return df

def region_bigquery_data():
    client = init_clients()["bq_client"]

    query = """
        select region, sum(adds) AS wishlist_cnt
        from `datahub-478802.external_data.steam_wishlist_region` 
        group by region
    """

    df = client.query(
        query, job_config=bigquery.QueryJobConfig()
    ).to_dataframe()

    return df



def send_wishlist_email():
    """extract_bigquery_data + region_bigquery_data 결과를 차트로 그려 이메일로 전송합니다."""
    df = extract_bigquery_data()
    df['datekey'] = pd.to_datetime(df['datekey'])
    df = df.sort_values('datekey')

    df_region = region_bigquery_data()
    df_region = df_region.sort_values('wishlist_cnt', ascending=False).reset_index(drop=True)

    # --- 차트 생성: 라인 차트(위) + 파이 차트(아래) ---
    fig = plt.figure(figsize=(12, 15))

    color_daily = "#23C479"
    color_cumul = "#FA792F"

    # 라인 차트
    ax1 = fig.add_subplot(2, 1, 1)
    ax1.set_xlabel('Date', fontsize=10)
    ax1.set_ylabel('Daily Wishlists', color=color_daily, fontsize=11)
    line1, = ax1.plot(df['datekey'], df['일자별 위시리스트 수'],
                      color=color_daily, marker='o', linewidth=2, label='Daily Wishlists')
    ax1.tick_params(axis='y', labelcolor=color_daily)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
    ax1.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    plt.setp(ax1.get_xticklabels(), rotation=45)

    ax2 = ax1.twinx()
    ax2.set_ylabel('Cumulative Wishlists', color=color_cumul, fontsize=10)
    line2, = ax2.plot(df['datekey'], df['누적 위시리스트 수'],
                      color=color_cumul, marker='s', linewidth=2, linestyle='--', label='Cumulative Wishlists')
    ax2.tick_params(axis='y', labelcolor=color_cumul)
    ax1.legend(handles=[line1, line2], loc='upper left', fontsize=10)
    ax1.set_title('STEAM Wishlist (Last 14 Days)', fontsize=14, pad=15)

    # 파이 차트 - 상위 10개 지역, 나머지는 Others로 합산
    TOP_N = 10
    if len(df_region) > TOP_N:
        top = df_region.head(TOP_N).copy()
        others_val = df_region.iloc[TOP_N:]['wishlist_cnt'].sum()
        others_row = pd.DataFrame([{'region': 'Others', 'wishlist_cnt': others_val}])
        pie_df = pd.concat([top, others_row], ignore_index=True)
    else:
        pie_df = df_region.copy()

    ax_pie = fig.add_subplot(2, 1, 2)
    _, _, autotexts = ax_pie.pie(
        pie_df['wishlist_cnt'].tolist(),
        labels=pie_df['region'].tolist(),
        autopct='%1.1f%%',
        startangle=90,
        pctdistance=0.8,
    )
    for at in autotexts:
        at.set_fontsize(8)
    ax_pie.set_title('Wishlist by Region (Cumulative)', fontsize=14, pad=15)

    fig.tight_layout(pad=3.0)

    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=150)
    buf.seek(0)
    plt.close(fig)

    # --- 이메일 본문 ---
    latest = df.iloc[-1]
    date_str = latest['datekey'].strftime('%Y-%m-%d')
    daily_val = int(latest['일자별 위시리스트 수'])
    cumul_val = int(latest['누적 위시리스트 수'])

    region_rows = ''.join(
        f"<tr><td style='padding:3px 12px;'>{row['region']}</td>"
        f"<td style='padding:3px 12px; text-align:right;'>{int(row['wishlist_cnt']):,}</td></tr>"
        for _, row in df_region.iterrows()
    )

    html_body = f"""
    <html><body>
    <h2>STEAM 위시리스트 리포트</h2>
    <p>기준일: <b>{date_str}</b></p>
    <ul>
      <li>일자별 위시리스트 수: <b>{daily_val:,}</b></li>
      <li>누적 위시리스트 수: <b>{cumul_val:,}</b></li>
    </ul>
    <h3>지역별 누적 위시리스트 수</h3>
    <table border="1" cellspacing="0" cellpadding="0" style="border-collapse:collapse;">
      <tr style="background:#f0f0f0;">
        <th style="padding:4px 12px;">Region</th>
        <th style="padding:4px 12px;">Cumulative Wishlists</th>
      </tr>
      {region_rows}
    </table>
    <br>
    <img src="cid:wishlist_chart">
    <a href="https://www.notion.so/joycity/32cea67a568180ce990ae74e85de7d3d?v=32cea67a568180d7beaa000cb0525e8f"
       style="display:inline-block; padding:10px 24px; background-color:#4C72B0; color:#ffffff;
              text-decoration:none; border-radius:6px; font-size:14px; font-weight:bold;">
      자세히 보기
    </a>
    </body></html>
    """

    # --- 이메일 전송 ---
    smtp_host = "61.43.45.137"
    smtp_port = 25
    smtp_user = "ds_bi@joycity.com"
    smtp_password = get_var("SMTP_PASSWORD")
    email_to = 'fc748c69.joycity.com@kr.teams.ms'

    msg = MIMEMultipart('related')
    msg['Subject'] = f'[STEAM 위시리스트] {date_str} 리포트'
    msg['From'] = smtp_user
    msg['To'] = email_to

    msg.attach(MIMEText(html_body, 'html'))

    img = MIMEImage(buf.read(), name='wishlist_chart.png')
    img.add_header('Content-ID', '<wishlist_chart>')
    img.add_header('Content-Disposition', 'inline', filename='wishlist_chart.png')
    msg.attach(img)

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.sendmail(smtp_user, email_to.split(','), msg.as_string())

    logger.info("위시리스트 리포트 이메일 전송 완료: %s", email_to)




# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
}

with DAG(
    dag_id='steam_wishlist_email_send',
    default_args=default_args,
    description='스팀 위시리스트 이메일 발송',
    schedule= '50 23 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['steam', 'wishlist', 'bigquery'],
) as dag:


    send_wishlist_email_task = PythonOperator(
        task_id='send_wishlist_email_task',
        python_callable=send_wishlist_email,
    )


send_wishlist_email_task