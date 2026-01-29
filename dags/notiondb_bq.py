import requests
import pandas as pd
import numpy as np
from google.cloud import bigquery
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
import os
import logging
import json
from google.oauth2 import service_account

# ===== 설정 =====
def get_var(key: str, default: str = None) -> str:
    """환경 변수 또는 Airflow Variable 조회"""
    return os.environ.get(key) or Variable.get(key, default_var=default)

# 전역 설정값
NOTION_TOKEN = get_var("NOTION_TOKEN")
PROJECT_ID = "datahub-478802"

EMAIL_TO = get_var("EMAIL_TO", "65e43b85.joycity.com@kr.teams.ms")
DBID = "23bea67a5681803db3c4f691c143a43d"
TABLE_ID = f"data-science-division-216308.PackageInfo.PackageInfo_Notion_RESU"

# SMTP 설정
SMTP_HOST = get_var("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(get_var("SMTP_PORT", "587"))
SMTP_USER = get_var("SMTP_USER")
SMTP_PASSWORD = get_var("SMTP_PASSWORD")
EMAIL_FROM = get_var("EMAIL_FROM", SMTP_USER)

print(f"📧 이메일 발송 설정: HOST={SMTP_HOST}, PORT={SMTP_PORT}, USER={SMTP_USER}, FROM={EMAIL_FROM}, TO={EMAIL_TO}, PW={SMTP_PASSWORD}")
# ===== 유틸 함수 =====
def extract_property(value):
    """Notion 속성값 추출"""
    prop_type = value["type"]
    mapping = {
        "title": lambda v: v.get("title", [{}])[0].get("text", {}).get("content"),
        "rich_text": lambda v: v.get("rich_text", [{}])[0].get("text", {}).get("content"),
        "select": lambda v: v.get("select", {}).get("name"),
        "multi_select": lambda v: [m["name"] for m in v.get("multi_select", [])],
        "status": lambda v: v.get("status", {}).get("name"),
        "date": lambda v: v.get("date", {}).get("start"),
        "checkbox": lambda v: v.get("checkbox"),
        "number": lambda v: v.get("number"),
    }
    try:
        return mapping.get(prop_type, lambda v: None)(value)
    except:
        return None

def create_email_html(title, color, data):
    """이메일 HTML 생성"""
    rows = "".join([f"<tr><td style='border: 1px solid #ddd; padding: 8px;'>{k}</td><td style='border: 1px solid #ddd; padding: 8px;'>{v}</td></tr>" for k, v in data.items()])
    return f"""
    <html>
        <body style="font-family: Arial, sans-serif;">
            <div style="background-color: {color}; color: white; padding: 20px; text-align: center;">
                <h1>{title}</h1>
            </div>
            <div style="padding: 20px;">
                <table style="width: 100%; border-collapse: collapse;">
                    <tr style="background-color: {color}; color: white;">
                        <th style="border: 1px solid #ddd; padding: 8px;">항목</th>
                        <th style="border: 1px solid #ddd; padding: 8px;">값</th>
                    </tr>
                    {rows}
                </table>
            </div>
        </body>
    </html>
    """


# ===== Task 함수 =====
def extract_notion_data(**context):
    """Notion 데이터 추출"""
    url = f"https://api.notion.com/v1/databases/{DBID}/query"
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": get_var("NOTION_API_VERSION", "2022-06-28"),
        "Content-Type": "application/json"
    }
    
    results = []
    next_cursor = None
    
    while True:
        payload = {"start_cursor": next_cursor} if next_cursor else {}
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
        results.extend(data["results"])
        if not data.get("has_more"):
            break
        next_cursor = data.get("next_cursor")
    
    context['ti'].xcom_push(key='raw_data', value=results)
    return len(results)



def transform_data(**context):
    """데이터 변환"""

    rows = context['ti'].xcom_pull(task_ids='extract_notion_data', key='raw_data')
    
    # 파싱
    parsed = []
    for row in rows:
        data = {"notion_page_id": row.get("id")}
        for key, value in row.get("properties", {}).items():
            data[key] = extract_property(value)
        if data.get("상품카인드"):
            parsed.append(data)
    
    df = pd.DataFrame(parsed)

    # DataFrame 변환
    df = df[df['상태'] == '컨펌 완료']
    
    # NaN 안전 결합(양쪽 값이 있을 때만 '_' 삽입)
    s1 = df['ShopBaseKind'].astype(str).str.strip()
    s2 = df['상품카인드'].astype(str).str.strip()

    # 'nan' 문자열 방지: 원래 NaN은 빈 문자열로
    s1 = s1.replace('nan', '', regex=False)
    s2 = s2.replace('nan', '', regex=False)

    df['PackageKind'] = np.where((s1 != '') & (s2 != ''), s1 + '_' + s2, s1 + s2)
    
    # Checkpoint: print loaded JSON
    print(df['PackageKind'].head())
    
    # 컬럼 정리
    df = df.drop(columns=['ShopBaseKind', '상품카인드'], errors='ignore')

    cols = ['상품명', '상품명_영문', 'PackageKind', '재화구분', 'IAP_CODE_GOOGLE', 
            'IAP_CODE_APPLE', 'IAP_CODE_ONESTORE', '상점 카테고리', '상품 카테고리', 
            '가격 (￦)', '판매 시작일', '판매 종료일', '상태']
    df = df[cols]
    
    # 컬럼명 변경
    df.columns = ['Package_Name', 'Package_Name_ENG', 'Package_Kind', 'Goods_Type',
                  'IAP_CODE_GOOGLE', 'IAP_CODE_APPLE', 'IAP_CODE_ONESTORE',
                  'Cat_Shop', 'Cat_Package', 'Price', 'Start_Date', 'End_Date', 'Task_State']
    
    # 날짜 변환
    df['Start_Date'] = pd.to_datetime(df['Start_Date'], errors='coerce').dt.date
    df['End_Date'] = pd.to_datetime(df['End_Date'], errors='coerce').dt.date
    
    try:
        credentials_json = Variable.get('GOOGLE_CREDENTIAL_JSON')
        cred_dict = json.loads(credentials_json)
        
        # 2. private_key 줄바꿈 문자 처리
        if 'private_key' in cred_dict:
            if '\\n' in cred_dict['private_key']:
                cred_dict['private_key'] = cred_dict['private_key'].replace('\\n', '\n')

        # 3. Credentials 객체 생성
        credentials = service_account.Credentials.from_service_account_info(
            cred_dict,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        
        # 4. Client 생성 시 credentials 전달 (여기가 핵심!)
        client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
        
    except Exception as e:
        print(f"❌ 인증 설정 실패: {e}")
        raise e
    
    job = client.load_table_from_dataframe(
        df, TABLE_ID,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
    )
    job.result()
    
    context['ti'].xcom_push(key='result', value={'table': TABLE_ID, 'rows': len(df)})
    return len(df)

def prepare_email(**context):
    """이메일 내용 준비"""
    ti = context['ti']
    
    try:
        extract_cnt = ti.xcom_pull(task_ids='extract_notion_data')
        transform_result = ti.xcom_pull(task_ids='transform_data', key='result')
        
        # 모든 값이 정상적으로 조회되었는지 확인
        if extract_cnt is None or transform_result is None:
            raise ValueError("이전 Task에서 데이터를 가져올 수 없습니다.")
        
        data = {
            '실행 시간': context['logical_date'].strftime('%Y-%m-%d %H:%M:%S'),
            '추출 행 수': f"{extract_cnt}개",
            '변환 및 적재 행 수': f"{transform_result['rows']}개",
            'BigQuery 테이블': transform_result['table'],
            '상태': '✅ 성공'
        }
        html = create_email_html('Notion to BigQuery 동기화 성공', '#4CAF50', data)
        subject = '✅ [Airflow] Notion to BigQuery 동기화 성공'
        
    except Exception as e:
        # 에러 발생 시 실패 이메일 생성
        logging.error(f"이메일 준비 중 에러 발생: {str(e)}")
        data = {
            '실행 시간': context['logical_date'].strftime('%Y-%m-%d %H:%M:%S'),
            '상태': '❌ 실패',
            '에러 내용': str(e)
        }
        html = create_email_html('Notion to BigQuery 동기화 실패', '#f44336', data)
        subject = '❌ [Airflow] Notion to BigQuery 동기화 실패'
    
    ti.xcom_push(key='email_html', value=html)
    ti.xcom_push(key='email_subject', value=subject)

def send_email_via_smtp(**context):
    """SMTP를 통한 이메일 발송"""
    ti = context['ti']
    subject = ti.xcom_pull(task_ids='prepare_email', key='email_subject')
    html_content = ti.xcom_pull(task_ids='prepare_email', key='email_html')
    
    # 이메일 메시지 생성
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = EMAIL_FROM
    msg['To'] = EMAIL_TO
    
    # HTML 내용 추가
    html_part = MIMEText(html_content, 'html', 'utf-8')
    msg.attach(html_part)
    
    try:
        # SMTP 서버 연결 및 이메일 발송
        logging.info(f"📧 SMTP 서버 연결 시도: {SMTP_HOST}:{SMTP_PORT}")
        
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()  # TLS 암호화 시작
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)
        
        logging.info(f"✅ 이메일 발송 완료: {EMAIL_TO}")
        return f"Email sent to {EMAIL_TO}"
        
    except Exception as e:
        logging.error(f"❌ 이메일 발송 실패: {str(e)}")
        raise


# ===== DAG 정의 =====
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [EMAIL_TO],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='notiondb_bq',
    default_args=default_args,
    description='Notion 데이터베이스를 BigQuery로 동기화',
    schedule='0 21 * * *',  # 매일 새벽 6시 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['notion', 'bigquery', 'etl'],
) as dag:
    
    extract = PythonOperator(
        task_id='extract_notion_data', 
        python_callable=extract_notion_data
    )
    
    transform = PythonOperator(
        task_id='transform_data', 
        python_callable=transform_data
    )
    
    # prepare_email_task = PythonOperator(
    #     task_id='prepare_email',
    #     python_callable=prepare_email,
    #     trigger_rule=TriggerRule.ALL_DONE
    # )
    
    # send_email_task = PythonOperator(
    #     task_id='send_email',
    #     python_callable=send_email_via_smtp,
    # )
    
    # Task 의존성: ETL 파이프라인 후 이메일 발송
    extract >> transform
    # [extract, transform] >> prepare_email_task >> send_email_task