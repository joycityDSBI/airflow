import requests
import pandas as pd
from google.cloud import bigquery
import os
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule



# ===== 설정 =====
def get_config(key: str, default: str = None) -> str:
    """환경 변수 또는 Airflow Variable에서 설정값 가져오기"""
    env_value = os.environ.get(key)
    if env_value:
        return env_value
    
    try:
        return Variable.get(key, default_var=default)
    except TypeError:
        return Variable.get(key, default)
    except KeyError:
        return default


# 전역 설정값
NOTION_TOKEN = get_config("NOTION_TOKEN")
DBID = get_config("NOTION_DBID")
PROJECT_ID = get_config("PROJECT_ID")
EMAIL_TO = get_config("EMAIL_TO", "your-email@example.com")
DATASET_ID = "Account_Info"
TABLE_ID = "RESU_account_info"
NOTION_API_VERSION = get_config("NOTION_API_VERSION", "2022-06-28")
FULL_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# SMTP 설정
SMTP_HOST = get_config("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(get_config("SMTP_PORT", "587"))
SMTP_USER = get_config("SMTP_USER")
SMTP_PASSWORD = get_config("SMTP_PASSWORD")
EMAIL_FROM = get_config("EMAIL_FROM", SMTP_USER)


# ===== 유틸 함수 =====
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

def extract_property_value(value):
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
    


# ===== Task 함수 =====
def query_notion_database(**context):
    """Notion 데이터베이스에서 데이터 조회"""
    url = f"https://api.notion.com/v1/databases/{DBID}/query"
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_API_VERSION,
        "Content-Type": "application/json"
    }

    results = []
    has_more = True
    next_cursor = None

    logging.info(f"🔍 Notion 데이터베이스 조회 시작: {DBID}")
    
    while has_more:
        payload = {"start_cursor": next_cursor} if next_cursor else {}
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
        results.extend(data["results"])
        has_more = data.get("has_more", False)
        next_cursor = data.get("next_cursor")

    logging.info(f"✅ Notion 데이터 조회 완료: {len(results)}개 행")
    
    context['task_instance'].xcom_push(key='notion_raw_data', value=results)
    return len(results)


def parse_and_transform_data(**context):
    """Notion 데이터 파싱 및 변환"""
    ti = context['task_instance']
    rows = ti.xcom_pull(task_ids='query_notion_database', key='notion_raw_data')
    
    logging.info(f"📊 데이터 파싱 시작: {len(rows)}개 행")
    
    # 데이터 파싱
    parsed = []
    for row in rows:
        row_data = {"notion_page_id": row.get("id")}
        props = row.get("properties", {})
        for key, value in props.items():
            row_data[key] = extract_property_value(value)
        parsed.append(row_data)
    
    # DataFrame 생성 및 변환
    df = pd.DataFrame(parsed)
    df.columns = df.columns.str.strip()
    df = df[['Userkey', 'UserID', '구분']]
    df = df.assign(build='RESU').rename(
        columns={'Userkey': 'userkey', 'UserID': 'charid', '구분': 'class'}
    )
    
    logging.info(f"✅ 데이터 변환 완료: {len(df)}개 행, {len(df.columns)}개 컬럼")
    
    ti.xcom_push(key='transformed_data', value=df.to_dict('records'))
    return len(df)

def upload_to_bigquery(**context):
    """BigQuery에 데이터 업로드"""
    ti = context['task_instance']
    data_dict = ti.xcom_pull(task_ids='parse_and_transform_data', key='transformed_data')
    df = pd.DataFrame(data_dict)
    
    logging.info(f"📤 BigQuery 업로드 시작: {len(df)}개 행")
    
    client = bigquery.Client(project=PROJECT_ID)
    job = client.load_table_from_dataframe(
        dataframe=df,
        destination=FULL_TABLE_ID,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True
        )
    )
    job.result()
    
    logging.info(f"✅ BigQuery 테이블 업로드 완료: {FULL_TABLE_ID}")
    
    ti.xcom_push(key='result', value={'table': FULL_TABLE_ID, 'rows': len(df)})
    return len(df)
    
def prepare_email(**context):
    """이메일 내용 준비"""
    ti = context['task_instance']
    dag_run = context['dag_run']
    
    try:
        query_cnt = ti.xcom_pull(task_ids='query_notion_database')
        transform_cnt = ti.xcom_pull(task_ids='parse_and_transform_data')
        result = ti.xcom_pull(task_ids='upload_to_bigquery', key='result')
        
        # 모든 값이 정상적으로 조회되었는지 확인
        if query_cnt is None or transform_cnt is None or result is None:
            raise ValueError("이전 Task에서 데이터를 가져올 수 없습니다.")
        
        data = {
            '실행 시간': context['logical_date'].strftime('%Y-%m-%d %H:%M:%S'),
            'DAG ID': dag_run.dag_id,
            'Notion 조회 행 수': f"{query_cnt}개",
            '변환 후 행 수': f"{transform_cnt}개",
            'BigQuery 적재 행 수': f"{result['rows']}개",
            'BigQuery 테이블': result['table'],
            '상태': '✅ 성공'
        }
        html = create_email_html('Notion Master List 동기화 성공', '#4CAF50', data)
        subject = '✅ [Airflow] Notion Master List 동기화 성공'
        
    except Exception as e:
        # 에러 발생 시 실패 이메일 생성
        logging.error(f"이메일 준비 중 에러 발생: {str(e)}")
        data = {
            '실행 시간': context['logical_date'].strftime('%Y-%m-%d %H:%M:%S'),
            'DAG ID': dag_run.dag_id,
            '상태': '❌ 실패',
            '에러 내용': str(e)
        }
        html = create_email_html('Notion Master List 동기화 실패', '#f44336', data)
        subject = '❌ [Airflow] Notion Master List 동기화 실패'
    
    ti.xcom_push(key='email_html', value=html)
    ti.xcom_push(key='email_subject', value=subject)


def send_email_via_smtp(**context):
    """SMTP를 통한 이메일 발송"""
    ti = context['task_instance']
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
    'owner': 'data-team',
    'depends_on_past': False,
    'email': [EMAIL_TO],
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}
with DAG(
    dag_id='notion_master_list',
    default_args=default_args,
    description='Notion 데이터베이스에서 데이터를 추출하여 BigQuery로 적재하는 ETL 파이프라인',
    schedule='0 1 * * *',  # 매일 새벽 1시 실행
    start_date=datetime(2025, 10, 21),
    catchup=False,
    tags=['bigquery', 'notion', 'metadata', 'etl'],
) as dag:

    # ETL Tasks
    query_task = PythonOperator(
        task_id='query_notion_database',
        python_callable=query_notion_database,
    )
    
    transform_task = PythonOperator(
        task_id='parse_and_transform_data',
        python_callable=parse_and_transform_data,
    )
    
    load_task = PythonOperator(
        task_id='upload_to_bigquery',
        python_callable=upload_to_bigquery,
    )
    
    # Email Tasks
    prepare_email_task = PythonOperator(
        task_id='prepare_email',
        python_callable=prepare_email,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    
    send_email_task = PythonOperator(
        task_id='send_email',
        python_callable=send_email_via_smtp,
    )
    
    # Task 의존성
    query_task >> transform_task >> load_task
    [query_task, transform_task, load_task] >> prepare_email_task >> send_email_task