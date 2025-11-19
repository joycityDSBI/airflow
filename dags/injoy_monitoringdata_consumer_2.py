from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import time
import os
from databricks import sql

# Airflow Variable import (버전 호환성 처리)
try:
    from airflow.sdk import Variable
except ImportError:
    from airflow.models import Variable


# ============================================================
# 기본 설정
# ============================================================
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}


# ============================================================
# 유틸리티 함수
# ============================================================
def get_var(key: str, default: str = None, required: bool = False) -> str:
    """환경 변수 → Airflow Variable 순서로 조회"""
    env_value = os.environ.get(key)
    if env_value:
        print(f"✓ 환경 변수에서 {key} 로드됨")
        return env_value
    
    try:
        try:
            var_value = Variable.get(key, default=None)
        except TypeError:
            var_value = Variable.get(key, default_var=None)
        
        if var_value:
            print(f"✓ Airflow Variable에서 {key} 로드됨")
            return var_value
    except Exception as e:
        print(f"⚠️  Variable.get({key}) 오류: {str(e)}")
    
    if default is not None:
        print(f"ℹ️  기본값으로 {key} 설정됨: {default}")
        return default
    
    if required:
        raise ValueError(f"필수 설정 {key}을(를) 찾을 수 없습니다.")
    
    print(f"ℹ️  {key} 값을 찾을 수 없습니다 (선택사항)")
    return None

NOTION_DATABASE_ID = get_var('NOTION_DB_ID_INJOY_MONITORINGDATA_CONSUMER', "230ea67a568180c591fee27d4e90e001")

def get_notion_headers():
    """Notion API 헤더 생성"""
    return {
        "Authorization": f"Bearer {get_var('NOTION_TOKEN', required=True)}",
        "Notion-Version": get_var("NOTION_API_VERSION", "2022-06-28"),
        "Content-Type": "application/json"
    }


def generate_row_key(row: dict) -> str:
    """고유 키 생성: 사용자_스페이스id_대화id_메시지id"""
    user = str(row.get('사용자', '')).strip()
    space_id = str(row.get('스페이스id', '')).strip()
    convo_id = str(row.get('대화id', '')).strip()
    msg_id = str(row.get('메시지id', '')).strip()
    
    return f"{user}_{space_id}_{convo_id}_{msg_id}"


def build_properties_payload(row_data: dict) -> dict:
    """DataFrame 행을 Notion API properties로 변환"""
    properties = {}

    for key, value in row_data.items():
        if pd.isna(value):
            continue

        if key == "사용자 질의":
            content = str(value or "")
            if len(content) > 2000:
                print(f"  -> ⚠️ 경고: 제목 필드는 2000자로 제한됩니다.")
                content = content[:2000]
            properties[key] = {"title": [{"text": {"content": content}}]}

        elif key == "질문날짜":
            properties[key] = {"date": {"start": str(value or "")}}

        elif key == "응답속도(초)":
            try:
                numeric_value = float(value)
                properties[key] = {"number": numeric_value}
            except (ValueError, TypeError):
                continue
        
        elif key == "쿼리":
            content = str(value or "")
            if len(content) > 2000:
                print(f"  -> ✨ '{key}'의 긴 텍스트({len(content)}자)를 분할하여 저장합니다.")
                text_chunks = []
                for i in range(0, len(content), 2000):
                    chunk = content[i:i + 2000]
                    text_chunks.append({"type": "text", "text": {"content": chunk}})
                properties[key] = {"rich_text": text_chunks}
            else:
                properties[key] = {"rich_text": [{"text": {"content": content}}]}

        elif key in ["status", "error_type", "feedback_rating"]:
            properties[key] = {"select": {"name": str(value or "")}}

        elif key in ['row_count', 'auth_regenerate_count']:
            try:
                int_value = int(value)
                properties[key] = {"number": int_value}
            except (ValueError, TypeError):
                continue

        else:
            properties[key] = {"rich_text": [{"text": {"content": str(value or "")}}]}
            
    return properties


def get_all_notion_pages(database_id: str, headers: dict) -> list:
    """Notion DB의 모든 페이지 조회 (페이지네이션 처리)"""
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    results = []
    has_more = True
    next_cursor = None
    
    print("⏳ Notion DB에서 모든 페이지 조회를 시작합니다...")
    while has_more:
        payload = {"page_size": 100}
        if next_cursor:
            payload["start_cursor"] = next_cursor
        
        try:
            res = requests.post(url, headers=headers, json=payload)
            res.raise_for_status()
            data = res.json()
            results.extend(data.get("results", []))
            next_cursor = data.get("next_cursor")
            has_more = data.get("has_more", False)
        except requests.exceptions.RequestException as e:
            print(f"❌ Notion API 에러 발생: {e}")
            break
        time.sleep(0.3)
        
    print(f"✅ Notion DB에서 총 {len(results)}개의 페이지를 조회했습니다.")
    return results


# ============================================================
# Task 함수들
# ============================================================
def extract_data(**context):
    """Databricks에서 데이터 추출"""
    print("=" * 50)
    print("Step 1: Databricks에서 데이터 조회를 시작합니다.")
    
    # Databricks 연결 정보 가져오기
    DATABRICKS_SERVER_HOSTNAME = get_var('DATABRICKS_SERVER_HOSTNAME', required=True)
    DATABRICKS_HTTP_PATH = get_var('databricks_http_path', required=True)
    DATABRICKS_TOKEN = get_var('databricks_token', required=True)
    
    try:
        # Databricks SQL 연결
        print("🔗 Databricks SQL에 연결 중...")
        connection = sql.connect(
            server_hostname=DATABRICKS_SERVER_HOSTNAME,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_TOKEN
        )
        
        sql_query = """
            SELECT
                content,
                user_email,
                space_name,
                space_id,
                conversation_id,
                message_id,
                query,
                message_response_duration_seconds,
                event_time_kst as event_time_kst,
                row_count,
                status,
                description,
                question,
                auth_regenerate_count,
                error,
                error_type,
                feedback_rating
            FROM 
                datahub.injoy_ops_schema.injoy_monitoring_data
            ORDER BY conversation_id, event_time_kst
        """
        
        print("📊 SQL 쿼리 실행 중...")
        cursor = connection.cursor()
        cursor.execute(sql_query)
        
        # 결과를 DataFrame으로 변환
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        source_df = pd.DataFrame(rows, columns=columns)
        
        # 하드코딩으로 유저 제외
        exclude_emails = ['heegle@joycity.com', 'kimjack415@joycity.com']
        source_df = source_df[~source_df['user_email'].isin(exclude_emails)]

        cursor.close()
        connection.close()
        
        print(f"✅ Databricks에서 총 {len(source_df)}개의 데이터를 조회했습니다.")
        email_list = source_df['user_email'].tolist()
        
        for email in email_list:
            print(f"💡 eeeeeee Email 리스트: {email}")

        
    except ImportError:
        print("❌ databricks-sql-connector 라이브러리가 설치되지 않았습니다.")
        print("💡 다음 명령어로 설치하세요: pip install databricks-sql-connector")
        raise
    except Exception as e:
        print(f"❌ Databricks 연결 또는 쿼리 실행 중 오류 발생: {str(e)}")
        raise
    
    print("=" * 50)
    
    # XCom으로 데이터 전달
    context['ti'].xcom_push(key='source_data', value=source_df.to_json(orient='records', date_format='iso'))


def transform_data(**context):
    """데이터 전처리 및 변환"""
    print("\n" + "=" * 50)
    print("Step 2: 데이터 전처리를 시작합니다.")
    
    # XCom에서 데이터 가져오기
    source_data_json = context['ti'].xcom_pull(key='source_data', task_ids='extract_data')
    source_df = pd.read_json(source_data_json, orient='records')
    
    # Notion DB 컬럼명에 맞게 변경
    df_renamed = source_df.rename(columns={
        "content": "사용자 질의",
        "user_email": "사용자",
        "space_name": "스페이스명",
        "space_id": "스페이스id",
        "conversation_id": "대화id",
        "message_id": "메시지id",
        "query": "쿼리",
        "message_response_duration_seconds": "응답속도(초)",
        "event_time_kst": "질문날짜"
    })
    print("🔄 컬럼명을 Notion DB에 맞게 변경했습니다.")
    
    # 날짜 형식 변환
    s = pd.to_datetime(df_renamed['질문날짜'], errors='coerce')
    
    # 타임존 처리: 이미 타임존이 있는지 확인 후 처리
    if s.dt.tz is None:
        # 타임존이 없는 경우: tz_localize 사용
        print("ℹ️  타임존 정보가 없습니다. Asia/Seoul로 설정합니다.")
        s = s.dt.tz_localize('Asia/Seoul', ambiguous='infer', nonexistent='shift_forward')
    else:
        # 이미 타임존이 있는 경우: tz_convert 사용
        print(f"ℹ️  기존 타임존({s.dt.tz})을 Asia/Seoul로 변환합니다.")
        print(f"ℹ️ 기존 타임존 {s.dt.tz}")
        s = s.dt.tz_convert('Asia/Seoul')
        print(f"ℹ️ 변경 타임존 {s.dt.tz}")
    
    # ISO 8601 형식으로 변환
    df_renamed['질문날짜'] = s.apply(lambda x: x.isoformat(timespec='seconds') if pd.notna(x) else None)
    df_renamed.loc[s.isna(), '질문날짜'] = None
    print("🔄 '질문날짜' 컬럼을 Notion 표준 시간 형식으로 변환했습니다.")
    
    print("✅ 데이터 전처리가 완료되었습니다.")
    print("=" * 50)
    
    # XCom으로 전달
    context['ti'].xcom_push(key='transformed_data', value=df_renamed.to_json(orient='records', date_format='iso'))


def load_to_notion(**context):
    """Notion DB에 데이터 적재"""
    print("\n" + "=" * 50)
    print("Step 3: Notion DB 동기화를 시작합니다.")
    
    # 설정 가져오기
    NOTION_DB_ID = get_var('NOTION_DB_ID_INJOY_MONITORINGDATA_CONSUMER', "230ea67a568180c591fee27d4e90e001")
    headers = get_notion_headers()
    
    # XCom에서 변환된 데이터 가져오기
    transformed_data_json = context['ti'].xcom_pull(key='transformed_data', task_ids='transform_data')
    df_renamed = pd.read_json(transformed_data_json, orient='records')
    
    # Notion 기존 데이터 조회
    print("\nStep 3-1: Notion DB의 기존 데이터 고유 키를 조회합니다.")
    notion_pages = get_all_notion_pages(NOTION_DB_ID, headers)
    
    existing_keys = set()
    for page in notion_pages:
        props = page.get("properties", {})
        
        user_rt = props.get('사용자', {}).get('rich_text', [])
        space_id_rt = props.get('스페이스id', {}).get('rich_text', [])
        convo_id_rt = props.get('대화id', {}).get('rich_text', [])
        msg_id_rt = props.get('메시지id', {}).get('rich_text', [])
        status_rt = props.get('status', {}).get('rich_text', [])
        error_type_rt = props.get('error_type', {}).get('rich_text', [])
        feedback_rating_rt = props.get('feedback_rating', {}).get('rich_text', [])
        
        row_values = {
            '사용자': user_rt[0].get('plain_text', '') if user_rt else '',
            '스페이스id': space_id_rt[0].get('plain_text', '') if space_id_rt else '',
            '대화id': convo_id_rt[0].get('plain_text', '') if convo_id_rt else '',
            '메시지id': msg_id_rt[0].get('plain_text', '') if msg_id_rt else '',
            'status': status_rt[0].get('select', '') if status_rt else '',
            'error_type': error_type_rt[0].get('select', '') if error_type_rt else '',
            'feedback_rating': feedback_rating_rt[0].get('select', '') if feedback_rating_rt else ''
        }

        key = generate_row_key(row_values)
        if key:
            existing_keys.add(key)
            
    print(f"✅ Notion DB에 존재하는 고유 키 {len(existing_keys)}개를 확인했습니다.")
    
    # 신규 데이터만 추가
    print(f"\nStep 3-2: 신규 데이터 추가를 시작합니다.")
    insert_count = 0
    
    for index, row in df_renamed.iterrows():
        row_dict = row.to_dict()
        key = generate_row_key(row_dict)
        
        if key not in existing_keys:
            insert_count += 1
            print(f"  -> [{insert_count}] 신규 데이터 발견! Notion에 추가합니다. (Key: {key})")
            
            properties_payload = build_properties_payload(row_dict)
            payload = {"parent": {"database_id": NOTION_DB_ID}, "properties": properties_payload}
            
            res = requests.post("https://api.notion.com/v1/pages", headers=headers, json=payload)
            
            if not res.ok:
                print(f"    ❌ 추가 실패! (Key: {key}) - 에러: {res.text}")
            
            time.sleep(0.3)

    if insert_count == 0:
        print("✅ 추가할 신규 데이터가 없습니다.")
    else:
        print(f"\n✅ 총 {insert_count}개의 신규 데이터를 Notion에 성공적으로 추가했습니다.")

    print("\n✨ 모든 동기화 작업이 완료되었습니다! ✨")
    print("=" * 50)


# ============================================================
# DAG 정의
# ============================================================

injoy_monitoringdata_producer = Dataset('injoy_monitoringdata_producer')

with DAG(
    dag_id='injoy_monitoringdata_consumer_2',
    default_args=default_args,
    description='Databricks 데이터를 Notion DB에 동기화하는 DAG',
    schedule=[injoy_monitoringdata_producer],
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['notion', 'sync', 'monitoring'],
) as dag:

    # Task 1: 데이터 추출
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        dag=dag,
    )

    # Task 2: 데이터 변환
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        dag=dag,
    )

    # Task 3: Notion 적재
    load_task = PythonOperator(
        task_id='load_to_notion',
        python_callable=load_to_notion,
        dag=dag,
    )

    # Task 의존성 설정
    extract_task >> transform_task >> load_task