from airflow import DAG, Dataset
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import time
import os
from databricks import sql

# notion : In-Joy 질의 모니터링 데이터 DB 에 동기화 처리
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
def get_var(key: str, default: str | None = None, required: bool = False) -> str:
    """환경 변수 → Airflow Variable 순서로 조회"""
    env_value = os.environ.get(key)
    if env_value:
        print(f"✓ 환경 변수에서 {key} 로드됨")
        return env_value
    
    try:
        try:
            var_value = Variable.get(key, None)
        except TypeError:
            var_value = Variable.get(key, None)
        
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
    return ''


NOTION_DATABASE_ID = get_var('NOTION_DB_ID_INJOY_MONITORINGDATA_CONSUMER', "230ea67a568180c591fee27d4e90e001")
## 복제 DB에 사용 시
# NOTION_DATABASE_ID = "30dea67a56818034bb5ae80442f5b0d6"

def get_notion_headers():
    """Notion API 헤더 생성"""
    return {
        "Authorization": f"Bearer {get_var('NOTION_TOKEN', required=True)}",
        "Notion-Version": get_var("NOTION_API_VERSION", "2022-06-28"),
        "Content-Type": "application/json"
    }


def generate_row_key(row: dict) -> str:
    """고유 키 생성: 사용자_스페이스id_대화id_메시지id"""
    
    def clean_key_value(val) -> str:
        # 1. Null, NaN, None 처리 -> 빈 문자열로 통일
        if pd.isna(val) or val is None:
            return ""
        
        s = str(val).strip()
        
        # 2. 파이썬이 'nan' 이라는 문자열로 만들어버린 경우 방어
        if s.lower() == 'nan':
            return ""
            
        # 3. Pandas 실수형 변환 방어 (예: 1234.0 -> 1234)
        if s.endswith('.0'):
            s = s[:-2]
            
        return s

    user = clean_key_value(row.get('사용자'))
    space_id = clean_key_value(row.get('스페이스id'))
    convo_id = clean_key_value(row.get('대화id'))
    msg_id = clean_key_value(row.get('메시지id'))
    
    return f"{user}_{space_id}_{convo_id}_{msg_id}"


def build_properties_payload(row_data: dict) -> dict:
    """DataFrame 행을 Notion API properties로 변환"""
    properties = {}

    for key, value in row_data.items():
        # 📌 1. 먼저 array/list 타입 확인
        if isinstance(value, (list, tuple, pd.Series))  :
            # 빈 배열이면 continue
            if len(value) == 0:
                continue
        else:
            # 일반 값은 pd.isna() 사용
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

        elif key in ["status", "error_type", "feedback_rating", "스페이스명"]:
            # 선택(Select) 속성은 값이 비어있을 때 빈 문자열("")을 보내면 에러가 날 수 있습니다.
            # 따라서 값이 있는 경우에만 select payload를 생성하도록 안전하게 처리하는 것이 좋습니다.
            val_str = str(value or "").strip()
            if val_str:
                properties[key] = {"select": {"name": val_str}}

        elif key in ['row_count', 'auto_regenerate_count']:
            try:
                for val in value:
                    int_value = int(val) if pd.notna(val) else 0
                properties[key] = {"number": int_value}
            except (ValueError, TypeError):
                continue

        # 📌 2. question 배열 처리 (새로 추가)
        elif key in ["questions"]:
            # value가 array/list인지 확인
            if isinstance(value, (list, tuple, pd.Series)):
                # 배열을 문자열로 변환
                question_list = []
                
                for item in value:
                    if pd.notna(item):  # None/NaN 제외
                        question_list.append(str(item).strip())
                
 
                question_str = "; ".join(question_list) if question_list else ""
                print(f"&&&&&&Question_str: {question_str}")
                if question_str:
                    # 2000자 제한 확인
                    if len(question_str) > 2000:
                        question_str = question_str[:1999]
                    
                    properties[key] = {"rich_text": [{"text": {"content": question_str}}]}
            else:
                # value가 배열이 아닌 경우 (단일 문자열)
                content = str(value or "")
                if content:
                    properties[key] = {"rich_text": [{"text": {"content": content}}]}
            
            continue  # 다음 반복으로

        elif key in ["error", "에러"]:
            content = str(value or "")
            if len(content) > 2000:
                print(f"  -> ✨ '{key}'의 긴 텍스트({len(content)}자)를 잘라서 저장합니다.")
                content = content[:2000]
            
            properties[key] = {"rich_text": [{"text": {"content": content}}]}

        else:
            properties[key] = {"rich_text": [{"text": {"content": str(value or "")}}]}
            
    return properties


def get_all_notion_pages(database_id: str, headers: dict, msg_id_list: list) -> dict:
    """df_renamed의 메시지id 목록에 해당하는 Notion 페이지만 조회 (배치 OR 필터)
    Returns: {msg_id: [pages]} 딕셔너리
    """
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    pages_map = {}

    unique_msg_ids = list(set(mid for mid in msg_id_list if mid))
    print(f"⏳ Notion DB에서 {len(unique_msg_ids)}개 메시지id에 해당하는 페이지 조회 시작...")

    # Notion OR 필터 최대 100개 제한 → 100개씩 배치
    for i in range(0, len(unique_msg_ids), 100):
        batch = unique_msg_ids[i:i + 100]
        payload = {
            "page_size": 100,
            "filter": {
                "or": [
                    {"property": "메시지id", "rich_text": {"equals": mid}}
                    for mid in batch
                ]
            }
        }

        has_more = True
        next_cursor = None

        while has_more:
            if next_cursor:
                payload["start_cursor"] = next_cursor
            try:
                res = requests.post(url, headers=headers, json=payload)
                res.raise_for_status()
                data = res.json()
                for page in data.get("results", []):
                    props = page.get("properties", {})
                    msg_id_rt = props.get("메시지id", {}).get("rich_text", [])
                    mid = msg_id_rt[0].get("plain_text", "") if msg_id_rt else ""
                    if mid:
                        pages_map.setdefault(mid, []).append(page)
                next_cursor = data.get("next_cursor")
                has_more = data.get("has_more", False)
            except requests.exceptions.RequestException as e:
                print(f"❌ Notion API 에러 발생: {e}")
                break

    total = sum(len(v) for v in pages_map.values())
    print(f"✅ Notion DB에서 총 {total}개의 페이지를 조회했습니다.")
    return pages_map


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
                user_name,
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
                questions,
                auto_regenerate_count,
                error,
                error_type,
                feedback_rating
            FROM 
                datahub.injoy_ops_schema.injoy_monitoring_data
            WHERE event_time_kst >= CAST(DATE(NOW()) - INTERVAL 3 DAYS AS TIMESTAMP) 
            AND event_time_kst < CAST(DATE(NOW()) AS TIMESTAMP) 
            ORDER BY conversation_id, event_time_kst
        """
         
        print("📊 SQL 쿼리 실행 중...")
        cursor = connection.cursor()
        cursor.execute(sql_query)
        
        # 결과를 DataFrame으로 변환
        columns = [desc[0] for desc in (cursor.description or [])]
        rows = cursor.fetchall()
        source_df = pd.DataFrame(rows, columns=columns)
        
        # 하드코딩으로 유저 제외
        exclude_emails = ['heegle@joycity.com', 'kimjack415@joycity.com', 'pradeepkumar.palaniswamy+dbadmin@databricks.com']
        source_df = source_df[~source_df['user_email'].isin(exclude_emails)]

        cursor.close()
        connection.close()
        
        print(f"✅ Databricks에서 총 {len(source_df)}개의 데이터를 조회했습니다.")
        email_list = source_df['user_email'].tolist()
        
        
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
        "user_name": "사용자명",
        "space_name": "스페이스명",
        "space_id": "스페이스id",
        "conversation_id": "대화id",
        "message_id": "메시지id",
        "query": "쿼리",
        "message_response_duration_seconds": "응답속도(초)",
        "event_time_kst": "질문날짜"
    })
    print("🔄 컬럼명을 Notion DB에 맞게 변경했습니다.")

    # 중복된 키가 있다면 가장 마지막(최신) 데이터 하나만 남기기
    df_renamed = df_renamed.drop_duplicates(
        subset=["사용자", "스페이스id", "대화id", "메시지id"], 
        keep="last"
    )
    
    # 날짜 형식 변환
    # event_time_kst는 Databricks에서 이미 KST로 변환된 값이지만 +00:00 라벨이 잘못 붙어 있음.
    # 따라서 잘못된 timezone 라벨을 제거한 뒤, 값 그대로를 KST(+09:00)로 재지정한다.
    # (tz_convert가 아닌 tz_localize를 사용하여 값 변환 없이 라벨만 교체)
    s = pd.to_datetime(df_renamed['질문날짜'], errors='coerce')
    if s.dt.tz is not None:
        print(f"ℹ️  잘못된 timezone 라벨({s.dt.tz}) 제거 후 KST(+09:00)로 재지정")
        s = s.dt.tz_localize(None)          # +00:00 라벨 제거 (값 변환 없음)
    s = s.dt.tz_localize('Asia/Seoul')      # +09:00 라벨 부착 (값 변환 없음)

    # ISO 8601 형식으로 변환 → Notion에 "2026-03-18T22:01:38+09:00" 형태로 전달
    df_renamed['질문날짜'] = s.apply(lambda x: x.isoformat(timespec='seconds') if pd.notna(x) else None)
    df_renamed.loc[s.isna(), '질문날짜'] = None
    print("🔄 '질문날짜' 컬럼을 Notion 표준 시간 형식으로 변환했습니다.")
    
    print("✅ 데이터 전처리가 완료되었습니다.")
    print("=" * 50)
    
    # XCom으로 전달
    # [핵심 수정 2] Key로 사용되는 컬럼들의 데이터 타입 엄격 통제
    def safe_string_convert(val):
        if pd.isna(val):
            return ""
        s = str(val)
        return s[:-2] if s.endswith('.0') else s  # 1234.0 -> 1234 로 정리

    key_columns = ["사용자", "스페이스id", "대화id", "메시지id"]
    for col in key_columns:
        df_renamed[col] = df_renamed[col].apply(safe_string_convert)

    context['ti'].xcom_push(key='transformed_data', value=df_renamed.to_json(orient='records', date_format='iso'))




def load_to_notion(**context):
    """Notion DB에 데이터 적재 (Insert, Update, 중복 제거)"""
    print("\n" + "=" * 50)
    print("Step 3: Notion DB 동기화를 시작합니다.")
    
    # 설정 가져오기
    NOTION_DB_ID = get_var('NOTION_DB_ID_INJOY_MONITORINGDATA_CONSUMER', "230ea67a568180c591fee27d4e90e001")
    headers = get_notion_headers()
    
    # XCom에서 변환된 데이터 가져오기
    transformed_data_json = context['ti'].xcom_pull(key='transformed_data', task_ids='transform_data')
    df_renamed = pd.read_json(transformed_data_json, orient='records')

    print(f"\n총 {len(df_renamed)}건에 대해 Notion 조회 및 upsert를 시작합니다.")
    for _, row in df_renamed.iterrows():
        r = row.to_dict()
        print(f"  [XCom 확인] key={generate_row_key(r)} | 사용자 질의={str(r.get('사용자 질의',''))[:30]} | 사용자명={r.get('사용자명')} | 응답속도(초)={r.get('응답속도(초)')} (type={type(r.get('응답속도(초)')).__name__})")

    # df_renamed의 메시지id 목록으로 Notion 페이지 일괄 조회
    msg_id_list = df_renamed['메시지id'].tolist()
    notion_pages_map = get_all_notion_pages(NOTION_DB_ID, headers, msg_id_list)

    insert_count = 0
    update_count = 0
    archive_count = 0

    for _, row in df_renamed.iterrows():
        row_dict = row.to_dict()
        key = generate_row_key(row_dict)
        msg_id = row_dict.get('메시지id', '')
        properties_payload = build_properties_payload(row_dict)

        # 사전 조회된 map에서 해당 메시지id 페이지 가져오기
        notion_pages = notion_pages_map.get(msg_id, [])

        # 조회된 페이지 중 key_columns 기준으로 정확히 일치하는 것만 필터링
        matched_pages = []
        for page in notion_pages:
            props = page.get("properties", {})
            user_rt = props.get('사용자', {}).get('rich_text', [])
            space_id_rt = props.get('스페이스id', {}).get('rich_text', [])
            convo_id_rt = props.get('대화id', {}).get('rich_text', [])
            msg_id_rt = props.get('메시지id', {}).get('rich_text', [])
            page_key = generate_row_key({
                '사용자': user_rt[0].get('plain_text', '') if user_rt else '',
                '스페이스id': space_id_rt[0].get('plain_text', '') if space_id_rt else '',
                '대화id': convo_id_rt[0].get('plain_text', '') if convo_id_rt else '',
                '메시지id': msg_id_rt[0].get('plain_text', '') if msg_id_rt else '',
            })
            if page_key == key:
                matched_pages.append({"id": page.get("id"), "created_time": page.get("created_time")})

        # 중복 페이지 처리: 가장 오래된 것 유지, 나머지 archive
        if len(matched_pages) > 1:
            matched_pages.sort(key=lambda x: x["created_time"])
            for dup in matched_pages[1:]:
                res = requests.patch(f"https://api.notion.com/v1/pages/{dup['id']}", headers=headers, json={"archived": True})
                if res.ok:
                    archive_count += 1
                    print(f"  -> 중복 삭제 완료 (Key: {key}, PageID: {dup['id']})")
                time.sleep(0.3)

        # Insert or Update
        if matched_pages:
            # Update (가장 오래된 페이지에 덮어쓰기)
            page_id = matched_pages[0]["id"]
            print(f"    [DEBUG] 응답속도(초) in payload: {properties_payload.get('응답속도(초)')}")
            print(f"    [DEBUG] payload keys: {list(properties_payload.keys())}")
            res = requests.patch(f"https://api.notion.com/v1/pages/{page_id}", headers=headers, json={"properties": properties_payload})
            if not res.ok:
                print(f"    ❌ 업데이트 실패! (Key: {key}) - 에러: {res.text}")
            else:
                update_count += 1
                print(f"    🔄 업데이트 성공! (Key: {key})")
        else:
            # Insert
            res = requests.post("https://api.notion.com/v1/pages", headers=headers, json={"parent": {"database_id": NOTION_DB_ID}, "properties": properties_payload})
            if not res.ok:
                print(f"    ❌ 추가 실패! (Key: {key}) - 에러: {res.text}")
            else:
                insert_count += 1
                print(f"    🆕 신규 추가 성공! (Key: {key})")

        time.sleep(0.3)

    if archive_count > 0:
        print(f"✅ 총 {archive_count}개의 중복 데이터를 Archive 처리했습니다.")
    print(f"\n✅ 작업 결과: {insert_count}개 신규 추가, {update_count}개 업데이트 완료.")
    print("\n✨ 모든 동기화 작업이 완료되었습니다! ✨")
    print("=" * 50)

# ============================================================
# DAG 정의
# ============================================================

injoy_monitoringdata_producer_ver2 = Dataset('injoy_monitoringdata_producer_ver2')

with DAG(
    dag_id='injoy_monitoringdata_consumer_2',
    default_args=default_args,
    description='Databricks 데이터를 Notion DB에 동기화하는 DAG',
    schedule=[injoy_monitoringdata_producer_ver2],
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