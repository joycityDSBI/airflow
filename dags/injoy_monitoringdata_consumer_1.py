"""
Notion DB 동기화 DAG
- Databricks에서 데이터 조회
- Pandas로 데이터 집계
- Notion DB와 동기화 (INSERT/UPDATE/DELETE)
"""

from airflow import DAG, Dataset
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import time
import json
import os

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
    """환경 변수 → Airflow Variable 순서로 조회
    
    Args:
        key: 변수 이름
        default: 기본값 (없으면 None)
        required: 필수 변수 여부
    
    Returns:
        환경 변수 값 또는 Airflow Variable 값
    """
    # 1단계: 환경 변수 확인
    env_value = os.environ.get(key)
    if env_value:
        print(f"✓ 환경 변수에서 {key} 로드됨")
        return env_value
    
    # 2단계: Airflow Variable 확인
    try:
        # airflow.sdk.Variable은 default 파라미터 사용
        # airflow.models.Variable은 default_var 파라미터 사용
        try:
            var_value = Variable.get(key, None)
        except TypeError:
            # fallback for older Airflow versions
            var_value = Variable.get(key, None)
        
        if var_value:
            print(f"✓ Airflow Variable에서 {key} 로드됨")
            return var_value
    except Exception as e:
        print(f"⚠️  Variable.get({key}) 오류: {str(e)}")
    
    # 3단계: 기본값 반환
    if default is not None:
        print(f"ℹ️  기본값으로 {key} 설정됨: {default}")
        return default
    
    # 4단계: 필수 변수인 경우 에러, 아니면 None 반환
    if required:
        raise ValueError(f"필수 설정 {key}을(를) 찾을 수 없습니다. "
                         f"환경 변수 또는 Airflow Variable에서 설정하세요.")
    
    print(f"ℹ️  {key} 값을 찾을 수 없습니다 (선택사항)")
    return ''



def get_notion_headers():
    """Notion API 헤더 생성"""
    return {
        "Authorization": f"Bearer {get_var('NOTION_TOKEN', required=True)}",
        "Notion-Version": get_var("NOTION_API_VERSION", "2022-06-28"),
        "Content-Type": "application/json"
    }


# ============================================================
# Notion API 함수들
# ============================================================
def get_all_notion_pages(database_id: str, headers: dict) -> list:
    """Notion DB의 모든 페이지를 조회하여 리스트로 반환합니다."""
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    results = []
    has_more = True
    next_cursor = None
    
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
            print(f"❌ Notion API 에러: {e}")
            break
        
        time.sleep(0.3)
    
    return results


def get_notion_data_map(pages: list) -> dict:
    """Notion 페이지 리스트를 {key: {page_id, values}} 맵으로 변환합니다."""
    page_map = {}
    
    for page in pages:
        props = page.get("properties", {})
        row = {}
        key_val = ""
        
        for prop_name, prop_val in props.items():
            content = ""
            prop_type = prop_val.get("type")

            if prop_type == "title":
                content = "".join([block.get("plain_text", "") for block in prop_val.get("title", [])])
            elif prop_type == "rich_text":
                content = "".join([block.get("plain_text", "") for block in prop_val.get("rich_text", [])])
            elif prop_type == "date":
                date_info = prop_val.get("date")
                if date_info:
                    content = date_info.get("start")
            
            row[prop_name] = content
            if prop_name == '대화id':
                key_val = content

        if key_val:
            page_map[key_val] = {"page_id": page["id"], "values": row}
    
    return page_map


def build_properties_payload(row_data: dict) -> dict:
    """DataFrame 행을 Notion API 페이로드로 변환합니다."""
    properties = {}
    prop_map = {
        "대화id": row_data.get("conversation_id"),
        "사용자": row_data.get("user_email"),
        "스페이스명": row_data.get("space_name"),
        "대화순서": row_data.get("conversation_flow"),
        "대화날짜": row_data.get("conversation_date")
    }

    for key, value in prop_map.items():
        if value is None:
            continue

        if key == "대화id":
            properties[key] = {"title": [{"text": {"content": str(value)[:2000]}}]}
        
        elif key == "대화날짜":
            if hasattr(value, 'strftime'):
                date_str = value.strftime('%Y-%m-%d')
                properties[key] = {"date": {"start": date_str}}
            elif isinstance(value, str):
                properties[key] = {"date": {"start": value}}
        
        else:  # 사용자, 스페이스명, 대화순서
            content = ""
            if isinstance(value, list):
                if not value:
                    continue
                content = '\n'.join(map(str, [item for item in value if item is not None]))
            else:
                content = str(value or "")
            
            if not content:
                continue

            if len(content) > 2000:
                chunks = [content[i:i + 2000] for i in range(0, len(content), 2000)]
                properties[key] = {"rich_text": [{"text": {"content": chunk}} for chunk in chunks]}
            else:
                properties[key] = {"rich_text": [{"text": {"content": content}}]}
                
    return properties


def has_data_changed(source_row: dict, notion_row: dict) -> bool:
    """소스 데이터와 Notion 데이터 변경 여부를 비교합니다."""
    conv_date = source_row.get("conversation_date")

    prop_map = {
        "대화id": str(source_row.get("conversation_id") or ""),
        "사용자": str(source_row.get("user_email") or ""),
        "스페이스명": str(source_row.get("space_name") or ""),
        "대화날짜": conv_date.strftime('%Y-%m-%d') if pd.notna(conv_date) else "",
    }

    source_flow_as_string = '\n'.join(map(str, [item for item in source_row.get("conversation_flow", []) if item is not None]))
    if source_flow_as_string != notion_row.get("대화순서", ""):
        return True

    for key, source_value in prop_map.items():
        notion_value = notion_row.get(key, "")
        if source_value != notion_value:
            return True
            
    return False


# ============================================================
# DAG Task 함수들
# ============================================================
def fetch_data_from_databricks(**context):
    """
    Step 1: Databricks에서 데이터 조회
    """
    print("Step 1: Databricks에서 데이터 조회를 시작합니다.")
    
    # Databricks 설정 확인
    db_hostname = get_var("DATABRICKS_SERVER_HOSTNAME")
    db_http_path = get_var("DATABRICKS_HTTP_PATH")
    db_token = get_var("DATABRICKS_TOKEN")
    
    # Databricks 설정이 모두 있는 경우에만 실제 연결 시도
    if db_hostname and db_http_path and db_token:
        try:
            from databricks import sql
            
            print("✓ Databricks 설정이 확인되었습니다. 연결을 시도합니다.")
            connection = sql.connect(
                server_hostname=db_hostname,
                http_path=db_http_path,
                access_token=db_token
            )
            cursor = connection.cursor()
            cursor.execute("""
                SELECT conversation_id, user_email, space_name, content, event_time_kst
                FROM datahub.injoy_ops_schema.injoy_monitoring_data
                WHERE event_time_kst >= CAST(DATE(NOW()) - INTERVAL 3 DAYS AS TIMESTAMP) 
                AND event_time_kst < CAST(DATE(NOW()) AS TIMESTAMP) 
                ORDER BY conversation_id, event_time_kst
            """)
            
            # 결과를 DataFrame으로 변환
            if cursor.description is not None:
                columns = [desc[0] for desc in cursor.description]
            else:
                # 쿼리 결과가 없는 경우(DML 작업 등)에 대한 처리
                print("⚠️ 쿼리 결과가 없거나 description을 가져올 수 없습니다.")
                
            data = cursor.fetchall()
            source_df = pd.DataFrame(data, columns=columns)
            
            cursor.close()
            connection.close()
            
            print(f"✅ Databricks에서 {len(source_df)}개의 메시지 데이터를 조회했습니다.")
            
        except ImportError:
            print("⚠️  databricks-sql-connector가 설치되지 않았습니다.")
            print("   pip install databricks-sql-connector 를 실행하세요.")
            source_df = pd.DataFrame({
                'conversation_id': [],
                'user_email': [],
                'space_name': [],
                'content': [],
                'event_time_kst': []
            })
        except Exception as e:
            print(f"❌ Databricks 연결 중 오류 발생: {e}")
            print("   빈 데이터프레임으로 계속 진행합니다.")
            source_df = pd.DataFrame({
                'conversation_id': [],
                'user_email': [],
                'space_name': [],
                'content': [],
                'event_time_kst': []
            })
    else:
        # Databricks 설정이 없는 경우
        print("⚠️  Databricks 설정이 완전하지 않습니다.")
        print("   다음 변수들을 설정하세요:")
        print("   - DATABRICKS_SERVER_HOSTNAME")
        print("   - DATABRICKS_HTTP_PATH")
        print("   - DATABRICKS_TOKEN")
        print("   빈 데이터프레임으로 계속 진행합니다.")
        
        source_df = pd.DataFrame({
            'conversation_id': [],
            'user_email': [],
            'space_name': [],
            'content': [],
            'event_time_kst': []
        })
    
    # XCom에 데이터 저장 (JSON 직렬화 가능한 형태로)
    context['task_instance'].xcom_push(
        key='source_data', 
        value=source_df.to_json(orient='records', date_format='iso')
    )


def aggregate_data(**context):
    """
    Step 2: Pandas로 데이터 집계
    """
    print("\nStep 2: Pandas를 사용하여 데이터를 대화별로 집계합니다.")
    
    # XCom에서 데이터 가져오기
    source_data_json = context['task_instance'].xcom_pull(key='source_data', task_ids='fetch_data')
    source_df = pd.read_json(source_data_json, orient='records')
    
    if source_df.empty:
        print("⚠️  조회된 데이터가 없습니다. 집계를 건너뜁니다.")
        context['task_instance'].xcom_push(key='aggregated_data', value='[]')
        return
    
    # 데이터 집계
    aggregated_df = source_df.groupby('conversation_id').agg(
        user_email=('user_email', 'first'),
        space_name=('space_name', 'first'),
        conversation_flow=('content', list),
        conversation_date=('event_time_kst', 'first')
    ).reset_index()

    # 데이터 타입 변환
    aggregated_df['conversation_date'] = pd.to_datetime(aggregated_df['conversation_date']).dt.date
    aggregated_df['conversation_id'] = aggregated_df['conversation_id'].astype(str)

    print(f"✅ Pandas 집계 완료 . 총 {len(aggregated_df)}개의 대화로 그룹화되었습니다.")
    
    # XCom에 집계 데이터 저장
    context['task_instance'].xcom_push(key='aggregated_data', value=aggregated_df.to_json(orient='records', date_format='iso'))


def sync_with_notion(**context):
    """
    Step 3: Notion DB와 데이터 동기화
    """
    print("\nStep 3: Notion DB와 데이터 동기화를 시작합니다.")
    
    # 설정 값 가져오기
    NOTION_DB_ID = get_var("NOTION_DB_ID_INJOY_MONITORINGDATA", "234ea67a568180029ffbfeaa7e73a011")
    headers = get_notion_headers()
    
    # XCom에서 집계 데이터 가져오기
    aggregated_data_json = context['task_instance'].xcom_pull(key='aggregated_data', task_ids='aggregate_data')
    aggregated_df = pd.read_json(aggregated_data_json, orient='records')
    
    if aggregated_df.empty:
        print("⚠️  집계된 데이터가 없습니다. 동기화를 건너뜁니다.")
        return
    
    # conversation_date를 date 객체로 변환
    aggregated_df['conversation_date'] = pd.to_datetime(aggregated_df['conversation_date']).dt.date
    
    # Notion DB 데이터 조회
    notion_pages = get_all_notion_pages(NOTION_DB_ID, headers)
    notion_map = get_notion_data_map(notion_pages)
    print(f"☁️ Notion DB에서 {len(notion_map)}개의 데이터를 확인했습니다.")

    # 소스 데이터 맵 생성
    source_map = {row['conversation_id']: row for row in aggregated_df.to_dict('records')}
    print(f"☁️ 소스에서 {len(source_map)}개의 데이터를 확인했습니다.")

    # 동기화 계획 수립
    source_keys = set(source_map.keys())
    notion_keys = set(notion_map.keys())
    to_insert = source_keys - notion_keys
    to_update_keys = source_keys & notion_keys
    to_delete = notion_keys - source_keys

    print(f"\n🔄 동기화 계획: 추가 {len(to_insert)}건, 업데이트 확인 {len(to_update_keys)}건, 삭제 {len(to_delete)}건")

    # INSERT 작업
    print("\n[INSERT 작업 시작]")
    for key in to_insert:
        print(f"  -> INSERT: {key}")
        row_data = source_map[key]
        properties = build_properties_payload(row_data)
        payload = {"parent": {"database_id": NOTION_DB_ID}, "properties": properties}
        
        try:
            res = requests.post("https://api.notion.com/v1/pages", headers=headers, json=payload)
            res.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"      ❌ 추가 실패: {e}\n      - Payload: {json.dumps(payload, indent=2, ensure_ascii=False)}")
        
        time.sleep(0.3)

    # UPDATE 작업
    print("\n[UPDATE 작업 시작]")
    updates_to_perform = []
    for key in to_update_keys:
        source_row = source_map[key]
        notion_row_data = notion_map[key]['values']
        if has_data_changed(source_row, notion_row_data):
            updates_to_perform.append(key)
            
    print(f"  -> 총 {len(updates_to_perform)}건의 데이터 변경 감지. 업데이트를 진행합니다.")
    for key in updates_to_perform:
        print(f"  -> UPDATE: {key}")
        page_id = notion_map[key]['page_id']
        properties = build_properties_payload(source_map[key])
        payload = {"properties": properties}
        
        try:
            res = requests.patch(f"https://api.notion.com/v1/pages/{page_id}", headers=headers, json=payload)
            res.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"      ❌ 업데이트 실패: {e}\n      - Payload: {json.dumps(payload, indent=2, ensure_ascii=False)}")
        
        time.sleep(0.3)

    # DELETE 작업
    print("\n[DELETE 작업 시작]")
    for key in to_delete:
        print(f"  -> DELETE: {key}")
        page_id = notion_map[key]['page_id']
        
        try:
            res = requests.patch(f"https://api.notion.com/v1/pages/{page_id}", headers=headers, json={"archived": True})
            res.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"      ❌ 삭제 실패: {e}")
        
        time.sleep(0.3)

    print("\n✨ 모든 동기화 작업이 완료되었습니다! ✨")



# ============================================================
# DAG 정의
# ============================================================

injoy_monitoringdata_producer_ver2 = Dataset('injoy_monitoringdata_producer2')

with DAG(
    dag_id='injoy_monitoringdata_consumer_1',
    default_args=default_args,
    description='Databricks 데이터를 Notion DB와 동기화',
    schedule=[injoy_monitoringdata_producer_ver2],
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['notion', 'databricks', 'sync'],
) as dag:

    # Task 1: Databricks에서 데이터 조회
    task_fetch_data = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data_from_databricks,
        dag=dag,
    )

    # Task 2: 데이터 집계
    task_aggregate = PythonOperator(
        task_id='aggregate_data',
        python_callable=aggregate_data,
        dag=dag,
    )

    # Task 3: Notion DB 동기화
    task_sync = PythonOperator(
        task_id='sync_with_notion',
        python_callable=sync_with_notion,
        dag=dag,
    )

    # Task 의존성 설정
    task_fetch_data >> task_aggregate >> task_sync