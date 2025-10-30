from datetime import datetime, timedelta, timezone as dt_timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.models import Variable
import pandas as pd
import requests
from airflow import Dataset
import numpy as np
import json
import logging

dataset_injoy_monitoringdata_producer = Dataset('injoy_monitoringdata_producer')

# 제외 그룹 필터링
# exclude_groups = ["DITeam", "admins", "users", 
#                     "전략사업본부-데이터사이언스실-마케팅사이언스팀", 
#                     "전략사업본부-데이터사이언스실-예측모델링팀"]

exclude_groups = []


# DAG 기본 설정
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='injoy_monitoringdata_producer',
    default_args=default_args,
    description='Process Databricks audit logs for aibiGenie',
    schedule='0 16 * * *',  # 매일 새벽 1시 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['databricks', 'audit', 'genie'],
)

def get_databricks_config():
    """Databricks 설정 가져오기"""
    return {
        'instance': Variable.get('databricks_instance'),
        'token': Variable.get('databricks_token')
    }

# 설정
headers = {
    "Authorization": f"Bearer {get_databricks_config()['token']}"
}

url = f"{get_databricks_config()['instance']}/api/2.0/token/list"

def tokenize_databricks(url, headers):
    
    resp = requests.get(url, headers=headers)

    if resp.status_code != 200:
        print(f"❌ 토큰 조회 실패: {resp.status_code}")
        print(resp.text)
    else:
        tokens = resp.json().get("token_infos", [])
        
        def convert_ms(ms):
            if ms == -1:
                return None
            return datetime.fromtimestamp(ms / 1000)

        for t in tokens:
            creation = convert_ms(t.get("creation_time"))
            expiry = convert_ms(t.get("expiry_time"))
            if creation:
                print(f"   생성시간: {creation.strftime('%Y-%m-%d %H:%M:%S')}")
            else:
                print("   생성시간: 알 수 없음")
            if expiry:
                print(f"   만료시간: {expiry.strftime('%Y-%m-%d %H:%M:%S')}")
            else:
                print("   만료시간: 무기한 또는 없음")



def extract_audit_logs(**context):
    """Databricks audit 로그 추출 및 처리"""
    from databricks import sql
    
    config = get_databricks_config()
    ds = context.get('ds')
    connection = sql.connect(
        server_hostname=config['instance'].replace('https://', ''),
        http_path=Variable.get('databricks_http_path'),
        access_token=config['token']
    )

    query = f"""
    WITH raw_log AS (
        SELECT 
            'audit_log' as log_type,
            user_identity.email as user_email,
            action_name,
            request_params,
            response.result,
            CAST(event_time AS TIMESTAMP) + INTERVAL 9 HOURS AS event_time_kst
        FROM system.access.audit
        WHERE service_name = 'aibiGenie'
            AND action_name IN ('createConversationMessage', 'updateConversationMessageFeedback', 'getMessageQueryResult')
            AND DATE(event_time) >= CURRENT_DATE - INTERVAL 1 DAYS
            AND DATE(event_time) < CURRENT_DATE
    ),

    message_tb AS (
        SELECT
            'audit_log' as log_type,
            user_email,
            get_json_object(result, '$.user_id') AS user_id,
            action_name,
            event_time_kst,
            get_json_object(result, '$.space_id') AS space_id,
            get_json_object(result, '$.conversation_id') AS conversation_id,
            get_json_object(result, '$.message_id') AS message_id
        FROM raw_log
        WHERE action_name IN ('createConversationMessage')
    ),

    feedback_rb AS (
        SELECT * EXCEPT(rn)
        FROM (
            SELECT 
                request_params.space_id, 
                request_params.conversation_id, 
                request_params.message_id, 
                request_params.feedback_rating,
                ROW_NUMBER() OVER(
                    PARTITION BY request_params.space_id, request_params.conversation_id, request_params.message_id 
                    ORDER BY event_time_kst DESC
                ) as rn
            FROM raw_log
            WHERE action_name = 'updateConversationMessageFeedback'
        )
        WHERE rn = 1
    )

    SELECT 
        a.* EXCEPT(event_time_kst), 
        b.feedback_rating, 
        a.event_time_kst 
    FROM message_tb AS a
    LEFT JOIN feedback_rb AS b
        ON a.space_id = b.space_id
        AND a.conversation_id = b.conversation_id
        AND a.message_id = b.message_id
    """
    cursor = connection.cursor()
    cursor.execute(query)
    df_audit = cursor.fetchall_arrow().to_pandas()
    
    cursor.close()
    connection.close()
    
    print(f"✅ Audit log 추출 완료: {len(df_audit)} rows")
    
    # XCom으로 데이터 전달
    context['ti'].xcom_push(key='df_audit', value=df_audit.to_json(orient='split', date_format='iso'))
    
    return len(df_audit)

def get_user_groups(**context):
    """
    Task 2: SCIM API로 그룹 및 사용자 정보 수집
    """
    config = get_databricks_config()
    headers = {"Authorization": f"Bearer {config['token']}"}
    
    # Step 1: 그룹 전체 목록 조회
    group_url = f"{config['instance']}/api/2.0/preview/scim/v2/Groups"
    group_resp = requests.get(group_url, headers=headers)
    
    if group_resp.status_code != 200:
        raise Exception(f"그룹 조회 실패: {group_resp.status_code} - {group_resp.text}")
    
    groups = group_resp.json().get("Resources", [])
    df_groups = pd.DataFrame([{"group_name": g["displayName"], "group_id": g["id"]} for g in groups])
    
    target_groups = df_groups[~df_groups["group_name"].isin(exclude_groups)]
    
    # Step 3: 각 그룹의 구성원 수집
    user_group_list = []
    exclude_user_id = "6547992203707764"
    
    for _, row in target_groups.iterrows():
        group_name = row["group_name"]
        group_id = row["group_id"]
        group_detail_url = f"{config['instance']}/api/2.0/preview/scim/v2/Groups/{group_id}"
        detail_resp = requests.get(group_detail_url, headers=headers)
        
        if detail_resp.status_code != 200:
            print(f"⚠️ 그룹 상세 조회 실패 (group_id={group_id}): {detail_resp.status_code}")
            continue
        
        group_detail = detail_resp.json()
        members = group_detail.get("members", [])
        
        for m in members:
            user_id = m.get("value")
            if user_id and user_id != exclude_user_id:
                user_group_list.append({"user_id": user_id, "group_name": group_name})
    
    # Step 4: DataFrame으로 변환 및 그룹핑
    df_user_groups = pd.DataFrame(user_group_list).drop_duplicates()
    
    df_user_groups = (
        df_user_groups
        .groupby("user_id")["group_name"]
        .apply(lambda x: sorted(set(x)))
        .reset_index()
    )
    
    print(f"✅ 사용자 그룹 정보 수집 완료: {len(df_user_groups)} users")
    
    # XCom으로 데이터 전달
    context['ti'].xcom_push(key='df_user_groups', value=df_user_groups.to_json(orient='split'))
    
    return len(df_user_groups)

def enrich_with_groups(**context):
    """
    Task 3: Audit log에 그룹 정보 병합
    """
    ti = context['ti']
    
    # XCom에서 데이터 가져오기
    df_audit = pd.read_json(ti.xcom_pull(task_ids='extract_audit_logs', key='df_audit'), orient='split')
    df_user_groups = pd.read_json(ti.xcom_pull(task_ids='get_user_groups', key='df_user_groups'), orient='split')
    
    # 병합
    df_audit_with_group = df_audit.merge(df_user_groups, on="user_id", how="left")
    
    # group_name이 NaN인 행 제거
    df_audit_with_group = df_audit_with_group.dropna(subset=["group_name"])
    
    print(f"✅ 그룹 정보 병합 완료: {len(df_audit_with_group)} rows")
    
    context['ti'].xcom_push(key='df_audit_with_group', value=df_audit_with_group.to_json(orient='split', date_format='iso'))
    
    return len(df_audit_with_group)

def get_space_info(**context):
    """
    Task 4: Genie API로 스페이스 정보 수집
    """
    config = get_databricks_config()
    headers = {"Authorization": f"Bearer {config['token']}"}
    
    # 스페이스 목록 조회
    spaces_url = f"{config['instance']}/api/2.0/genie/spaces"
    resp = requests.get(spaces_url, headers=headers)
    
    if resp.status_code != 200:
        print(f"⚠️ 스페이스 목록 조회 실패: {resp.status_code}")
        space_id_to_name = {}
    else:
        spaces = resp.json().get("spaces", [])
        space_df = pd.DataFrame([
            {"space_id": s.get("space_id"), "space_name": s.get("title")}
            for s in spaces
        ])
        space_id_to_name = dict(zip(space_df["space_id"], space_df["space_name"]))
    
    print(f"✅ 스페이스 정보 수집 완료: {len(space_id_to_name)} spaces")
    
    context['ti'].xcom_push(key='space_id_to_name', value=space_id_to_name)
    
    return len(space_id_to_name)

def get_message_details(**context):
    """
    Task 5: Genie API로 메시지 상세 정보 수집
    """
    ti = context['ti']
    config = get_databricks_config()
    headers = {"Authorization": f"Bearer {config['token']}"}
    
    # 데이터 가져오기
    df_audit_with_group = pd.read_json(ti.xcom_pull(task_ids='enrich_with_groups', key='df_audit_with_group'), orient='split')
    space_id_to_name = ti.xcom_pull(task_ids='get_space_info', key='space_id_to_name')
    
    # 스페이스 이름 추가
    df_audit_with_group["space_name"] = df_audit_with_group["space_id"].map(space_id_to_name)
    
    # group_name이 리스트 형태이므로 any로 체크
    def should_exclude(group_list):
        if isinstance(group_list, list):
            return any(g in exclude_groups for g in group_list)
        return False
    
    df_target = df_audit_with_group[~df_audit_with_group["group_name"].apply(should_exclude)].copy()
    
    # 메시지 상세 정보 수집
    contents = []
    queries = []
    statement_ids = []
    
    total_rows = len(df_target)
    print(f"🔄 메시지 상세 정보 수집 시작: {total_rows} rows")
    
    for idx, row in df_target.iterrows():
        if idx % 100 == 0:
            print(f"  Progress: {idx}/{total_rows}")
        
        space_id = row["space_id"]
        conversation_id = row["conversation_id"]
        message_id = row["message_id"]
        
        if None in [space_id, conversation_id, message_id]:
            contents.append(None)
            queries.append(None)
            statement_ids.append(None)
            continue
        
        url = f"{config['instance']}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}"
        
        try:
            resp = requests.get(url, headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                
                # Content 처리
                content_raw = data.get("content")
                if isinstance(content_raw, str):
                    content = content_raw.replace("\n", " ")
                else:
                    content = str(content_raw) if content_raw is not None else None
                
                # Query 처리
                attachments = data.get("attachments", [])
                if attachments and isinstance(attachments, list):
                    query = attachments[0].get("query", {}).get("query", None)
                else:
                    query = None
                
                # Statement ID 처리
                statement_id = data.get("query_result", {}).get("statement_id")
                
            else:
                content, query, statement_id = None, None, None
                
        except Exception as e:
            print(f"❌ 예외 발생 ({idx}행): {e}")
            content, query, statement_id = None, None, None
        
        contents.append(content)
        queries.append(query)
        statement_ids.append(statement_id)
    
    # 데이터 추가
    df_target['content'] = contents
    df_target['query'] = queries
    df_target['statement_id'] = statement_ids
    
    print(f"✅ 메시지 상세 정보 수집 완료: {len(df_target)} rows")
    
    context['ti'].xcom_push(key='df_target', value=df_target.to_json(orient='split', date_format='iso'))
    
    return len(df_target)


def merge_query_history(**context):
    """
    Task 6: Query history와 병합 및 최종 데이터 저장
    """
    from databricks import sql
    import pandas as pd
    from io import StringIO
    import numpy as np
    import json
    
    ti = context['ti']
    config = get_databricks_config()
    
    # 데이터 가져오기
    json_data = ti.xcom_pull(task_ids='get_message_details', key='df_target')
    df_target = pd.read_json(StringIO(json_data), orient='split')
    
    # Databricks SQL 연결
    connection = sql.connect(
        server_hostname=config['instance'].replace('https://', ''),
        http_path=Variable.get('databricks_http_path'),
        access_token=config['token']
    )
    
    cursor = connection.cursor()
    
    # Query history 조회
    query_history_sql = """
    SELECT 
        statement_id, 
        executed_by, 
        execution_status, 
        CAST(total_duration_ms AS DOUBLE) / 1000 AS query_duration_seconds, 
        CAST(result_fetch_duration_ms AS DOUBLE) / 1000 AS query_result_fetch_duration_seconds, 
        CAST(end_time AS TIMESTAMP) + INTERVAL 9 HOURS AS query_end_time_kst
    FROM system.query.history
    WHERE query_source.genie_space_id IS NOT NULL
        AND statement_type = 'SELECT'
        AND DATE(end_time) >= CURRENT_DATE - INTERVAL 1 DAYS
        AND DATE(end_time) < CURRENT_DATE
    """
    
    cursor.execute(query_history_sql)
    query_df = cursor.fetchall_arrow().to_pandas()
    
    print(f"📊 Query history 조회 완료: {len(query_df)} rows")
    
    # 컬럼 존재 확인 및 rename
    if 'executed_by' in query_df.columns:
        query_df_renamed = query_df.rename(columns={"executed_by": "user_email"})
    else:
        print(f"⚠️ Warning: 'executed_by' 컬럼이 없습니다.")
        cursor.close()
        connection.close()
        raise KeyError(f"'executed_by' 컬럼을 찾을 수 없습니다.")
    
    # 병합
    df_audit_enriched = df_target.merge(
        query_df_renamed[[
            "statement_id", "user_email", "query_end_time_kst", 
            "query_duration_seconds", "query_result_fetch_duration_seconds", "execution_status"
        ]],
        how="left",
        on=["statement_id", "user_email"]
    )
    
    # 응답 소요시간 계산
    df_audit_enriched["event_time_kst"] = pd.to_datetime(df_audit_enriched["event_time_kst"])
    df_audit_enriched["query_end_time_kst"] = pd.to_datetime(df_audit_enriched["query_end_time_kst"])
    df_audit_enriched["message_response_duration_seconds"] = (
        df_audit_enriched["query_end_time_kst"] - df_audit_enriched["event_time_kst"]
    ).dt.total_seconds()
    
    print(f"✅ Query history 병합 완료: {len(df_audit_enriched)} rows")
    
    # ===== 기존 테이블 스키마 확인 =====
    
    cursor.execute("DESCRIBE datahub.injoy_ops_schema.injoy_monitoring_data")
    table_schema = cursor.fetchall()
    table_column_types = {row[0]: row[1] for row in table_schema}
    table_columns = list(table_column_types.keys())
    
    print(f"📊 기존 테이블 컬럼: {table_columns}")
    
    # DataFrame 컬럼을 테이블 컬럼에 맞추기
    available_columns = [col for col in table_columns if col in df_audit_enriched.columns]
    df_to_insert = df_audit_enriched[available_columns].copy()
    
    print(f"📊 UPSERT할 컬럼: {available_columns}")
    
    # ===== SQL 리터럴 변환 함수 =====
    
    def value_to_sql_literal(val, col_type):
        """값을 SQL 리터럴로 변환"""
        # NULL 처리
        if val is None:
            return 'NULL'
        
        try:
            if pd.isna(val):
                return 'NULL'
        except (ValueError, TypeError):
            pass
        
        # ARRAY 타입 처리
        if 'array' in col_type.lower():
            if isinstance(val, list):
                # 리스트의 각 요소를 문자열로 변환하고 이스케이프
                escaped_items = [f"'{str(item).replace(chr(39), chr(39)+chr(39))}'" for item in val]
                return f"array({', '.join(escaped_items)})"
            else:
                return 'NULL'
        
        # TIMESTAMP 처리
        if 'timestamp' in col_type.lower():
            if isinstance(val, pd.Timestamp):
                return f"timestamp '{val.strftime('%Y-%m-%d %H:%M:%S')}'"
            else:
                return f"timestamp '{str(val)}'"
        
        # DOUBLE/FLOAT 처리
        if col_type.lower() in ['double', 'float']:
            if pd.notna(val):
                return str(float(val))
            else:
                return 'NULL'
        
        # BIGINT/INT 처리
        if col_type.lower() in ['bigint', 'int', 'integer']:
            if pd.notna(val):
                return str(int(val))
            else:
                return 'NULL'
        
        # STRING 처리 (기본)
        if isinstance(val, str):
            # 작은따옴표 이스케이프
            escaped_val = val.replace("'", "''")
            return f"'{escaped_val}'"
        
        # 기타
        return f"'{str(val)}'"
    
    # ===== 임시 테이블 생성 (CREATE TABLE AS SELECT VALUES) =====
    
    temp_table = "datahub.injoy_ops_schema.temp_monitoring_data"
    
    # 기존 임시 테이블 삭제
    cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
    print(f"✅ 기존 임시 테이블 삭제")
    
    # VALUES 절 생성
    values_list = []
    for idx, row in df_to_insert.iterrows():
        row_values = []
        for col in available_columns:
            col_type = table_column_types[col]
            sql_literal = value_to_sql_literal(row[col], col_type)
            row_values.append(sql_literal)
        values_list.append(f"({', '.join(row_values)})")
    
    # 100개씩 나눠서 INSERT (SQL 문이 너무 길어지는 것 방지)
    batch_size = 100
    
    # 첫 번째 배치로 테이블 생성
    first_batch = values_list[:batch_size]
    columns_str = ', '.join([f"`{col}`" for col in available_columns])
    
    create_temp_sql = f"""
    CREATE TABLE {temp_table} AS
    SELECT * FROM VALUES
    {', '.join(first_batch)}
    AS t({columns_str})
    """
    
    print(f"📝 임시 테이블 생성 (첫 {len(first_batch)} rows)...")
    cursor.execute(create_temp_sql)
    print(f"✅ 임시 테이블 생성 완료")
    
    # 나머지 배치 INSERT
    remaining_batches = [values_list[i:i+batch_size] for i in range(batch_size, len(values_list), batch_size)]
    
    for batch_idx, batch in enumerate(remaining_batches):
        insert_batch_sql = f"""
        INSERT INTO {temp_table}
        SELECT * FROM VALUES
        {', '.join(batch)}
        AS t({columns_str})
        """
        print(f"📝 배치 {batch_idx + 2} 삽입 중 ({len(batch)} rows)...")
        cursor.execute(insert_batch_sql)
    
    print(f"✅ 임시 테이블에 총 {len(values_list)} rows 삽입 완료")
    
    # ===== MERGE 실행 (UPSERT) =====
    
    merge_key = "statement_id"
    update_columns = [col for col in available_columns if col != merge_key]
    update_set = ', '.join([f"target.`{col}` = source.`{col}`" for col in update_columns])
    
    insert_columns = ', '.join([f"`{col}`" for col in available_columns])
    insert_values = ', '.join([f"source.`{col}`" for col in available_columns])
    
    merge_sql = f"""
    MERGE INTO datahub.injoy_ops_schema.injoy_monitoring_data AS target
    USING {temp_table} AS source
    ON target.`{merge_key}` = source.`{merge_key}`
    WHEN MATCHED THEN
        UPDATE SET {update_set}
    WHEN NOT MATCHED THEN
        INSERT ({insert_columns})
        VALUES ({insert_values})
    """
    
    print(f"📝 MERGE 실행 중...")
    cursor.execute(merge_sql)
    print("✅ 데이터 UPSERT 완료")
    
    # 임시 테이블 삭제
    cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
    print(f"✅ 임시 테이블 삭제")
    
    cursor.close()
    connection.close()
    
    print(f"✅ Delta 테이블 저장 완료: datahub.injoy_ops_schema.injoy_monitoring_data")
    print(f"✅ 총 {len(values_list)} rows UPSERT됨")
    
    return len(values_list)


# Task 정의
task0 = PythonOperator(
    task_id='tokenize_databricks',
    python_callable=tokenize_databricks,
    op_kwargs={'url': url, 'headers': headers},
    dag=dag,
)

task1 = PythonOperator(
    task_id='extract_audit_logs',
    python_callable=extract_audit_logs,
    dag=dag,
)

task2 = PythonOperator(
    task_id='get_user_groups',
    python_callable=get_user_groups,
    dag=dag,
)

task3 = PythonOperator(
    task_id='enrich_with_groups',
    python_callable=enrich_with_groups,
    dag=dag,
)

task4 = PythonOperator(
    task_id='get_space_info',
    python_callable=get_space_info,
    dag=dag,
)

task5 = PythonOperator(
    task_id='get_message_details',
    python_callable=get_message_details,
    dag=dag,
)

task6 = PythonOperator(
    task_id='merge_query_history',
    python_callable=merge_query_history,
    dag=dag,
)

# Task 의존성 설정
task0 >> [task1, task2] >> task3
[task3, task4] >> task5 >> task6