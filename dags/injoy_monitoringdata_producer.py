from datetime import datetime, timedelta, timezone as dt_timezone
from io import StringIO
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.models import Variable
import pandas as pd
import requests
from airflow import Dataset
import numpy as np
import json
import pyspark
from airflow.operators.bash import BashOperator

injoy_monitoringdata_producer = Dataset('injoy_monitoringdata_producer')

# 제외 그룹 필터링
exclude_groups = ["Operators", "admins", "users", 
                    "전략사업본부-데이터사이언스실-마케팅사이언스팀", 
                    "전략사업본부-데이터사이언스실-예측모델링팀"]

# DAG 기본 설정
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=10),
}

dag = DAG(
    dag_id='injoy_monitoringdata_producer',
    default_args=default_args,
    description='Process Databricks audit logs for aibiGenie',
    schedule='5 0 * * *',  # 매일 아침 9시 실행
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

url = f"https://{get_databricks_config()['instance']}/api/2.0/token/list"

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

    # 아침 8시에 배치가 진행되기 때문에 intervarl 2days와 1days를 진행
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
            AND DATE(event_time) >= CURRENT_DATE - INTERVAL 120 DAYS
            AND DATE(event_time) < CURRENT_DATE - INTERVAL 0 DAYS
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
    group_url = f"http://{config['instance']}/api/2.0/preview/scim/v2/Groups"
    group_resp = requests.get(group_url, headers=headers)
    
    if group_resp.status_code != 200:
        raise Exception(f"그룹 조회 실패: {group_resp.status_code} - {group_resp.text}")
    
    groups = group_resp.json().get("Resources", [])
    print(f"⏭️ 전체 그룹 리스트: {groups}")
    
    # Step 2: 각 그룹의 구성원 수집 (exclude_groups 제외)
    user_group_list = []
    
    for g in groups:
        group_name = g.get("displayName", "")
        group_id = g.get("id", "")
        
        # exclude_groups에 포함된 그룹은 건너뛰기
        if group_name in exclude_groups:
            print(f"⏭️ 제외된 그룹 건너뛰기: {group_name}")
            continue
        
        group_detail_url = f"https://{config['instance']}/api/2.0/preview/scim/v2/Groups/{group_id}"
        detail_resp = requests.get(group_detail_url, headers=headers)
        
        if detail_resp.status_code != 200:
            print(f"⚠️ 그룹 상세 조회 실패 (group_id={group_id}): {detail_resp.status_code}")
            continue
        
        group_detail = detail_resp.json()
        members = group_detail.get("members", [])
        
        for m in members:
            user_id = m.get("value")
            if user_id:
                user_group_list.append({"user_id": user_id, "group_name": group_name})
    
    # Step 3: DataFrame으로 변환 및 그룹핑
    if not user_group_list:
        print("⚠️ 수집된 사용자-그룹 매핑이 없습니다.")
        df_user_groups = pd.DataFrame(columns=["user_id", "group_name"])
    else:
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
    
    df_audit = pd.read_json(StringIO(ti.xcom_pull(task_ids='extract_audit_logs', key='df_audit')), orient='split')
    df_user_groups = pd.read_json(StringIO(ti.xcom_pull(task_ids='get_user_groups', key='df_user_groups')), orient='split')
    
    # 병합
    df_audit_with_group = df_audit.merge(df_user_groups, on="user_id", how="left")
    
    # group_name이 NaN인 행 제거
    df_audit_with_group = df_audit_with_group.dropna(subset=["group_name"])
    
    print(f"✅ 그룹 정보 병합 완료: {len(df_audit_with_group)} rows")
    print(f"✅ 사용자 그룹 리스트:")
    for _, row in df_audit_with_group.iterrows():
        print(f"  - {row['user_email']} ({row['user_id']})")
    
    context['ti'].xcom_push(key='df_audit_with_group', value=df_audit_with_group.to_json(orient='split', date_format='iso'))
    
    return len(df_audit_with_group)

def get_space_info(**context):
    """
    Task 4: Genie API로 스페이스 정보 수집
    """
    config = get_databricks_config()
    headers = {"Authorization": f"Bearer {config['token']}"}
    
    # 스페이스 목록 조회
    spaces_url = f"https://{config['instance']}/api/2.0/genie/spaces"
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
    df_audit_with_group = pd.read_json(StringIO(ti.xcom_pull(task_ids='enrich_with_groups', key='df_audit_with_group')), orient='split')
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
    row_counts = []
    statuses = []
    descriptions = []
    questionss = []
    auto_regenerate_counts = []
    errors = []
    error_types = []
    feedback_ratings = []
    
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
            row_counts.append(None)
            statuses.append(None)
            descriptions.append(None)
            questionss.append(None)
            auto_regenerate_counts.append(None)
            errors.append(None)
            error_types.append(None)
            feedback_ratings.append(None)
            continue
        
        url = f"https://{config['instance']}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}"
        
        try:
            resp = requests.get(url, headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                
                # Content 처리
                content_raw = data.get("content")
                print(f"⚠️ content 내용 : {content_raw}")
                if isinstance(content_raw, str):
                    content = content_raw.replace("\n", " ")
                else:
                    content = str(content_raw) if content_raw is not None else None

                # Query, description, question 처리
                attachments = data.get("attachments", [])
                if attachments and isinstance(attachments, list):
                    query = attachments[0].get("query", {}).get("query", None)
                    description = attachments[0].get("query", {}).get("description", None)
                    if attachments[0].get("suggested_questions", {}).get("questions"):
                        questions = attachments[0].get("suggested_questions", {}).get("questions", [])
                    elif attachments[1].get("suggested_questions", {}).get("questions"):
                        questions = attachments[1].get("suggested_questions", {}).get("questions", [])

                else:
                    query = None
                    description = None
                    question = None
                
                # Statement ID 처리
                query_result = data.get("query_result")
                if query_result and isinstance(query_result, dict):
                    statement_id = data.get("query_result", {}).get("statement_id")
                    row_count = data.get("query_result", {}).get("row_count")
                else:
                    statement_id = None
                    row_count = None

                if isinstance(data.get("status"), str):
                    status = data.get("status")
                else:
                    status = None

                if isinstance(data.get("auto_regenerate_count"), int):
                    auto_regenerate_count = data.get("auto_regenerate_count")
                else:
                    auto_regenerate_count = None

                error_info = data.get("error", {})
                if error_info and isinstance(error_info, dict):
                    error = error_info.get("error")
                    error_type = error_info.get("type")
                else:
                    error = None
                    error_type = None

                feedback_info = data.get("feedback", {})
                if feedback_info and isinstance(feedback_info, dict):
                    feedback_rating = feedback_info.get("rating")
                else:
                    feedback_rating = None

                print(f"⚠️ questions 내용 : {questions}")
                
            else:
                content, query, description, statement_id, row_count, status, questions, auto_regenerate_count, error, error_type, feedback_rating = None, None, None, None, None, None, None, None, None, None, None
                
        except Exception as e:
            print(f"❌ 예외 발생 ({idx}행): {e}")
            content, query, description, statement_id, row_count, status, questions, auto_regenerate_count, error, error_type, feedback_rating = None, None, None, None, None, None, None, None, None, None, None
        
        contents.append(content)
        queries.append(query)
        statement_ids.append(statement_id)
        row_counts.append(row_count)
        statuses.append(status)
        descriptions.append(description)
        questionss.append(questions)
        auto_regenerate_counts.append(auto_regenerate_count)
        errors.append(error)
        error_types.append(error_type)
        feedback_ratings.append(feedback_rating)
    
    # 데이터 추가
    df_target['content'] = contents
    df_target['query'] = queries
    df_target['statement_id'] = statement_ids
    df_target['row_count'] = row_counts
    df_target['status'] = statuses ## select type
    df_target['description'] = descriptions
    df_target['questions'] = questionss
    df_target['auto_regenerate_count'] = auto_regenerate_counts
    df_target['error'] = errors
    df_target['error_type'] = error_types ## select type
    df_target['feedback_rating'] = feedback_ratings ## select type
    
    print(f"✅ 메시지 상세 정보 수집 완료: {len(df_target)} rows")
    print("✅ df_target 데이터 head 3 : ", df_target.head(3))
    
    context['ti'].xcom_push(key='df_target', value=df_target.to_json(orient='split', date_format='iso'))
    
    return len(df_target)


def merge_query_history(**context):
    """
    Task 6: Query history와 병합 및 최종 데이터 저장
    """
    from databricks import sql
    import numpy as np
    
    ti = context['ti']
    config = get_databricks_config()
    
    # 데이터 가져오기
    json_data = ti.xcom_pull(task_ids='get_message_details', key='df_target')
    df_target = pd.read_json(StringIO(json_data), orient='split')
        
    print(f"📊 df_target 컬럼: {df_target.columns.tolist()}")
    print(f"📊 df_target head:\n{df_target.head(3)}")

    if df_target.empty:
        print("⚠️ df_target이 비어있습니다. 작업을 중단합니다.")
        print("이전 Task('get_message_details')에서 데이터 조회가 실패했을 수 있습니다.")
        return 0  # 또는 raise Exception("df_target is empty")


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
    # ===== 🔥 빈 DataFrame 체크 추가 =====
    if query_df.empty:
        print("⚠️ Query history가 비어있습니다. Query 관련 컬럼을 NULL로 설정합니다.")
        
        # Query 관련 컬럼을 NULL로 추가
        df_audit_enriched = df_target.copy()
        df_audit_enriched['query_end_time_kst'] = None
        df_audit_enriched['query_duration_seconds'] = None
        df_audit_enriched['query_result_fetch_duration_seconds'] = None
        df_audit_enriched['execution_status'] = None
        df_audit_enriched['message_response_duration_seconds'] = None
        
        print(f"✅ Query history 없이 진행: {len(df_audit_enriched)} rows")
        print(f"📊 df_audit_enriched 컬럼: {df_audit_enriched.columns.tolist()}")
    else:
        # 컬럼 rename
        query_df_renamed = query_df.rename(columns={"executed_by": "user_email"})

        df_target['statement_id'] = df_target['statement_id'].astype(str)
        query_df_renamed['statement_id'] = query_df_renamed['statement_id'].astype(str)

        # 병합
        df_audit_enriched = df_target.merge(
            query_df_renamed[[
                "statement_id", "user_email", "query_end_time_kst", 
                "query_duration_seconds", "query_result_fetch_duration_seconds", "execution_status"
            ]],
            how="left",
            on=["statement_id", "user_email"]
        )
        
        print(f"📊 Query history 병합 완료: {len(df_audit_enriched)} rows")
        
        # ===== message_response_duration_seconds 계산 =====
        # event_time_kst와 query_end_time_kst의 차이를 초 단위로 계산
        if 'event_time_kst' in df_audit_enriched.columns and 'query_end_time_kst' in df_audit_enriched.columns:
            # 문자열을 datetime으로 변환
            df_audit_enriched['event_time_kst'] = pd.to_datetime(df_audit_enriched['event_time_kst'])
            df_audit_enriched['query_end_time_kst'] = pd.to_datetime(df_audit_enriched['query_end_time_kst'])
            
            # 시간 차이 계산 (초 단위)
            df_audit_enriched['message_response_duration_seconds'] = (
                df_audit_enriched['query_end_time_kst'] - df_audit_enriched['event_time_kst']
            ).dt.total_seconds()
            
            print(f"✅ message_response_duration_seconds 계산 완료")
        else:
            # 컬럼이 없으면 NULL로 추가
            df_audit_enriched['message_response_duration_seconds'] = None
            print(f"⚠️ event_time_kst 또는 query_end_time_kst 컬럼이 없어 message_response_duration_seconds를 NULL로 설정")
        
        print(f"📋 DataFrame 컬럼: {df_audit_enriched.columns.tolist()}")
        
        # ===== 타겟 테이블 스키마 조회 =====
        target_table = "datahub.injoy_ops_schema.injoy_monitoring_data"
        temp_table = "datahub.injoy_ops_schema.temp_merge_data"
        
        # 타겟 테이블의 컬럼 목록 및 타입 조회
        cursor.execute(f"DESCRIBE TABLE {target_table}")
        table_schema = cursor.fetchall_arrow().to_pandas()
        
        print(f"📋 타겟 테이블 스키마:")
        print(table_schema[['col_name', 'data_type']].to_string())
        
        # 컬럼명을 키로, 데이터 타입을 값으로 하는 딕셔너리 생성
        target_column_types = dict(zip(table_schema['col_name'], table_schema['data_type']))
        target_columns = table_schema['col_name'].tolist()
        
        # DataFrame과 타겟 테이블 공통 컬럼만 선택
        df_columns = df_audit_enriched.columns.tolist()
        common_columns = [col for col in df_columns if col in target_columns]
        missing_in_target = [col for col in df_columns if col not in target_columns]
        missing_in_df = [col for col in target_columns if col not in df_columns]
        
        if missing_in_target:
            print(f"⚠️ 타겟 테이블에 없는 컬럼 (제외됨): {missing_in_target}")
        
        if missing_in_df:
            print(f"⚠️ DataFrame에 없지만 타겟 테이블에 있는 컬럼: {missing_in_df}")
            # 타겟 테이블에는 있지만 DataFrame에 없는 컬럼을 NULL로 추가
            for col in missing_in_df:
                if col not in ['id', 'created_at', 'updated_at']:  # 자동 생성 컬럼 제외
                    df_audit_enriched[col] = None
                    common_columns.append(col)
                    print(f"   → {col} 컬럼을 NULL로 추가")
        
        # 공통 컬럼만 사용하여 데이터 필터링
        df_to_insert = df_audit_enriched[common_columns].copy()
        
        print(f"✅ 사용할 컬럼 ({len(common_columns)}개): {common_columns}")
        
        # ===== 임시 테이블 생성 (타겟 테이블의 실제 타입 사용) =====
        
        # 기존 임시 테이블 삭제
        cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
        
        # 타겟 테이블의 타입을 사용하여 CREATE TABLE 문 생성
        column_definitions = []
        for col in common_columns:
            sql_type = target_column_types.get(col, 'STRING')  # 타겟 테이블의 실제 타입 사용
            column_definitions.append(f"`{col}` {sql_type}")
        
        create_table_sql = f"""
        CREATE TABLE {temp_table} (
            {', '.join(column_definitions)}
        )
        USING DELTA
        """
        
        print(f"📝 임시 테이블 생성 SQL:")
        print(create_table_sql)
        
        cursor.execute(create_table_sql)
        print(f"✅ 임시 테이블 생성 완료: {temp_table}")
        
        # 생성된 임시 테이블 스키마 확인
        cursor.execute(f"DESCRIBE TABLE {temp_table}")
        temp_schema = cursor.fetchall_arrow().to_pandas()
        print(f"📋 생성된 임시 테이블 스키마:")
        print(temp_schema[['col_name', 'data_type']].to_string())
        
        # DataFrame을 batch INSERT
        columns = common_columns
        column_str = ", ".join([f"`{col}`" for col in columns])
        
        # NULL 값을 SQL NULL로 변환하는 함수
        def convert_value(val, col_name):
            """
            값을 SQL 문자열로 변환
            col_name: 타입 체크를 위한 컬럼명
            """
            # None 체크 먼저
            if val is None:
                return "NULL"
            # pandas NA 타입 체크
            if pd.isna(val) if not isinstance(val, (list, tuple, np.ndarray)) else False:
                return "NULL"
            # numpy nan 체크
            try:
                if np.isnan(val):
                    return "NULL"
            except (TypeError, ValueError):
                pass
            
            # 타겟 테이블의 타입 확인
            target_type = target_column_types.get(col_name, '').lower()
            
            # ARRAY 타입 처리
            if 'array' in target_type:
                if isinstance(val, (list, tuple)):
                    # 리스트를 SQL ARRAY로 변환
                    array_elements = [f"'{str(v).replace(chr(39), chr(39)+chr(39))}'" for v in val]
                    return f"ARRAY({', '.join(array_elements)})"
                elif isinstance(val, str):
                    # 문자열을 단일 요소 배열로 변환
                    escaped = val.replace("'", "''")
                    return f"ARRAY('{escaped}')"
                else:
                    return "NULL"
            
            # 타입별 처리
            if isinstance(val, str):
                # SQL Injection 방지를 위해 escape 처리
                escaped = val.replace("'", "''").replace("\\", "\\\\")
                return f"'{escaped}'"
            elif isinstance(val, bool):
                return str(val).upper()  # TRUE/FALSE
            elif isinstance(val, (int, float, np.integer, np.floating)):
                return str(val)
            elif isinstance(val, (pd.Timestamp, np.datetime64)):
                try:
                    ts = pd.Timestamp(val)
                    return f"'{ts.strftime('%Y-%m-%d %H:%M:%S')}'"
                except:
                    return "NULL"
            else:
                # 기타 타입은 문자열로 변환
                try:
                    escaped = str(val).replace("'", "''").replace("\\", "\\\\")
                    return f"'{escaped}'"
                except:
                    return "NULL"
        
        # 배치 단위로 INSERT
        batch_size = 1000
        total_rows = len(df_to_insert)
        
        for start_idx in range(0, total_rows, batch_size):
            end_idx = min(start_idx + batch_size, total_rows)
            batch_df = df_to_insert.iloc[start_idx:end_idx]
            
            values_list = []
            for idx, row in batch_df.iterrows():
                try:
                    row_values = ", ".join([convert_value(row[col], col) for col in columns])
                    values_list.append(f"({row_values})")
                except Exception as e:
                    print(f"⚠️ Row {idx} 변환 실패: {e}")
                    print(f"   문제 데이터: {row.to_dict()}")
                    raise
            
            values_str = ", ".join(values_list)
            
            insert_sql = f"""
            INSERT INTO {temp_table} ({column_str})
            VALUES {values_str}
            """
            
            try:
                cursor.execute(insert_sql)
                print(f"📝 배치 INSERT 완료: {start_idx+1}-{end_idx}/{total_rows}")
            except Exception as e:
                print(f"⚠️ INSERT 실패 at batch {start_idx}-{end_idx}")
                print(f"   SQL 미리보기: {insert_sql[:500]}...")
                raise
        
        print(f"✅ 임시 테이블 데이터 적재 완료: {total_rows} rows")
        
        # ===== MERGE 실행 =====
        merge_key = 'message_id'
        
        # 동적으로 컬럼 리스트 생성
        update_set = ", ".join([f"target.`{col}` = source.`{col}`" for col in columns if col != merge_key])
        insert_columns = ", ".join([f"`{col}`" for col in columns])
        insert_values = ", ".join([f"source.`{col}`" for col in columns])
        
        merge_sql = f"""
        MERGE INTO {target_table} AS target
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
        print(f"🗑️ 임시 테이블 삭제 완료")
        
        cursor.close()
        connection.close()
        
        print(f"✅ Delta 테이블 저장 완료")
        
        return len(df_to_insert)

# Task 정의
bash_task = BashOperator(
    task_id = 'bash_task',
    outlets = [injoy_monitoringdata_producer],
    bash_command = 'echo "producer_1 수행 완료"'
)

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
[task3, task4] >> task5 >> task6 >> bash_task