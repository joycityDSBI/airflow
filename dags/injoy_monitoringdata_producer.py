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
    dag_id='injoy_monitoringdata_producer_ver2',
    default_args=default_args,
    description='Process Databricks audit logs for aibiGenie ver2 modifing',
    schedule='5 0 * * *',  # 매일 아침 9시 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['databricks', 'monitoring', 'staging'],
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
    merge_query = f"""
        MERGE INTO datahub.injoy_ops_schema.injoy_monitoring_audit AS target
        USING (
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
                    AND DATE(event_time) >= CURRENT_DATE - INTERVAL 3 DAYS
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
                WHERE action_name = 'createConversationMessage'
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
                a.log_type,
                a.user_email,
                a.user_id,
                a.action_name,
                a.space_id,
                a.conversation_id,
                a.message_id,
                b.feedback_rating,
                a.event_time_kst,
                c.user_name 
            FROM message_tb AS a
            LEFT JOIN feedback_rb AS b
                ON a.space_id = b.space_id
                AND a.conversation_id = b.conversation_id
                AND a.message_id = b.message_id
            LEFT JOIN (SELECT DISTINCT email, user_name FROM datahub.injoy_ops_schema.user_permission_snapshot) AS c
                ON a.user_email = c.email
        ) AS source
        ON target.space_id = source.space_id 
           AND target.conversation_id = source.conversation_id 
           AND target.message_id = source.message_id
        
        WHEN MATCHED THEN
          UPDATE SET 
            target.feedback_rating = source.feedback_rating,
            target.user_name = source.user_name,
            target.event_time_kst = source.event_time_kst
            
        WHEN NOT MATCHED THEN
          INSERT *;
        """

    try:
            # MERGE 실행
            cursor = connection.cursor()
            cursor.execute(merge_query)

            # 2. 이번 배치에서 처리된 Key 리스트만 별도로 추출하여 XCom에 저장
            # MERGE가 완료된 후, 최신 event_time_kst 기준으로 처리된 ID들만 가져옵니다.
            # (필요에 따라 WHERE 조건을 조정하세요)
            key_extract_query = """
            SELECT distinct space_id, conversation_id, message_id, user_email, user_id, user_name  
            FROM datahub.injoy_ops_schema.injoy_monitoring_audit
            -- WHERE event_time_kst >= CURRENT_TIMESTAMP - INTERVAL 1 HOUR
            """
            cursor.execute(key_extract_query)
            df_keys = cursor.fetchall_arrow().to_pandas()

            # 리스트 형태로 변환 (ex: [{'space_id': '...', 'message_id': '...'}, ...])
            merge_key_list = df_keys.to_dict(orient='records')

            # XCom으로 Key 리스트 전달
            context['ti'].xcom_push(key='merge_key_list', value=merge_key_list)
            print(f"✅ MERGE 완료 및 {len(merge_key_list)}개의 Key 리스트 XCom 저장 완료")

    finally:
        cursor.close()
        connection.close()

    return len(merge_key_list)


def get_user_groups(**context):
    """
    Task 2: SCIM API로 그룹 및 사용자 정보 수집
    """
    config = get_databricks_config()
    headers = {"Authorization": f"Bearer {config['token']}"}
    
    # Step 1: 그룹 전체 목록 조회
    group_url = f"https://{config['instance']}/api/2.0/preview/scim/v2/Groups"
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
    
    df_audit = pd.read_json(StringIO(ti.xcom_pull(task_ids='extract_audit_logs', key='merge_key_list')), orient='split')
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
    
    # XCom에서 데이터 가져오기
    # 1. XCom에서 리스트 데이터 그대로 가져오기
    merge_keys = ti.xcom_pull(task_ids='extract_audit_logs', key='merge_key_list')
    
    # 2. StringIO나 read_json 없이 바로 데이터프레임으로 변환!
    df_audit = pd.DataFrame(merge_keys)
    
    # (참고) 아래 코드는 JSON 문자열로 넘어오기 때문에 원래 쓰시던 방식 그대로 두시면 됩니다.
    df_user_groups = pd.read_json(StringIO(ti.xcom_pull(task_ids='get_user_groups', key='df_user_groups')), orient='split')
    
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
    
    if not df_target.empty:
        print("🔄 Databricks MERGE 작업 시작...")
        
        target_table = "datahub.injoy_ops_schema.injoy_monitoring_api_message_details"
        staging_table = f"{target_table}_staging" # 임시 스테이징 테이블
        
        # DataFrame의 NaN, None 값을 SQL 호환을 위해 처리
        df_target = df_target.where(pd.notnull(df_target), None)

        try:
            # SQLAlchemy 연동이 되어 있다면 df_target.to_sql() 을 쓰는 것이 가장 편리합니다.
            # 아래는 SQLAlchemy 엔진(engine)이 준비되어 있다고 가정한 코드입니다.
            # (만약 databricks-sql-connector를 직접 쓴다면 executemany 등을 활용해야 합니다.)
            from sqlalchemy import create_engine
            
            # Databricks SQLAlchemy URI 생성 (Airflow Connection을 가져와도 됨)
            # http_path는 Databricks SQL Warehouse 또는 Cluster의 연결 정보 탭에서 확인 가능합니다.
            db_uri = f"databricks://token:{config['token']}@{config['instance']}?http_path={config.get('http_path', 'YOUR_HTTP_PATH')}"
            engine = create_engine(db_uri)

            # 1. Staging 테이블에 데이터 삽입 (기존 데이터 덮어쓰기)
            print(" 1️⃣ 스테이징 테이블에 데이터 적재 중...")
            df_target.to_sql(
                name="injoy_monitoring_api_message_details_staging", 
                con=engine, 
                schema="datahub.injoy_ops_schema", 
                if_exists="replace", 
                index=False
            )

            # 2. MERGE INTO 쿼리 실행
            print(" 2️⃣ 타겟 테이블에 MERGE 수행 중...")
            merge_query = f"""
            MERGE INTO {target_table} AS target
            USING {staging_table} AS source
            ON target.space_id = source.space_id
               AND target.conversation_id = source.conversation_id
               AND target.message_id = source.message_id
            WHEN MATCHED THEN
              UPDATE SET *
            WHEN NOT MATCHED THEN
              INSERT *
            """
            
            with engine.begin() as connection:
                connection.execute(merge_query)
                
            # 3. (선택) 스테이징 테이블 삭제
            print(" 3️⃣ 스테이징 테이블 정리 중...")
            with engine.begin() as connection:
                connection.execute(f"DROP TABLE IF EXISTS {staging_table}")
                
            print("✅ Databricks MERGE 작업 완료!")

        except Exception as e:
            print(f"❌ MERGE 작업 중 오류 발생: {e}")
            raise e
    else:
        print("⚠️ MERGE할 데이터가 없습니다 (df_target이 비어 있음).")


# Task 정의
# bash_task = BashOperator(
#     task_id = 'bash_task',
#     outlets = [injoy_monitoringdata_producer],
#     bash_command = 'echo "producer_1 수행 완료"'
# )

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


# Task 의존성 설정
task0 >> [task1, task2] >> task3
[task3, task4] >> task5 ## >> task6 >> bash_task