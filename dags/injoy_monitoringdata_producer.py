from datetime import datetime, timedelta, timezone as dt_timezone
from io import StringIO
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.models import Variable
import pandas as pd
import requests
from airflow import Dataset
import numpy as np
import json
import pyspark
from databricks import sql

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
    description='Process Databricks audit logs for aibiGenie ver2',
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
    """Task 1 : Databricks Genie audit 로그 추출 및 처리"""
    from databricks import sql
    
    config = get_databricks_config()
    ds = context.get('ds')
    connection = sql.connect(
        server_hostname=config['instance'].replace('https://', ''),
        http_path=Variable.get('databricks_http_path'),
        access_token=config['token']
    )

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
                    AND event_time >= CAST(DATE(NOW()) - INTERVAL 3 DAYS AS TIMESTAMP) - INTERVAL 9 HOURS
                    AND event_time < CAST(DATE(NOW()) AS TIMESTAMP) - INTERVAL 9 HOURS
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

            # 💡 영향을 받은 행(row)의 수를 가져옵니다.
            affected_rows = cursor.rowcount

            print("-" * 50)
            print(f"✅ query 실행 성공!")
            print(f"📊 처리된 데이터 수: {affected_rows} rows (Inserted/Updated)")
            print("-" * 50)

            # 2. 이번 배치에서 처리된 Key 리스트만 별도로 추출하여 XCom에 저장
            # MERGE가 완료된 후, 최신 event_time_kst 기준으로 처리된 ID들만 가져옵니다.
            # (필요에 따라 WHERE 조건을 조정하세요)
            key_extract_query = """
            SELECT distinct space_id, conversation_id, message_id, user_email, user_id, user_name  
            FROM datahub.injoy_ops_schema.injoy_monitoring_audit
            WHERE DATE(event_time_kst) >= DATE(NOW()) - INTERVAL 3 DAYS
            AND   event_time_kst < DATE(NOW())
            """
            cursor.execute(key_extract_query)
            df_keys = cursor.fetchall_arrow().to_pandas()

            # 리스트 형태로 변환 (ex: [{'space_id': '...', 'message_id': '...'}, ...])
            merge_key_list = df_keys.to_dict(orient='records')

            # XCom으로 Key 리스트 전달
            context['ti'].xcom_push(key='merge_key_list', value=merge_key_list)
            print(f"✅ MERGE 완료 및 {len(merge_key_list)}개의 Key 리스트 XCom 저장 완료") 

            keys_info = "\n".join([f"({k['space_id']} / {k['conversation_id']} / {k['message_id']})" for k in merge_key_list])
            print(f"📍 대상 ID 리스트 (space/conv/msg): {keys_info}")
    finally:
        cursor.close()
        connection.close() 

    return len(merge_key_list)

def get_message_details(**context):
    """
    Task 2: Genie API로 메시지 상세 정보 수집 -> Databricks 테이블에 저장.(datahub.injoy_ops_schema.injoy_monitoring_api_message_details)
    """
    ti = context['ti']
    config = get_databricks_config()
    
    # 1. 데이터 가져오기
    merge_key_list = ti.xcom_pull(task_ids='extract_audit_logs', key='merge_key_list')
    
    if not merge_key_list:
        print("ℹ️ 처리할 메시지 키(Key) 데이터가 없습니다.")
        return 0

    headers = {"Authorization": f"Bearer {config['token']}"}
    instance_host = config['instance'].replace('https://', '')
    api_results = []


    # 2. 각 Key를 순회하며 API 호출
    for row in merge_key_list:
        space_id = row['space_id']
        conversation_id = row['conversation_id']
        message_id = row['message_id']
        
        url = f"https://{instance_host}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}"
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()  # 200 OK가 아닐 경우 에러 발생
            
            # 응답받은 JSON을 문자열 형태로 변환하여 저장
            api_json_str = json.dumps(response.json(), ensure_ascii=False)
            
            api_results.append({
                "space_id": space_id,
                "conversation_id": conversation_id,
                "message_id": message_id,
                "user_id": row.get('user_id'),
                "user_name": row.get('user_name'),
                "user_email": row.get('user_email'),
                "api_response": api_json_str
            })
        except Exception as e:
            print(f"❌ API 호출 실패 | [Space: {space_id}] [Conv: {conversation_id}] [Msg: {message_id}] | Error: {e}")
            # 에러가 나더라도 다른 메시지는 계속 처리할 수 있도록 pass

    if not api_results:
        print("ℹ️ API 호출 성공 데이터가 없어 적재를 건너뜁니다.")
        return 0

    # 3. Databricks 테이블에 결과 저장 (MERGE INTO 구문 활용)
    if api_results:
    # 1. 리스트를 Pandas DataFrame으로 변환
        df_api = pd.DataFrame(api_results)

        connection = sql.connect(
            server_hostname=instance_host,
            http_path=Variable.get('databricks_http_path'),
            access_token=config['token']
        )

    try:
        cursor = connection.cursor()
        
        # 2. 임시 스테이징 테이블(또는 임시 뷰) 생성
        staging_table = "datahub.injoy_ops_schema.temp_genie_api_staging"
        cursor.execute(f"DROP TABLE IF EXISTS {staging_table}")
        
        # 데이터를 튜플 형태로 변환하여 한 번에 삽입 (Bulk insert)
        values_to_insert = [tuple(x) for x in df_api.values]
        
        # 테이블 생성
        cursor.execute(f"""
            CREATE TABLE {staging_table} (
                space_id STRING, conversation_id STRING, message_id STRING, 
                user_id STRING, user_name STRING, user_email STRING, api_response STRING
            )
        """)
        
        insert_sql = f"INSERT INTO {staging_table} VALUES (?, ?, ?, ?, ?, ?, ?)"
        cursor.executemany(insert_sql, values_to_insert)

        final_merge_query = f"""
        MERGE INTO datahub.injoy_ops_schema.injoy_monitoring_api_message_details AS target
        USING {staging_table} AS source
        ON target.space_id = source.space_id 
           AND target.conversation_id = source.conversation_id 
           AND target.message_id = source.message_id

        -- 💡 매칭될 경우 (기존 데이터 업데이트 + 시간 갱신)
        WHEN MATCHED THEN
            UPDATE SET 
                target.user_id = source.user_id,
                target.user_name = source.user_name,
                target.user_email = source.user_email,
                target.api_response = source.api_response,
                target.update_datetime_kst = NOW() + INTERVAL 9 HOURS

        -- 💡 매칭되지 않을 경우 (새 데이터 인서트 + 시간 기록)
        WHEN NOT MATCHED THEN
            INSERT (
                space_id, 
                conversation_id, 
                message_id, 
                user_id, 
                user_name, 
                user_email, 
                api_response, 
                update_datetime_kst
            ) 
            VALUES (
                source.space_id, 
                source.conversation_id, 
                source.message_id, 
                source.user_id, 
                source.user_name, 
                source.user_email, 
                source.api_response, 
                NOW() + INTERVAL 9 HOURS
            );
        """
        cursor.execute(final_merge_query)

        # 💡 영향을 받은 행(row)의 수를 가져옵니다.
        affected_rows = cursor.rowcount

        print("-" * 50)
        print(f"✅ query 실행 성공!")
        print(f"📊 처리된 데이터 수: {affected_rows} rows (Inserted/Updated)")
        print("-" * 50)

        # 임시 테이블 삭제
        cursor.execute(f"DROP TABLE IF EXISTS {staging_table}")
        
    finally:
        cursor.close()
        connection.close()

    return len(api_results)

def get_space_list(**context):
    """
    Task 3: Genie API로 스페이스 정보 수집 및 DB 적재(datahub.injoy_ops_schema.injoy_space_list)
    """
    config = get_databricks_config()
    headers = {"Authorization": f"Bearer {config['token']}"}
    
    # 1. 스페이스 목록 조회 API 호출
    spaces_url = f"https://{config['instance']}/api/2.0/genie/spaces"
    resp = requests.get(spaces_url, headers=headers)
    
    connection = sql.connect(
        server_hostname=config['instance'].replace('https://', ''),
        http_path=Variable.get('databricks_http_path'),
        access_token=config['token']
    )

    if resp.status_code != 200:
        print(f"⚠️ 스페이스 목록 조회 실패: {resp.status_code}")
        return 0
        
    spaces = resp.json().get("spaces", [])
    if not spaces:
        print("⚠️ 수집된 스페이스가 없습니다.")
        return 0

    # 2. SQL 구문에 들어갈 다중 VALUES 텍스트 생성
    values_list = []
    space_id_to_name = {}
    
    for s in spaces:
        space_id = s.get("space_id")
        raw_name = s.get("title", "")
        space_id_to_name[space_id] = raw_name
        
        # 💡 SQL 에러 방지: 스페이스 이름에 홑따옴표(')가 있을 경우 탈출(Escape) 처리
        safe_name = raw_name.replace("'", "''") 
        values_list.append(f"('{space_id}', '{safe_name}')")
        
    values_str = ",\n        ".join(values_list)

    # 3. MERGE 쿼리 작성 (Upsert)
    query = f"""
    MERGE INTO datahub.injoy_ops_schema.injoy_space_list AS target
    USING (
        SELECT * FROM VALUES
        {values_str}
        AS t(space_id, space_name)
    ) AS source
    ON target.space_id = source.space_id
    
    -- 기존에 존재하는 스페이스면 무조건 이름과 마지막 수집 시간(KST)을 갱신
    WHEN MATCHED THEN 
        UPDATE SET 
            target.space_name = source.space_name,
            target.update_datetime_kst = from_utc_timestamp(current_timestamp(), 'Asia/Seoul')
            
    -- 새롭게 발견된 스페이스일 경우 인서트
    WHEN NOT MATCHED THEN 
        INSERT (space_id, space_name, update_datetime_kst) 
        VALUES (source.space_id, source.space_name, from_utc_timestamp(current_timestamp(), 'Asia/Seoul'))
    """
    
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        # 💡 영향을 받은 행(row)의 수를 가져옵니다.
        affected_rows = cursor.rowcount

        print("-" * 50)
        print(f"✅ query 실행 성공!")
        print(f"📊 처리된 데이터 수: {affected_rows} rows (Inserted/Updated)")
        print("-" * 50)
        
    except Exception as e:
        print(f"❌ 스페이스 정보 적재 중 오류 발생: {e}")
        raise e
        
    finally:
        cursor.close()
        connection.close()
    
    return len(spaces)


def extract_audit_query(**context):
    """
    task4 : Databricks Query History(Genie Space 관련 쿼리) 추출 및 테이블 적재(datahub.injoy_ops_schema.injoy_monitoring_audit_query)
    """
    config = get_databricks_config()
    connection = sql.connect(
        server_hostname=config['instance'].replace('https://', ''),
        http_path=Variable.get('databricks_http_path'),
        access_token=config['token']
    )

    # query history 적재를 위한 MERGE 쿼리
    query_history_merge = f"""
        MERGE INTO datahub.injoy_ops_schema.injoy_monitoring_audit_query AS target
        USING (
            SELECT 
                statement_id, 
                executed_by, 
                execution_status, 
                CAST(total_duration_ms AS DOUBLE) / 1000 AS query_duration_seconds, 
                CAST(result_fetch_duration_ms AS DOUBLE) / 1000 AS query_result_fetch_duration_seconds, 
                CAST(end_time AS TIMESTAMP) + INTERVAL 9 HOURS AS query_end_time_kst,
                query_source.genie_space_id as space_id
            FROM system.query.history
            WHERE query_source.genie_space_id IS NOT NULL
                AND statement_type = 'SELECT'
                AND end_time >= CAST(DATE(NOW()) - INTERVAL 3 DAYS AS TIMESTAMP) - INTERVAL 9 HOURS
                AND end_time < CAST(DATE(NOW()) AS TIMESTAMP) - INTERVAL 9 HOURS
        ) AS source
        ON target.statement_id = source.statement_id
        
        WHEN MATCHED THEN
          UPDATE SET 
            target.execution_status = source.execution_status,
            target.query_duration_seconds = source.query_duration_seconds,
            target.query_result_fetch_duration_seconds = source.query_result_fetch_duration_seconds
            
        WHEN NOT MATCHED THEN
          INSERT *;
    """

    try:
        cursor = connection.cursor()
        cursor.execute(query_history_merge)
        # 💡 영향을 받은 행(row)의 수를 가져옵니다.
        affected_rows = cursor.rowcount

        print("-" * 50)
        print(f"✅ query 실행 성공!")
        print(f"📊 처리된 데이터 수: {affected_rows} rows (Inserted/Updated)")
        print("-" * 50)
        
    except Exception as e:
        print(f"❌ Query History 적재 중 오류 발생: {e}")
        raise e
        
    finally:
        cursor.close()
        connection.close()

def processing_message_details(**context):
    """
    task5 : api responce 데이터 파싱하여 컬럼 생성하여 테이블저장
    """
    config = get_databricks_config()
    connection = sql.connect(
        server_hostname=config['instance'].replace('https://', ''),
        http_path=Variable.get('databricks_http_path'),
        access_token=config['token']
    )

    query = f"""
    MERGE INTO datahub.injoy_ops_schema.injoy_monitoring_api_message_details_parsing AS target
    USING (
        SELECT 
            space_id,
            conversation_id,
            message_id,
            user_id,
            user_name,
            user_email,
            update_datetime_kst,
            -- 기존 원본 테이블에 있고 파싱 테이블에도 남겨야 하는 컬럼이 있다면 추가 (예: created_at 등)
            get_json_object(api_response, '$.attachments[0].query.description')                     AS description, 
            get_json_object(api_response, '$.attachments[0].query.query')                           AS query,
            get_json_object(api_response, '$.attachments[0].query.query_result_metadata.row_count') AS row_count,
            get_json_object(api_response, '$.attachments[0].query.statement_id') AS statement_id,
            get_json_object(api_response, '$.attachments[1].suggested_questions.questions') AS questions,
            get_json_object(api_response, '$.content') AS content,
            get_json_object(api_response, '$.status') AS status,
            get_json_object(api_response, '$.auto_regenerate_count') AS auto_regenerate_count,
            get_json_object(api_response, '$.error.error') AS error,
            get_json_object(api_response, '$.error.type') AS error_type,
            get_json_object(api_response, '$.feedback.rating') AS feedback_rating
        FROM datahub.injoy_ops_schema.injoy_monitoring_api_message_details
        WHERE DATE(update_datetime_kst) = DATE(NOW()) 
    ) AS source
    ON target.space_id = source.space_id 
       AND target.conversation_id = source.conversation_id 
       AND target.message_id = source.message_id
       
    WHEN MATCHED THEN
        UPDATE SET *
        
    WHEN NOT MATCHED THEN
        INSERT *
    """
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        
        # 💡 영향을 받은 행(row)의 수를 가져옵니다.
        affected_rows = cursor.rowcount

        print("-" * 50)
        print(f"✅ query 실행 성공!")
        print(f"📊 처리된 데이터 수: {affected_rows} rows (Inserted/Updated)")
        print("-" * 50)

    except Exception as e:
        print(f"❌ Query History 적재 중 오류 발생: {e}")
        raise e
        
    finally:
        cursor.close()
        connection.close()


def merge_final_monitoring_data(**context):
    """
    Task 4: 여러 테이블을 조인하여 최종 모니터링 데이터(injoy_monitoring_data)에 MERGE
    """
    config = get_databricks_config()
    connection = sql.connect(
        server_hostname=config['instance'].replace('https://', ''),
        http_path=Variable.get('databricks_http_path'),
        access_token=config['token']
    )

    # 작성해주신 조인 쿼리를 USING (source) 절에 삽입
    query = """
    MERGE INTO datahub.injoy_ops_schema.injoy_monitoring_data AS target
    USING (
        SELECT 
            b.content,
            a.user_email,
            a.user_id,
            a.user_name,
            c.space_name,
            a.space_id,
            a.conversation_id,
            a.message_id,
            b.query,
            CAST(UNIX_TIMESTAMP(d.query_end_time_kst) - UNIX_TIMESTAMP(a.event_time_kst) AS INT) AS message_response_duration_seconds,
            a.event_time_kst,
            b.row_count,
            b.status,
            b.description,
            b.questions,
            b.auto_regenerate_count,
            b.error,
            b.error_type,
            b.feedback_rating
        FROM datahub.injoy_ops_schema.injoy_monitoring_audit AS a
        LEFT JOIN datahub.injoy_ops_schema.injoy_monitoring_api_message_details_parsing AS b
            ON a.space_id = b.space_id 
            AND a.conversation_id = b.conversation_id 
            AND a.message_id = b.message_id
        LEFT JOIN datahub.injoy_ops_schema.injoy_space_list AS c
            ON a.space_id = c.space_id
        LEFT JOIN datahub.injoy_ops_schema.injoy_monitoring_audit_query AS d
            ON b.statement_id = d.statement_id
        WHERE DATE(A.event_time_kst) >= DATE(NOW()) - INTERVAL 3 DAYS
        AND   DATE(A.event_time_kst) < DATE(NOW())             
    ) AS source
    ON target.space_id = source.space_id 
       AND target.conversation_id = source.conversation_id 
       AND target.message_id = source.message_id
       
    -- 기존에 존재하는 메시지면 업데이트
    WHEN MATCHED THEN
        UPDATE SET *
        
    -- 새로운 메시지면 인서트
    WHEN NOT MATCHED THEN
        INSERT *
    """

    try:
        cursor = connection.cursor()
        cursor.execute(query)
        # 💡 영향을 받은 행(row)의 수를 가져옵니다.
        affected_rows = cursor.rowcount

        print("-" * 50)
        print(f"✅ query 실행 성공!")
        print(f"📊 처리된 데이터 수: {affected_rows} rows (Inserted/Updated)")
        print("-" * 50)
        
    except Exception as e:
        print(f"❌ 최종 데이터 MERGE 중 오류 발생: {e}")
        raise e
        
    finally:
        cursor.close()
        connection.close()

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
    task_id='get_message_details',
    python_callable=get_message_details,
    dag=dag,
)

task3 = PythonOperator(
    task_id='get_space_list',
    python_callable=get_space_list,
    dag=dag,
)

task4 = PythonOperator(
    task_id='extract_audit_query',
    python_callable=extract_audit_query,
    dag=dag,
)

task5 = PythonOperator(
    task_id='processing_message_details',
    python_callable= processing_message_details,
    dag=dag,
)

task6 = PythonOperator(
    task_id='merge_final_monitoring_data',
    python_callable= merge_final_monitoring_data,
    dag=dag,
)


# Task 의존성 설정
task0 >> task1 >> [task2, task3, task4]
task2 >> task5 >> task6