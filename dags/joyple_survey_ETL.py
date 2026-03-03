import pandas as pd
import pymysql
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from google.oauth2 import service_account
from google.cloud import bigquery
import json
import os


def get_var(key: str, default: str = None) -> str:
    """환경 변수 또는 Airflow Variable 조회"""
    return os.environ.get(key) or Variable.get(key, default_var=default)


# 접속 정보 설정
MYSQL_CONFIG = {
    'host': '10.8.40.96',
    'port': 50013,
    'user': get_var('JOYPLE_SURVEY_USER'),
    'password': get_var('JOYPLE_SURVEY_PW'),
    'db': 'joycity_survey',
    'charset': 'utf8mb4'
}

PROJECT_ID = 'datahub-478802'
DATASET_ID = 'external_data'

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


## joyple_survey_list 데이터를 모두 가져와서 truncate 이후 밀어 넣음
def extract_load_joyple_survey_list():
    # 1. MySQL 데이터 추출 (최근 7일)
    conn = pymysql.connect(**MYSQL_CONFIG)
    
    TABLE_ID = 'joyple_survey_list'
    FINAL_TABLE = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    query = """
        SELECT 
            survey_list_id,
            created_date,
            game_code,
            sort_num_by_game,
            survey_title,
            darkmode_yn,
            vertical_yn,
            question_display_type,
            start_date,
            end_date,
            display_survey_yn,
            temp_save_yn,
            del_yn,
            promotion_id
        FROM joycity_survey.joyple_survey_list 
    """
    df = pd.read_sql(query, conn)
    conn.close()

    if df.empty:
        print("No data found to upload.")
        return

    # 2. BigQuery Staging 테이블로 업로드 (매번 Overwrite)
    client = init_clients()["bq_client"]
    
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    load_job = client.load_table_from_dataframe(df, FINAL_TABLE, job_config=job_config)
    load_job.result()  # 완료 대기
    print(f"✅ WRITE_TRUNCATE {FINAL_TABLE} 업로드 완료.")


## 유저 응답 데이터에 대해서 최근 일주일간의 데이터를 가져온 뒤 upsert 처리
def extract_load_joyple_response():
    conn = pymysql.connect(**MYSQL_CONFIG)

    TABLE_ID = 'joyple_survey_response'
    FINAL_TABLE = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    today = datetime.now().date()
    six_days_ago = today - timedelta(days=6)
    start_date_str = six_days_ago.strftime("%Y-%m-%d")
    
    query = f"""
        SELECT 
            A.response_id 
            , A.survey_list_id
            , A.question_info_id
            , A.option_id
            , A.response
            , A.other_yn
            , A.respondent_id
            , A.created_date
            , B.game_code
            , B.joyple_userkey
            , B.game_userkey
            , B.country
            , B.game_grade_code
            , C.subjective_yn
            , C.answer_limit
            , C.answer_required_yn
            , C.multi_answer_yn
            , C.lang
            , C.question
            , C.question_order
            , C.question_card_uid
        FROM joycity_survey.joyple_survey_response as A
        LEFT JOIN
        joycity_survey.joyple_survey_respondent as B
        ON A.respondent_id = B.respondent_id
        LEFT JOIN 
        joycity_survey.joyple_template_question_info as C
        ON A.question_info_id = C.question_info_id
        WHERE A.created_date >= {start_date_str}
    """

    print(f"Extracting data from {start_date_str}...")
    df = pd.read_sql(query, conn)
    conn.close()

    if df.empty:
        print("No data found to upload.")
        return

    # 2. BigQuery Staging 테이블로 업로드 (매번 Overwrite)
    client = init_clients()["bq_client"]

    # staging 테이블로 데이터 적재 (WRITE_TRUNCATE)
    timestamp_str = datetime.now().strftime("%Y%m%d%H%M%S")
    temp_staging_table_name = f"{TABLE_ID}_{timestamp_str}"
    TEMP_TABLE_FULL_ID = f"{PROJECT_ID}.{DATASET_ID}.{temp_staging_table_name}"

    try:
        # 3. 데이터 업로드 (테이블이 없으면 자동 생성됨)
        # WRITE_TRUNCATE: 테이블이 있으면 비우고 넣고, 없으면 새로 만듭니다.
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        load_job = client.load_table_from_dataframe(df, TEMP_TABLE_FULL_ID, job_config=job_config)
        load_job.result()
        print(f"[{datetime.now()}] 스테이징 테이블 생성 및 업로드 완료: {TEMP_TABLE_FULL_ID}")

        # 4. MERGE 쿼리를 통한 Upsert 처리
        merge_sql = f"""
            MERGE `{FINAL_TABLE}` T
            USING `{TEMP_TABLE_FULL_ID}` S
            ON T.response_id = S.response_id
            WHEN MATCHED THEN
              UPDATE SET 
                T.survey_list_id = S.survey_list_id,
                T.question_info_id = S.question_info_id,
                T.option_id = S.option_id,
                T.response = S.response,
                T.other_yn = S.other_yn,
                T.respondent_id = S.respondent_id,
                T.created_date = S.created_date,
                T.game_code = S.game_code,
                T.joyple_userkey = S.joyple_userkey,
                T.game_userkey = S.game_userkey,
                T.country = S.country,
                T.game_grade_code = S.game_grade_code,
                T.subjective_yn = S.subjective_yn,
                T.answer_limit = S.answer_limit,
                T.answer_required_yn = S.answer_required_yn,
                T.multi_answer_yn = S.multi_answer_yn,
                T.lang = S.lang,
                T.question = S.question,
                T.question_order = S.question_order,
                T.question_card_uid = S.question_card_uid
            WHEN NOT MATCHED THEN
              INSERT (
                response_id,
                survey_list_id,
                question_info_id,
                option_id,
                response,
                other_yn,
                respondent_id,
                created_date,
                game_code,
                joyple_userkey,
                game_userkey,
                country,
                game_grade_code,
                subjective_yn,
                answer_limit,
                answer_required_yn,
                multi_answer_yn,
                lang,
                question,
                question_order,
                question_card_uid
                )
                VALUES (
                S.response_id,
                S.survey_list_id,
                S.question_info_id,
                S.option_id,
                S.response,
                S.other_yn,
                S.respondent_id,
                S.created_date,
                S.game_code,
                S.joyple_userkey,
                S.game_userkey,
                S.country,
                S.game_grade_code,
                S.subjective_yn,
                S.answer_limit,
                S.answer_required_yn,
                S.multi_answer_yn,
                S.lang,
                S.question,
                S.question_order,
                S.question_card_uid
                )
        """
        
        query_job = client.query(merge_sql)
        query_job.result()
        print(f"[{datetime.now()}] 최종 테이블 Upsert 완료.")

    except Exception as e:
        print(f"오류 발생: {e}")
        raise e

    finally:
        # 5. 작업이 성공하든 실패하든 스테이징 테이블 삭제
        client.delete_table(TEMP_TABLE_FULL_ID, not_found_ok=True)
        print(f"[{datetime.now()}] {TEMP_TABLE_FULL_ID} 스테이징 테이블 삭제 완료.")


## 사용자 질문지 데이터를 가져온 뒤 upsert 처리. 전체 데이터를 가져온 뒤 upsert 처리
def extract_load_joyple_question_info():
    conn = pymysql.connect(**MYSQL_CONFIG)

    TABLE_ID = 'joyple_survey_question_info'
    FINAL_TABLE = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    query = f"""
        SELECT  
            C.survey_list_id,
            A.question_info_id,
            A.common_info_id,
            B.option_id,
            A.created_date,
            A.subjective_yn,
            A.answer_limit,
            A.answer_required_yn,
            A.multi_answer_yn,
            A.first_party_data_collect_yn,
            A.del_yn,
            A.lang,
            A.question,
            A.other_yn,
            A.question_order,
            A.question_card_uid,
            B.question_option,
            B.option_order,
            B.option_group_uid
        FROM joycity_survey.joyple_survey_question_info as A
        INNER JOIN
        joycity_survey.joyple_survey_question_option as B
        ON A.question_info_id = B.question_info_id
        INNER  JOIN 
        joycity_survey.joyple_survey_common_info as C
        ON A.common_info_id = C.common_info_id
    """

    print(f"Extracting data from ...")
    df = pd.read_sql(query, conn)
    conn.close()

    if df.empty:
        print("No data found to upload.")
        return

    # 2. BigQuery Staging 테이블로 업로드 (매번 Overwrite)
    client = init_clients()["bq_client"]

    # staging 테이블로 데이터 적재 (WRITE_TRUNCATE)
    timestamp_str = datetime.now().strftime("%Y%m%d%H%M%S")
    temp_staging_table_name = f"{TABLE_ID}_{timestamp_str}"
    # 반드시 전체 경로(Full Path)를 사용해야 합니다.
    TEMP_TABLE_FULL_ID = f"{PROJECT_ID}.{DATASET_ID}.{temp_staging_table_name}"

    try:
        # 3. 데이터 업로드 (테이블이 없으면 자동 생성됨)
        # WRITE_TRUNCATE: 테이블이 있으면 비우고 넣고, 없으면 새로 만듭니다.
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        load_job = client.load_table_from_dataframe(df, TEMP_TABLE_FULL_ID, job_config=job_config)
        load_job.result()
        print(f"[{datetime.now()}] 스테이징 테이블 생성 및 업로드 완료: {TEMP_TABLE_FULL_ID}")

        # 4. MERGE 쿼리를 통한 Upsert 처리
        merge_sql = f"""
            MERGE `{FINAL_TABLE}` T
            USING `{TEMP_TABLE_FULL_ID}` S
                ON T.survey_list_id = S.survey_list_id 
                AND T.question_info_id = S.question_info_id 
                AND T.common_info_id = S.common_info_id 
                AND T.option_id = S.option_id
            WHEN MATCHED THEN
              UPDATE SET 
                T.survey_list_id = S.survey_list_id,
                T.question_info_id = S.question_info_id,
                T.common_info_id = S.common_info_id,
                T.option_id = S.option_id,
                T.created_date = S.created_date,
                T.subjective_yn = S.subjective_yn,
                T.answer_limit = S.answer_limit,
                T.answer_required_yn = S.answer_required_yn,
                T.first_party_data_collect_yn = S.first_party_data_collect_yn,
                T.del_yn = S.del_yn,
                T.lang = S.lang,
                T.question = S.question,
                T.other_yn = S.other_yn,
                T.question_order = S.question_order,
                T.question_card_uid = S.question_card_uid,
                T.question_option = S.question_option,
                T.option_order = S.option_order,
                T.option_group_uid = S.option_group_uid
            WHEN NOT MATCHED THEN
              INSERT (
                response_id,
                survey_list_id,
                question_info_id,
                option_id,
                response,
                other_yn,
                respondent_id,
                created_date,
                game_code,
                joyple_userkey,
                game_userkey,
                country,
                game_grade_code,
                subjective_yn,
                answer_limit,
                answer_required_yn,
                multi_answer_yn,
                lang,
                question,
                question_order,
                question_card_uid
              )
              VALUES (
                  S.response_id, 
                  S.survey_list_id, 
                  S.question_info_id, 
                  S.option_id, 
                  S.response, 
                  S.other_yn, 
                  S.respondent_id, 
                  S.created_date, 
                  S.game_code, 
                  S.joyple_userkey, 
                  S.game_userkey, 
                  S.country, 
                  S.game_grade_code, 
                  S.subjective_yn, 
                  S.answer_limit, 
                  S.answer_required_yn, 
                  S.multi_answer_yn, 
                  S.lang, 
                  S.question, 
                  S.question_order, 
                  S.question_card_uid
              )
        """
        
        query_job = client.query(merge_sql)
        query_job.result()
        print(f"[{datetime.now()}] {TEMP_TABLE_FULL_ID} 최종 테이블 Upsert 완료.")

    except Exception as e:
        print(f"오류 발생: {e}")
        raise e

    finally:
        # 5. 작업이 성공하든 실패하든 스테이징 테이블 삭제
        client.delete_table(TEMP_TABLE_FULL_ID, not_found_ok=True)
        print(f"[{datetime.now()}] 스테이징 테이블 삭제 완료.")


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
    dag_id='joyple_survey_ETL',
    default_args=default_args,
    description='Joyple Survey 데이터를 가져와 BigQuery에 적재',
    schedule= '0 21 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['survey', 'joyple', 'bigquery'],
) as dag:


    extract_load_joyple_survey_list_task = PythonOperator(
        task_id='extract_load_joyple_survey_list',
        python_callable=extract_load_joyple_survey_list,
    )

    extract_load_joyple_response_task = PythonOperator(
        task_id='extract_load_joyple_response',
        python_callable=extract_load_joyple_response,
    )

    extract_load_joyple_question_info_task = PythonOperator(
        task_id='extract_load_joyple_question_info',
        python_callable=extract_load_joyple_question_info,
    )


extract_load_joyple_survey_list_task >> extract_load_joyple_response_task >> extract_load_joyple_question_info_task