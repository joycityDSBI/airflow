from airflow import DAG, Dataset
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import timedelta, datetime
import os
from airflow.models import Variable
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import json


# 환경 변수 가져오기
def get_var(key: str, default: str = None) -> str:
    """환경 변수 또는 Airflow Variable 조회"""
    return os.environ.get(key) or Variable.get(key, default_var=default)

def get_bigquery_client():
    """BigQuery 클라이언트 생성"""
    CREDENTIALS_JSON = get_var('GOOGLE_CREDENTIAL_JSON')
    cred_dict = json.loads(CREDENTIALS_JSON)
    
    # ✅ private_key의 \\n을 실제 줄바꿈으로 변환
    if 'private_key' in cred_dict:
        cred_dict['private_key'] = cred_dict['private_key'].replace('\\n', '\n')
    
    credentials = service_account.Credentials.from_service_account_info(
        cred_dict,
        scopes=[
            'https://www.googleapis.com/auth/bigquery',
            'https://www.googleapis.com/auth/drive.readonly',
            ]
    )
    
    client = bigquery.Client(
        credentials=credentials,
        project='data-science-division-216308'
    )
    
    return client

# 사용
client = get_bigquery_client()


## DAG 설정
gcs_creative_file_upload = Dataset('gcs_creative_file_upload')

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

with DAG(
    dag_id='bq_create_ms_table',
    default_args=default_args,
    description='BigQuery 테이블 생성 예시',
    schedule=[gcs_creative_file_upload],
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['bigquery', 'create_table'],
) as dag:

    def POTC_standard_creative_list(**context):
        query = """
        CREATE OR REPLACE EXTERNAL TABLE `data-science-division-216308.Creative.POTC_standard_creative_list`
        (
            category STRING,
            subcategory STRING,
            type STRING,
            ai STRING,
            horizontal INT64,
            vertical INT64,
            orientation STRING,
            creation_date STRING,
            creative_number INT64,
            creative_bundle INT64,
            video_duration INT64,
            text STRING,
            file_name STRING,
            youtube_link STRING,
            file_path STRING,
            past_name STRING,
            note STRING,
            file_link STRING
        )
        OPTIONS (
            format = 'GOOGLE_SHEETS',
            uris = ['https://docs.google.com/spreadsheets/d/1n2-pedl4-QH8Jo4poqPk62ynyaezMaM0dKuWitRqyDo/export?format=csv&gid=865936054'],
            sheet_range = 'POTC!A6:R999'
        )
        """

        job = client.query(query)
        job.result()

        print(f"✅ POTC_standard_creative_list ✅ 테이블 생성 완료")


    def GBTW_standard_creative_list(**context):
        query = """
            CREATE OR REPLACE EXTERNAL TABLE `data-science-division-216308.Creative.GBTW_standard_creative_list`
            (
            category STRING,
            subcategory STRING,
            type STRING,
            ai STRING,
            horizontal INT64,
            vertical INT64,
            orientation STRING,
            creation_date STRING,
            creative_number INT64,
            creative_bundle INT64,
            video_duration INT64,
            text STRING,
            file_name STRING,
            youtube_link STRING,
            file_path STRING,
            past_name STRING,
            note STRING,
            file_link STRING
            )
            OPTIONS (
            format = 'GOOGLE_SHEETS',
            uris = ['https://docs.google.com/spreadsheets/d/1n2-pedl4-QH8Jo4poqPk62ynyaezMaM0dKuWitRqyDo/export?format=csv&gid=1357829068'],
            sheet_range = 'GBTW!A6:R999'
            )
        """
        job = client.query(query)
        job.result()

        print(f"✅ GBTW_standard_creative_list ✅ 테이블 생성 완료")


    def GBTW_Old_standard_creative_list(**context):
        query = """
            CREATE OR REPLACE EXTERNAL TABLE `data-science-division-216308.Creative.GBTW_Old_standard_creative_list`
            (
            category STRING,
            subcategory STRING,
            gcat STRING,
            type STRING,
            horizontal INT64,
            vertical INT64,
            orientation STRING,
            creation_date STRING,
            creative_number STRING,
            creative_bundle STRING,
            video_duration INT64,
            text STRING,
            file_name STRING,
            note STRING
            )
            OPTIONS (
            format = 'GOOGLE_SHEETS',
            uris = ['https://docs.google.com/spreadsheets/d/1iviFONO9Rbs1cErAQwOR2oHR0d6uRKeaNImRtg6lvPg/export?format=csv&gid=0'],
            sheet_range = 'GBTW_파일명생성!B6:O999'
            )
        """
        job = client.query(query)
        job.result()

        print(f"✅ GBTW_Old_standard_creative_list ✅ 테이블 생성 완료")

    def WWM_standard_creative_list(**context):
        query = """
            CREATE OR REPLACE EXTERNAL TABLE `data-science-division-216308.Creative.WWM_standard_creative_list`
            (
            category STRING,
            subcategory STRING,
            type STRING,
            ai STRING,
            horizontal INT64,
            vertical INT64,
            orientation STRING,
            creation_date STRING,
            creative_number INT64,
            creative_bundle INT64,
            video_duration INT64,
            text STRING,
            file_name STRING,
            youtube_link STRING,
            file_path STRING,
            past_name STRING,
            note STRING,
            file_link STRING
            )
            OPTIONS (
            format = 'GOOGLE_SHEETS',
            uris = ['https://docs.google.com/spreadsheets/d/1n2-pedl4-QH8Jo4poqPk62ynyaezMaM0dKuWitRqyDo/export?format=csv&gid=1039473270'],
            sheet_range = 'WWMC!A4:R999'
            )
            """
        job = client.query(query)
        job.result()

        print(f"✅ WWM_standard_creative_list ✅ 테이블 생성 완료")


    def DS_standard_creative_list(**context):
        query= """
            CREATE OR REPLACE EXTERNAL TABLE `data-science-division-216308.Creative.DS_standard_creative_list`
            (
            category STRING,
            subcategory STRING,
            type STRING,
            ai STRING,
            horizontal INT64,
            vertical INT64,
            orientation STRING,
            creation_date STRING,
            creative_number INT64,
            creative_bundle INT64,
            video_duration INT64,
            text STRING,
            file_name STRING,
            youtube_link STRING,
            file_path STRING,
            past_name STRING,
            note STRING,
            file_link STRING
            )
            OPTIONS (
            format = 'GOOGLE_SHEETS',
            uris = ['https://docs.google.com/spreadsheets/d/1n2-pedl4-QH8Jo4poqPk62ynyaezMaM0dKuWitRqyDo/export?format=csv&gid=348430804'],
            sheet_range = 'DRSG!A4:R999'
            )
            """
        job = client.query(query)
        job.result()

        print(f"✅ DS_standard_creative_list ✅ 테이블 생성 완료")

    def DRB_standard_creative_list(**context):
        query="""
            CREATE OR REPLACE EXTERNAL TABLE `data-science-division-216308.Creative.DRB_standard_creative_list`
            (
            category STRING,
            subcategory STRING,
            type STRING,
            ai STRING,
            horizontal INT64,
            vertical INT64,
            orientation STRING,
            creation_date STRING,
            creative_number INT64,
            creative_bundle INT64,
            video_duration INT64,
            text STRING,
            file_name STRING,
            youtube_link STRING,
            file_path STRING,
            past_name STRING,
            note STRING,
            file_link STRING
            )
            OPTIONS (
            format = 'GOOGLE_SHEETS',
            uris = ['https://docs.google.com/spreadsheets/d/1n2-pedl4-QH8Jo4poqPk62ynyaezMaM0dKuWitRqyDo/export?format=csv&gid=280833505'],
            sheet_range = 'DRB!A4:R999'
            )
            """
        job = client.query(query)
        job.result()

        print(f"✅ DRB_standard_creative_list ✅ 테이블 생성 완료")    


    def JYWN_standard_creative_list(**context):
        query= """
            CREATE OR REPLACE EXTERNAL TABLE `data-science-division-216308.Creative.JYWN_standard_creative_list`
            (
            category STRING,
            subcategory STRING,
            type STRING,
            ai STRING,
            horizontal INT64,
            vertical INT64,
            orientation STRING,
            creation_date STRING,
            creative_number INT64,
            creative_bundle INT64,
            video_duration INT64,
            text STRING,
            file_name STRING,
            youtube_link STRING,
            file_path STRING,
            note STRING,
            file_link STRING
            )
            OPTIONS (
            format = 'GOOGLE_SHEETS',
            uris = ['https://docs.google.com/spreadsheets/d/1n2-pedl4-QH8Jo4poqPk62ynyaezMaM0dKuWitRqyDo/export?format=csv&gid=499963117'],
            sheet_range = 'JTWN!A4:Q999'
            )
            """
        job = client.query(query)
        job.result()

        print(f"✅ JYWN_standard_creative_list ✅ 테이블 생성 완료")           


    def BSTD_standard_creative_list(**context):
        query = """
            CREATE OR REPLACE EXTERNAL TABLE `data-science-division-216308.Creative.BSTD_standard_creative_list`
            (
            category STRING,
            subcategory STRING,
            type STRING,
            ai STRING,
            horizontal INT64,
            vertical INT64,
            orientation STRING,
            creation_date STRING,
            creative_number INT64,
            creative_bundle INT64,
            video_duration INT64,
            text STRING,
            file_name STRING,
            youtube_link STRING,
            file_path STRING,
            note STRING,
            file_link STRING
            )
            OPTIONS (
            format = 'GOOGLE_SHEETS',
            uris = ['https://docs.google.com/spreadsheets/d/1n2-pedl4-QH8Jo4poqPk62ynyaezMaM0dKuWitRqyDo/export?format=csv&gid=940524887'],
            sheet_range = 'BSTD!A4:Q999'
            )
            """
        job = client.query(query)
        job.result()

        print(f"✅ BSTD_standard_creative_list ✅ 테이블 생성 완료")          

    def RESU_standard_creative_list(**context):
        query = """
            CREATE OR REPLACE EXTERNAL TABLE `data-science-division-216308.Creative.RESU_standard_creative_list`
            (
            category STRING,
            subcategory STRING,
            type STRING,
            ai STRING,
            horizontal INT64,
            vertical INT64,
            orientation STRING,
            creation_date STRING,
            creative_number INT64,
            creative_bundle INT64,
            video_duration INT64,
            text STRING,
            file_name STRING,
            youtube_link STRING,
            file_path STRING,
            note STRING,
            file_link STRING
            )
            OPTIONS (
            format = 'GOOGLE_SHEETS',
            uris = ['https://docs.google.com/spreadsheets/d/1n2-pedl4-QH8Jo4poqPk62ynyaezMaM0dKuWitRqyDo/export?format=csv&gid=814324770'],
            sheet_range = 'RESU!A4:Q999'
            )
            """
        job = client.query(query)
        job.result()

        print(f"✅ RESU_standard_creative_list ✅ 테이블 생성 완료")

    def all_standard_creative_list(**context):
        query = """
            CREATE OR REPLACE TABLE `data-science-division-216308.Creative.all_standard_creative_list` AS 
            select
            project,
            category1,
            category2,
            subcategory,
            type,
            ai,
            horizontal,
            vertical,
            orientation,
            -- 동일 번호 소재이나, 생성 일자가 다를시, 가장 빠른 일자 추출
            case
                when creative_number is null then creation_date
                else first_value(creation_date) over(partition by project, creative_number order by creation_date)
            end as creation_date,
            creative_number,
            creative_bundle,
            text,
            file_name,
            past_name,
            case when rn_video_id = 1 then video_id else null end as video_id,
            case when rn_video_id = 1 then temp_youtube_link else null end as youtube_link,
            note,
            file_link
            from (
            select
                project,
                split(category, ',')[safe_offset(0)] as category1,
                split(category, ',')[safe_offset(1)] as category2,
                subcategory,
                type,
                ai,
                horizontal,
                vertical,
                orientation,
                parse_date('%y%m%d', creation_date) as creation_date,
                creative_number,
                creative_bundle,
                text,
                file_name,
                past_name,
                temp_youtube_link,
                regexp_extract(temp_youtube_link, r'(?:youtu\.be/|v=|shorts/)([a-zA-Z0-9_-]+)') as video_id,
                note,
                file_link,
                -- 특정 순서에 상관없이
                -- 비디오 ID가 동일한 케이스의 경우 첫번째 값으로만 데이터 적재 진행
                row_number() over(partition by project, regexp_extract(temp_youtube_link, r'(?:youtu\.be/|v=|shorts/)([a-zA-Z0-9_-]+)')) as rn_video_id
            from (
                select 'GBTW' as project, * from `data-science-division-216308.Creative.GBTW_standard_creative_list`
                union all
                select
                'GBTW_Old' as project,
                category,
                subcategory,
                type,
                null as ai,
                horizontal,
                vertical,
                orientation,
                creation_date, 
                safe_cast(creative_number as int64) as creative_number,
                safe_cast(creative_bundle as int64) as creative_bundle,
                video_duration,
                text,
                -- GBTW 메타 소재 링크 제외
                case
                    when file_name like '%https:fb.me/%' then null
                    when type = 'Image' then file_name
                    else null
                end as file_name,
                case
                    when file_name like '%https:fb.me/%' then null
                    when type = 'Video' then file_name
                    else null
                end as youtube_link, 
                null as file_path,
                null as past_name,
                note,
                null as file_link
                from `data-science-division-216308.Creative.GBTW_Old_standard_creative_list`
                union all
                select 'POTC' as project, * from `data-science-division-216308.Creative.POTC_standard_creative_list`
                union all
                select 'WWM' as project, *  from `data-science-division-216308.Creative.WWM_standard_creative_list`
                union all
                select 'DRB' as project, * from `data-science-division-216308.Creative.DRB_standard_creative_list`
                union all
                select 'DS' as project, * from `data-science-division-216308.Creative.DS_standard_creative_list`
                union all
                select
                'JTWN' as project,
                category,
                subcategory,
                type,
                ai,
                horizontal,
                vertical,
                orientation,
                creation_date,
                creative_number,
                creative_bundle,
                video_duration,
                text,
                file_name,
                youtube_link,
                file_path,
                null as past_name,
                note,
                file_link
                from `data-science-division-216308.Creative.JYWN_standard_creative_list`
                union all
                select
                'BSTD' as project,
                category,
                subcategory,
                type,
                ai,
                horizontal,
                vertical,
                orientation,
                creation_date,
                creative_number,
                creative_bundle,
                video_duration,
                text,
                file_name,
                youtube_link,
                file_path,
                null as past_name,
                note,
                file_link
                from `data-science-division-216308.Creative.BSTD_standard_creative_list`  
                union all
                select
                'RESU' as project,
                category,
                subcategory,
                type,
                ai,
                horizontal,
                vertical,
                orientation,
                creation_date,
                creative_number,
                creative_bundle,
                video_duration,
                text,
                file_name,
                youtube_link,
                file_path,
                null as past_name,
                note,
                file_link
                from `data-science-division-216308.Creative.RESU_standard_creative_list`  
            ) as a
            , unnest(
                if(
                youtube_link is null,
                array<string>[null],
                split(youtube_link, ',')
                )
            ) as temp_youtube_link
            where category is not null 
            )
            order by project, creative_number 
            """
        job = client.query(query)
        job.result()

        print(f"✅ all_standard_creative_list ✅ 테이블 생성 완료")   


    def get_var(key: str, default: str = None) -> str:
        """환경 변수 또는 Airflow Variable 조회"""
        return os.environ.get(key) or Variable.get(key, default_var=default)

    RECIPIENT_EMAIL = '65e43b85.joycity.com@kr.teams.ms'
    SMTP_HOST = get_var("SMTP_HOST", "smtp.gmail.com")
    SMTP_PORT = int(get_var("SMTP_PORT", "587"))
    SMTP_USER = get_var("SMTP_USER", RECIPIENT_EMAIL)
    SMTP_PASSWORD = get_var("SMTP_PASSWORD", "ciucjokomvmipxej")
    EMAIL_FROM = get_var("EMAIL_FROM", SMTP_USER)

    def send_status_email(**context):
        """작업 완료 후 간단한 알림 이메일 발송"""
        print("=" * 80)
        print("Task 3: 완료 알림 이메일 발송 시작")
        print("=" * 80)
        
        # 실행 날짜 (logical_date 사용)
        logical_date = context['logical_date']
        execution_date = logical_date.strftime('%Y-%m-%d %H:%M:%S KST')
        
        # 이메일 제목 및 본문 작성
        subject = "✅ MS Creative List Table 생성 완료"
        body = f"""
    Creative List Table 생성이 완료되었습니다.

    실행 시간: {execution_date}

    이 이메일은 Airflow 자동 배치 작업에서 발송되었습니다.
        """
        
        # 이메일 발송
        try:
            msg = MIMEMultipart()
            msg['Subject'] = subject
            msg['From'] = EMAIL_FROM
            msg['To'] = RECIPIENT_EMAIL
            
            text_part = MIMEText(body, 'plain')
            msg.attach(text_part)
            
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
                server.starttls()
                server.login(SMTP_USER, SMTP_PASSWORD)
                server.send_message(msg)
            
            print(f"✓ 이메일 발송 완료: {RECIPIENT_EMAIL}")
            print(f"  제목: {subject}")
        except Exception as e:
            print(f"✗ 이메일 발송 실패: {str(e)}")
            raise
        
        print("=" * 80)
        print("Task 3: 완료 알림 이메일 발송 완료")
        print("=" * 80)




    task_POTC_standard_creative_list = PythonOperator(
        task_id='POTC_standard_creative_list',
        python_callable=POTC_standard_creative_list,
    )

    task_GBTW_standard_creative_list = PythonOperator(
        task_id='GBTW_standard_creative_list',
        python_callable=GBTW_standard_creative_list,
    )

    task_GBTW_Old_standard_creative_list = PythonOperator(
        task_id='GBTW_Old_standard_creative_list',
        python_callable=GBTW_Old_standard_creative_list,
    )

    task_WWM_standard_creative_list = PythonOperator(
        task_id='WWM_standard_creative_list',
        python_callable=WWM_standard_creative_list,
    )

    task_DS_standard_creative_list = PythonOperator(
        task_id='DS_standard_creative_list',
        python_callable=DS_standard_creative_list,
    )

    task_DRB_standard_creative_list = PythonOperator(
        task_id='DRB_standard_creative_list',
        python_callable=DRB_standard_creative_list,
    )

    task_JYWN_standard_creative_list = PythonOperator(
        task_id='JYWN_standard_creative_list',
        python_callable=JYWN_standard_creative_list,
    )

    task_BSTD_standard_creative_list = PythonOperator(
        task_id='BSTD_standard_creative_list',
        python_callable=BSTD_standard_creative_list,
    )

    task_RESU_standard_creative_list = PythonOperator(
        task_id='RESU_standard_creative_list',
        python_callable=RESU_standard_creative_list,
    )

    task_all_standard_creative_list = PythonOperator(
        task_id='all_standard_creative_list',
        python_callable=all_standard_creative_list,
    )



    task_send_email = PythonOperator(
        task_id='send_status_email',
        python_callable=send_status_email,
        trigger_rule='all_done',  # 이전 task 성공/실패 관계없이 항상 실행
        dag=dag,
    )

    [ 
        task_POTC_standard_creative_list, 
        task_GBTW_standard_creative_list,
        task_GBTW_Old_standard_creative_list,
        task_WWM_standard_creative_list,
        task_DS_standard_creative_list,
        task_DRB_standard_creative_list,
        task_JYWN_standard_creative_list,
        task_BSTD_standard_creative_list,
        task_RESU_standard_creative_list 
    ] >> task_all_standard_creative_list >> task_send_email


