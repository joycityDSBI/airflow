# Airflow function
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from google.cloud import bigquery
import logging
from datetime import datetime, timedelta
import pytz
import json
from google.oauth2 import service_account


#### Fact table 처리 함수 불러오기
from ETL_Fact_tracker import * 
from ETL_Fact_access import * 
from ETL_Fact_payment import * 
from ETL_Fact_funnel import * 
from ETL_Fact_IAA import * 
from ETL_Fact_usermap import * 

PROJECT_ID = "data-science-division-216308"
LOCATION = "us-central1"

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


def init_clients():
    """Task 내부에서 실행되어 필요한 클라이언트들을 생성하여 반환합니다."""
    creds = get_gcp_credentials()
    
    # 1. GCP Clients
    bq_client = bigquery.Client(project=PROJECT_ID, credentials=creds)
    
    return {
        "bq_client": bq_client
    }

def calc_target_date(logical_date):
    """
    [핵심 로직]
    Airflow 실행 시점(logical_date)을 KST로 변환한 후,
    '하루 전(Yesterday)' 날짜를 계산하여 리스트 형태로 반환합니다.
    """
    utc = pytz.utc
    kst = pytz.timezone('Asia/Seoul')
    
    # 1. UTC 실행 시간을 KST로 변환
    run_date_kst = logical_date.replace(tzinfo=utc).astimezone(kst)
    
    # 2. KST 기준 하루 전 날짜 계산 (Yesterday)
    target_d = run_date_kst.date() - timedelta(days=1)
    
    # 3. datetime 객체로 변환 (00:00:00) 및 리스트화
    target_dt = datetime.combine(target_d, datetime.min.time())
    
    return [target_dt], run_date_kst



def target_date_range(start_date_str, end_date_str):
    """날짜 데이터 백필용"""
    # 문자열을 datetime 객체로 변환
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    date_list = []
    current_date = start_date
    
    # 종료 날짜까지 하루씩 더하며 리스트에 추가
    while current_date <= end_date:
        date_list.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)
        
    return date_list


    
def etl_fact_tracker(**context):
    logger = logging.getLogger(__name__)
    
    # target_date = target_date_range("2026-01-02", "2026-01-22")  ## 백필용
    target_date = target_date_range("2026-01-23", "2026-01-24")  ## 백필용
    run_kst = None

    # 날짜 계산
    # target_date, run_kst = calc_target_date(context['logical_date'])
    logger.info(f"🚀 배치 실행 시점(KST): {run_kst}")
    logger.info(f"📅 처리 대상 날짜(Target Date, D-1): {target_date[0]}")

    client = init_clients()
    bq_client = client["bq_client"]

    try:
        etl_f_tracker_install(target_date=target_date, client=bq_client)
        # etl_f_tracker_re_engagement(target_date=target_date, client=bq_client)
        etl_pre_joytracking_tracker(target_date=target_date, client=bq_client)
        etl_f_cost_campaign_rule(client=bq_client)
        logger.info("✅ etl_fact_tracker completed successfully")
        return True
    
    except Exception as e:
        logger.error(f"❌ etl_fact_tracker failed with error: {e}")
        raise e

def etl_fact_access(**context):
    logger = logging.getLogger(__name__)

    # target_date = target_date_range("2026-01-01", "2026-01-22")  ## 백필용
    target_date = target_date_range("2026-01-23", "2026-01-24")  ## 백필용

    # 날짜 계산
    # target_date, _ = calc_target_date(context['logical_date'])
    logger.info(f"📅 Access ETL Target Date: {target_date[0]}")

    client = init_clients()
    bq_client = client["bq_client"]
    try:
        # etl_f_common_register(target_date=target_date, client=bq_client)
        # adjust_f_common_register(target_date=target_date, client=bq_client)
        # etl_f_common_register_char(target_date=target_date, client=bq_client)
        # adjust_f_common_register_char(target_date=target_date, client=bq_client)
        # etl_f_common_access(target_date=target_date, client=bq_client)
        logger.info("✅ etl_fact_access completed successfully")
        return True
    except Exception as e:
        logger.error(f"❌ etl_fact_access failed with error: {e}")
        raise e

def etl_fact_payment(**context):
    logger = logging.getLogger(__name__)

    # target_date = target_date_range("2026-01-01", "2026-01-22")  ## 백필용
    target_date = target_date_range("2026-01-23", "2026-01-24")  ## 백필용

    # 날짜 계산
    # target_date, _ = calc_target_date(context['logical_date'])
    logger.info(f"📅 Payment ETL Target Date: {target_date[0]}")

    client = init_clients()
    bq_client = client["bq_client"]
    try:
        # etl_f_common_payment(target_date=target_date, client=bq_client)
        logger.info("✅ etl_fact_payment completed successfully")
        return True
    except Exception as e:
        logger.error(f"❌ etl_fact_payment failed with error: {e}")
        raise e

def etl_fact_funnel(**context):
    logger = logging.getLogger(__name__)

    # target_date = target_date_range("2026-01-02", "2026-01-22")  ## 백필용
    target_date = target_date_range("2026-01-23", "2026-01-24")  ## 백필용

    # 날짜 계산
    # target_date, _ = calc_target_date(context['logical_date'])
    logger.info(f"📅 Funnel ETL Target Date: {target_date[0]}")

    client = init_clients()
    bq_client = client["bq_client"]
    try:
        # etl_f_funnel_access_first(target_date=target_date, client=bq_client)
        # etl_f_funnel_access(target_date=target_date, client=bq_client)
        logger.info("✅ etl_fact_funnel completed successfully")
        return True
    except Exception as e:
        logger.error(f"❌ etl_fact_funnel failed with error: {e}")
        raise e

def etl_fact_IAA(**context):
    logger = logging.getLogger(__name__)

    # target_date = target_date_range("2026-01-02", "2026-01-22")  ## 백필용
    target_date = target_date_range("2026-01-23", "2026-01-24")  ## 백필용

    # 날짜 계산
    # target_date, _ = calc_target_date(context['logical_date'])
    logger.info(f"📅 IAA ETL Target Date: {target_date[0]}")

    client = init_clients()
    bq_client = client["bq_client"]
    try:
        # etl_f_IAA_game_sub_user_watch(target_date=target_date, client=bq_client)
        # etl_f_IAA_performance(client=bq_client)
        # etl_f_IAA_auth_account_performance_joyple(target_date=target_date, client=bq_client)
        # etl_f_IAA_auth_account_performance(target_date=target_date, client=bq_client)
        logger.info("✅ etl_fact_IAA completed successfully")
        return True
    except Exception as e:
        logger.error(f"❌ etl_fact_IAA failed with error: {e}")
        raise e

def etl_fact_usermap(**context):
    logger = logging.getLogger(__name__)

    # target_date = target_date_range("2026-01-02", "2026-01-22")  ## 백필용
    target_date = target_date_range("2026-01-23", "2026-01-24")  ## 백필용

    # 날짜 계산
    # target_date, _ = calc_target_date(context['logical_date'])
    logger.info(f"📅 Usermap ETL Target Date: {target_date[0]}")

    client = init_clients()
    bq_client = client["bq_client"]

    for date in target_date:
        logger.info(f"🔄 Processing date: {date}")
        tsa = [date]

        try:
            etl_f_common_access_last_login(target_date=tsa, client=bq_client)
            etl_f_user_map(target_date=tsa, client=bq_client)
            etl_f_user_map_char(target_date=tsa, client=bq_client)

        except Exception as e:
            logger.error(f"❌ etl_fact_usermap failed with error: {e}")
            raise e

    logger.info("✅ etl_fact_usermap completed successfully")
    return True


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
    dag_id='ETL_ALL_Fact',
    default_args=default_args,
    description='전체 fact table에 대해서 OLAP 처리 (KST D-1 기준)',
    schedule= '30 20 * * *', ## KST 05:30 AM 매일 실행 -> UTC 20:30 PM 전날 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['ETL', 'fact', 'bigquery'],
) as dag:

    etl_fact_tracker_task = PythonOperator(
        task_id='etl_fact_tracker',
        python_callable=etl_fact_tracker,
    )
    
    etl_fact_access_task = PythonOperator(
        task_id='etl_fact_access',
        python_callable=etl_fact_access,
    )
        
    etl_fact_payment_task = PythonOperator(
        task_id='etl_fact_payment',
        python_callable=etl_fact_payment,
    )

    etl_fact_funnel_task = PythonOperator(
        task_id='etl_fact_funnel',
        python_callable=etl_fact_funnel,
    )

    etl_fact_IAA_task = PythonOperator(
        task_id='etl_fact_IAA',
        python_callable=etl_fact_IAA,
    )

    etl_fact_usermap_task = PythonOperator(
        task_id='etl_fact_usermap',
        python_callable=etl_fact_usermap,
    )


    etl_fact_tracker_task >> etl_fact_access_task >> etl_fact_payment_task >> etl_fact_funnel_task >> etl_fact_IAA_task >> etl_fact_usermap_task

