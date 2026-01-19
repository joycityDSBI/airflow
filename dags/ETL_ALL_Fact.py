# Airflow function
from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.models.baseoperator import chain

from google.cloud import bigquery
import logging
from datetime import datetime, timezone, timedelta
import time
import pytz
import json
from google.oauth2 import service_account


#### Fact table 처리 함수 불러오기
from ETL_Fact_tracker import *
from ETL_Fact_access import *
from ETL_Fact_payment import *
from ETL_Fact_funnel import *
from ETL_Fact_IAA import *

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
    description='전체 fact table에 대해서 OLAP 처리',
    schedule= '30 20 * * *', ## 9시 이후에 처리 필요 (appsflyer 데이터 가져오는 시각이 9시)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['ETL', 'fact', 'bigquery'],
) as dag:
    
    # logger 호출
    logger = logging.getLogger(__name__)

    # 방법 1: 현재 KST 날짜 기준
    target_date_kst = datetime.now(pytz.timezone('Asia/Seoul')).date()  # 2025-11-24
    kst = pytz.timezone('Asia/Seoul')

    # target_date_kst를 datetime으로 변환
    target_date = datetime.combine(target_date_kst, datetime.min.time())  # 2025-11-24 00:00:00
    print(f"Target KST Date: {target_date_kst}")
    target_date = [target_date]
    

    def etl_fact_tracker():
        client = init_clients()
        bq_client = client["bq_client"]
        try:
            etl_f_tracker_install(target_date=target_date, client=bq_client)
            etl_f_tracker_re_engagement(target_date=target_date, client=bq_client)
            etl_pre_joytracking_tracker(target_date=target_date, client=bq_client)
            etl_f_cost_campaign_rule(client=bq_client)
            logger.info("etl_fact_tracker completed successfully")
            return True
        except Exception as e:
            logger.error(f"etl_fact_tracker failed with error: {e}")
            return False
    
    def etl_fact_access(target_date:list):
        client = init_clients()
        bq_client = client["bq_client"]
        try:
            etl_f_common_register(target_date=target_date, client=bq_client)
            adjust_f_common_register(target_date=target_date, client=bq_client)
            etl_f_common_register_char(target_date=target_date, client=bq_client)
            adjust_f_common_register_char(target_date=target_date, client=bq_client)
            etl_f_common_access(target_date=target_date, client=bq_client)
            etl_f_common_access_last_login(target_date=target_date, client=bq_client)
            logger.info("etl_fact_access completed successfully")
            return True
        except Exception as e:
            logger.error(f"etl_fact_access failed with error: {e}")
            return False
    
    def etl_fact_payment(target_date:list):
        client = init_clients()
        bq_client = client["bq_client"]
        try:
            etl_f_common_payment(target_date=target_date, client=bq_client)
            logger.info("etl_fact_payment completed successfully")
            return True
        except Exception as e:
            logger.error(f"etl_fact_payment failed with error: {e}")
            return False
    
    def etl_fact_funnel(target_date:list):
        client = init_clients()
        bq_client = client["bq_client"]
        try:
            etl_f_funnel_access_first(target_date=target_date, client=bq_client)
            etl_f_funnel_access(target_date=target_date, client=bq_client)
            logger.info("etl_fact_funnel completed successfully")
            return True
        except Exception as e:
            logger.error(f"etl_fact_funnel failed with error: {e}")
            return False
   
    def etl_fact_IAA(target_date:list):
        client = init_clients()
        bq_client = client["bq_client"]
        try:
            etl_f_IAA_game_sub_user_watch(target_date=target_date, client=bq_client)
            etl_f_IAA_performance(client=bq_client)
            etl_f_IAA_auth_account_performance_joyple(target_date=target_date, client=bq_client)
            etl_f_IAA_auth_account_performance(target_date=target_date, client=bq_client)
            logger.info("etl_fact_IAA completed successfully")
            return True
        except Exception as e:
            logger.error(f"etl_fact_IAA failed with error: {e}")
            return False
    

    etl_fact_tracker_task = PythonOperator(
        task_id='etl_fact_tracker',
        python_callable=etl_fact_tracker,
        op_kwargs = {'target_date': target_date},
        dag=dag,
    )
    
    etl_fact_access_task = PythonOperator(
        task_id='etl_fact_access',
        python_callable=etl_fact_access,
        op_kwargs = {'target_date': target_date},
        dag=dag,
    )
        
    etl_fact_payment_task = PythonOperator(
        task_id='etl_fact_payment',
        python_callable=etl_fact_payment,
        op_kwargs = {'target_date': target_date},
        dag=dag,
    )

    etl_fact_funnel_task = PythonOperator(
        task_id='etl_fact_funnel',
        python_callable=etl_fact_funnel,
        op_kwargs = {'target_date': target_date},
        dag=dag,
    )

    etl_fact_IAA_task = PythonOperator(
        task_id='etl_fact_IAA',
        python_callable=etl_fact_IAA,
        op_kwargs = {'target_date': target_date},
        dag=dag,
    )

    etl_fact_tracker_task >> etl_fact_access_task >> etl_fact_payment_task >> etl_fact_funnel_task >> etl_fact_IAA_task

