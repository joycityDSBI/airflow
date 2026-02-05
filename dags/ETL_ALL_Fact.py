# Airflow function
from multiprocessing import context
from airflow import DAG, Dataset
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
from ETL_Utils import init_clients, calc_target_date, target_date_range


ETL_dimension = Dataset('ETL_dimension')

PROJECT_ID = "datahub-478802"
LOCATION = "US"




    
def etl_fact_tracker(**context):
    logger = logging.getLogger(__name__)

    client = init_clients()
    bq_client = client["bq_client"]

    ########### 백필용 데이터 처리    
    # target_date = target_date_range("2026-01-06", "2026-01-26")  ## 백필용
    # run_kst = None

    # 날짜 계산
    run_date = context.get('logical_date') or context.get('execution_date')

    if not run_date:
        print("Context에서 날짜 정보를 찾을 수 없습니다. (logical_date or execution_date missing)")
        run_date = datetime.now()

    # 3. 날짜 계산 함수 호출
    target_date, _ = calc_target_date(run_date)
    print("✅✅✅✅ Calculated target_date:", target_date[0])

    try:
        etl_f_tracker_install(target_date=target_date, client=bq_client)
        # etl_f_tracker_re_engagement(target_date=target_date, client=bq_client) ## 제거됨
        etl_pre_joytracking_tracker(target_date=target_date, client=bq_client)
        etl_f_cost_campaign_rule(client=bq_client)
        logger.info("✅ etl_fact_tracker completed successfully")
        return True
    
    except Exception as e:
        logger.error(f"❌ etl_fact_tracker failed with error: {e}")
        print("error:", e)
        raise e

def etl_fact_access(**context):
    logger = logging.getLogger(__name__)

    client = init_clients()
    bq_client = client["bq_client"]

    ########### 백필용 데이터 처리    
    # target_date = target_date_range("2026-01-06", "2026-01-26")  ## 백필용
    # run_kst = None

    # 날짜 계산
    run_date = context.get('logical_date') or context.get('execution_date')

    if not run_date:
        print("Context에서 날짜 정보를 찾을 수 없습니다. (logical_date or execution_date missing)")
        run_date = datetime.now()

    # 3. 날짜 계산 함수 호출
    target_date, _ = calc_target_date(run_date)
    print("✅✅✅✅ Calculated target_date:", target_date[0])

    try:
        etl_f_common_register(target_date=target_date, client=bq_client)
        adjust_f_common_register(target_date=target_date, client=bq_client)
        etl_f_common_register_char(target_date=target_date, client=bq_client)
        adjust_f_common_register_char(target_date=target_date, client=bq_client)
        etl_f_common_access(target_date=target_date, client=bq_client)
        logger.info("✅ etl_fact_access completed successfully")
        return True
    except Exception as e:
        logger.error(f"❌ etl_fact_access failed with error: {e}")
        raise e

def etl_fact_payment(**context):
    logger = logging.getLogger(__name__)

    ########### 백필용 데이터 처리    
    # target_date = target_date_range("2026-01-06", "2026-01-26")  ## 백필용
    # run_kst = None

    # 날짜 계산
    run_date = context.get('logical_date') or context.get('execution_date')

    if not run_date:
        print("Context에서 날짜 정보를 찾을 수 없습니다. (logical_date or execution_date missing)")
        run_date = datetime.now()

    # 3. 날짜 계산 함수 호출
    target_date, _ = calc_target_date(run_date)
    print("✅✅✅✅ Calculated target_date:", target_date[0])

    client = init_clients()
    bq_client = client["bq_client"]
    try:
        etl_f_common_payment(target_date=target_date, client=bq_client)
        logger.info("✅ etl_fact_payment completed successfully")
        return True
    except Exception as e:
        logger.error(f"❌ etl_fact_payment failed with error: {e}")
        raise e


def etl_fact_funnel(**context):
    logger = logging.getLogger(__name__)

    # target_date = target_date_range("2026-01-06", "2026-01-26")  ## 백필용

    # 날짜 계산
    run_date = context.get('logical_date') or context.get('execution_date')

    if not run_date:
        print("Context에서 날짜 정보를 찾을 수 없습니다. (logical_date or execution_date missing)")
        run_date = datetime.now()

    # 3. 날짜 계산 함수 호출
    target_date, _ = calc_target_date(run_date)
    print("✅✅✅✅ Calculated target_date:", target_date[0])

    client = init_clients()
    bq_client = client["bq_client"]
    try:
        etl_f_funnel_access_first(target_date=target_date, client=bq_client)
        etl_f_funnel_access(target_date=target_date, client=bq_client)
        logger.info("✅ etl_fact_funnel completed successfully")
        return True
    except Exception as e:
        logger.error(f"❌ etl_fact_funnel failed with error: {e}")
        raise e

def etl_fact_IAA(**context):
    logger = logging.getLogger(__name__)

    # target_date = target_date_range("2026-01-06", "2026-01-26")  ## 백필용

    # 날짜 계산
    run_date = context.get('logical_date') or context.get('execution_date')

    if not run_date:
        print("Context에서 날짜 정보를 찾을 수 없습니다. (logical_date or execution_date missing)")
        run_date = datetime.now()

    # 3. 날짜 계산 함수 호출
    target_date, _ = calc_target_date(run_date)
    print("✅✅✅✅ Calculated target_date:", target_date[0])

    client = init_clients()
    bq_client = client["bq_client"]
    try:
        etl_f_IAA_game_sub_user_watch(target_date=target_date, client=bq_client)
        etl_f_IAA_performance(client=bq_client)
        etl_f_IAA_auth_account_performance_joyple(target_date=target_date, client=bq_client)
        etl_f_IAA_auth_account_performance(target_date=target_date, client=bq_client)
        logger.info("✅ etl_fact_IAA completed successfully")
        return True
    except Exception as e:
        logger.error(f"❌ etl_fact_IAA failed with error: {e}")
        raise e

def etl_fact_usermap(**context):
    logger = logging.getLogger(__name__)

    # target_date = target_date_range("2026-01-06", "2026-01-26")  ## 백필용

    # 날짜 계산
    run_date = context.get('logical_date') or context.get('execution_date')

    if not run_date:
        print("Context에서 날짜 정보를 찾을 수 없습니다. (logical_date or execution_date missing)")
        run_date = datetime.now()

    # 3. 날짜 계산 함수 호출
    target_date, _ = calc_target_date(run_date)
    print("✅✅✅✅ Calculated target_date:", target_date[0])

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
    # schedule='30 03 * * *',  # 매일 오전 3시 30분 실행
    schedule= [ETL_dimension], ################ ETL dimension DAG 완료 후 실행
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
