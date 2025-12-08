# Airflow function
from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.models.baseoperator import chain

import pandas as pd
from google.cloud import bigquery
from google.auth.transport.requests import Request
import logging

from datetime import datetime, timezone, timedelta
import time
import os
import pytz

#### Fact table 처리 함수 불러오기
from ETL_Fact_tracker import *
from ETL_Fact_access import *
from ETL_Fact_payment import *
from ETL_Fact_funnel import *
from ETL_Fact_IAA import *


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
    
    # 빅쿼리 클라이언트 연결
    client = bigquery.Client()

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
        try:
            etl_pre_tracker_install()
            etl_f_tracker_install()
            etl_f_cost_campaign_rule()
            logger.info("etl_fact_tracker completed successfully")
            return True
        except Exception as e:
            logger.error(f"etl_fact_tracker failed with error: {e}")
            return False
    
    def etl_fact_access(target_date:list):
        try:
            etl_f_common_register(target_date=target_date)
            etl_f_common_register_char(target_date=target_date)
            etl_f_common_access(target_date=target_date)
            return True
        except Exception as e:
            logger.error(f"etl_fact_access failed with error: {e}")
            return False
    
    def etl_fact_payment(target_date:list):
        try:
            etl_pre_payment_deduct_user(target_date=target_date)
            etl_pre_payment_deduct_order(target_date=target_date)
            etl_pre_payment_info_fix()
            etl_f_common_payment_total(target_date=target_date)
            etl_f_common_payment(target_date=target_date)
            return True
        except Exception as e:
            logger.error(f"etl_fact_payment failed with error: {e}")
            return False
    
    def etl_fact_funnel(target_date:list):
        try:
            etl_f_funnel_access_first(target_date=target_date)
            etl_f_funnel_access(target_date=target_date)
            return True
        except Exception as e:
            logger.error(f"etl_fact_funnel failed with error: {e}")
            return False
   
    def etl_fact_IAA(target_date:list):
        try:
            etl_f_IAA_game_sub_user_watch(target_date=target_date)
            etl_f_IAA_performance()
            etl_f_IAA_auth_account_performance_joyple(target_date=target_date)
            etl_f_IAA_auth_account_performance(target_date=target_date)
            return True
        except Exception as e:
            logger.error(f"etl_fact_IAA failed with error: {e}")
            return False
    

    etl_fact_tracker_task = PythonOperator(
        task_id='etl_fact_tracker',
        python_callable=etl_fact_tracker,
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