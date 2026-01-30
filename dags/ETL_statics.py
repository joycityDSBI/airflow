# Airflow function
from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.models.baseoperator import chain
from google.oauth2 import service_account
import json

import pandas as pd
from google.cloud import bigquery
from google.auth.transport.requests import Request
import logging

from datetime import datetime, timezone, timedelta
import time
import os
import pytz

#### Dimension table 처리 함수 불러오기
PROJECT_ID = "datahub-478802"
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
    
    # 문자열로 변환하여 return 
    target_date_str = target_d.strftime("%Y-%m-%d")
    
    return [target_date_str], run_date_kst


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



def etl_statics_daily_kpi(**context):

    # 클라이언트 호출
    client = init_clients()["bq_client"]

    target_date, _ = calc_target_date(context['logical_date'])
    #################### 백필용 데이터 처리
    # target_date = target_date_range("2026-01-24", "2026-01-26")  ## 백필용

    for td_str in target_date:
               
        print(f"📝 대상날짜: {td_str}")

        # ETL 작업 수행
        query = f"""
        
        MERGE INTO `datahub-478802.datahub.statics_daily_kpi` as target
        USING (
        SELECT TA.datekey, TA.joyple_game_code, TA.DAU, TB.DRU, TC.PU
        , CAST(TC.IAP_revenue + TD.IAA_rev as INT64) as total_rev
        , CAST(TC.IAP_revenue as INT64) as IAP_rev
        , CAST(TC.IAP_revenue - TC.IAP_none_market_revenue as INT64) as IAP_market_rev
        , CAST(TC.IAP_none_market_revenue as INT64) as IAP_none_market_rev
        , CAST(TD.IAA_rev as INT64) as IAA_rev
        , ROUND(TC.PU/TA.DAU * 100, 2) as PUR
        , ROUND(TC.IAP_revenue / TC.PU, 0) as ARPPU
        , ROUND(TC.IAP_revenue / TA.DAU, 0) as ARPDAU
        , TE.installs_funnel
        , TF.installs_appsflyer
        , ROUND(TG.NNPU / TC.PU * 100, 2) as NNPUR
        , TG.NNPU
        , CAST(TG.NNPU_rev as INT64) as NNPU_rev
        FROM
        (
            select datekey, joyple_game_code, count(distinct auth_account_name) as DAU
            from `datahub-478802.datahub.f_common_access`
            where datekey >= '{td_str}' and datekey < DATE_ADD('{td_str}', INTERVAL 1 DAY)
            and access_type_id = 1
            group by datekey, joyple_game_code 
        ) as TA
        left join
        (
            select reg_datekey as datekey, joyple_game_code, count(distinct auth_account_name) as DRU
            from `datahub-478802.datahub.f_common_register`
            where reg_datekey >= '{td_str}' and reg_datekey < DATE_ADD('{td_str}', INTERVAL 1 DAY)
            group by reg_datekey, joyple_game_code 
        ) as TB
        ON TA.datekey = TB.datekey AND TA.joyple_game_code = TB.joyple_game_code
        left join
        (
            select datekey, joyple_game_code, count(distinct auth_account_name) as PU
            , sum(revenue) as IAP_revenue
            , sum(
            CASE WHEN pg_id in (select pg_id from `datahub-478802.datahub.dim_special_pg`) 
            AND platform_device_type in (select platform_device_type from `datahub-478802.datahub.dim_special_pg`)
            THEN revenue END
            ) as IAP_none_market_revenue
            from `datahub-478802.datahub.f_common_payment`
            where datekey >= '{td_str}' and datekey < DATE_ADD('{td_str}', INTERVAL 1 DAY)
            group by datekey, joyple_game_code 
        ) as TC
        ON TA.datekey = TC.datekey AND TA.joyple_game_code = TC.joyple_game_code
        left join 
        (
        select watch_datekey as datekey, joyple_game_code
        , sum(revenue_per_user_KRW) as IAA_rev
        from `datahub-478802.datahub.f_IAA_auth_account_performance`
        where watch_datekey >= '{td_str}' and watch_datekey < DATE_ADD('{td_str}', INTERVAL 1 DAY)
        group by watch_datekey, joyple_game_code
        ) as TD
        ON TA.datekey = TD.datekey AND TA.joyple_game_code = TD.joyple_game_code
        left join 
        (
        select datekey as datekey, joyple_game_code
        , count(distinct device_id) as installs_funnel
        from `datahub-478802.datahub.f_funnel_access_first`
        where datekey >= '{td_str}' and datekey < DATE_ADD('{td_str}', INTERVAL 1 DAY)
        group by datekey, joyple_game_code
        ) as TE
        ON TA.datekey = TE.datekey AND TA.joyple_game_code = TE.joyple_game_code
        left join 
        (
        select install_datekey as datekey, joyple_game_code
        , count(distinct tracker_account_id) as installs_appsflyer
        from `datahub-478802.datahub.f_tracker_install`
        where install_datekey >= '{td_str}' and install_datekey < DATE_ADD('{td_str}', INTERVAL 1 DAY)
        group by install_datekey, joyple_game_code
        ) as TF
        ON TA.datekey = TF.datekey AND TA.joyple_game_code = TF.joyple_game_code
        left join 
        (
        select datekey, joyple_game_code
        , count(distinct auth_account_name) as NNPU
        , sum(revenue) as NNPU_rev
        from `datahub-478802.datahub.f_common_payment`
        where datekey >= '{td_str}' and datekey < DATE_ADD('{td_str}', INTERVAL 1 DAY)
        and reg_datekey >= '{td_str}' and reg_datekey < DATE_ADD('{td_str}', INTERVAL 1 DAY)
        and reg_datediff = 0
        group by datekey, joyple_game_code
        ) as TG
        ON TA.datekey = TG.datekey AND TA.joyple_game_code = TG.joyple_game_code
        ) as source
        ON target.datekey = source.datekey AND target.joyple_game_code = source.joyple_game_code
        WHEN MATCHED THEN 
        UPDATE SET 
        target.DAU = source.DAU,
        target.DRU = source.DRU,
        target.PU = source.PU,
        target.total_rev = source.total_rev,
        target.IAP_rev = source.IAP_rev,
        target.IAP_market_rev = source.IAP_market_rev,
        target.IAP_none_market_rev = source.IAP_none_market_rev,
        target.IAA_rev = source.IAA_rev,
        target.PUR = source.PUR,
        target.ARPPU = source.ARPPU,
        target.ARPDAU = source.ARPDAU,
        target.installs_funnel = source.installs_funnel,
        target.installs_appsflyer = source.installs_appsflyer,
        target.NNPUR = source.NNPUR,
        target.NNPU = source.NNPU,
        target.NNPU_rev = source.NNPU_rev
        WHEN NOT MATCHED THEN 
        INSERT
        (
        datekey,
        joyple_game_code,
        DAU,
        DRU,
        PU,
        total_rev,
        IAP_rev,
        IAP_market_rev,
        IAP_none_market_rev,
        IAA_rev,
        PUR,
        ARPPU,
        ARPDAU,
        installs_funnel,
        installs_appsflyer,
        NNPUR,
        NNPU,
        NNPU_rev
        )
        VALUES 
        (
        source.datekey,
        source.joyple_game_code,
        source.DAU,
        source.DRU,
        source.PU,
        source.total_rev,
        source.IAP_rev,
        source.IAP_market_rev,
        source.IAP_none_market_rev,
        source.IAA_rev,
        source.PUR,
        source.ARPPU,
        source.ARPDAU,
        source.installs_funnel,
        source.installs_appsflyer,
        source.NNPUR,
        source.NNPU,
        source.NNPU_rev
        )
        """

        # 1. 쿼리 실행
        query_job = client.query(query)

        try:
            # 2. 작업 완료 대기 (여기서 쿼리가 끝날 때까지 블로킹됨)
            # 쿼리에 에러가 있다면 이 라인에서 예외(Exception)가 발생합니다.
            query_job.result()

            # 3. 성공 시 출력
            print(f"✅ 쿼리 실행 성공! (Job ID: {query_job.job_id})")
            print(f"■ {td_str} statics_daily_kpi Batch 완료")

        except Exception as e:
            # 4. 실패 시 출력
            print(f"❌ 쿼리 실행 중 에러 발생: {e}")
            raise e
    
    print("✅ statics_daily_kpi ETL 완료")
    return True
