# Airflow function
from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.models.baseoperator import chain
from google.oauth2 import service_account
import json
import calendar

from google.cloud import bigquery
from google.auth.transport.requests import Request
import logging

from datetime import datetime, timezone, timedelta

# 유틸 함수 불러오기
from ETL_Utils import init_clients, calc_target_date, target_date_range


ETL_ALL_Fact = Dataset('ETL_ALL_Fact')

#### Dimension table 처리 함수 불러오기
PROJECT_ID = "datahub-478802"
LOCATION = "us-central1"


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
            SELECT
            TA.datekey,
            TA.joyple_game_code,
            TA.DAU,
            TB.DRU,
            TC.PU,
            CAST(IFNULL(TC.IAP_revenue, 0) + IFNULL(TD.IAA_rev, 0) AS INT64) as total_rev,
            CAST(IFNULL(TC.IAP_revenue, 0) AS INT64) as IAP_rev,
            CAST(IFNULL(TC.IAP_revenue, 0) - IFNULL(TC.IAP_none_market_revenue, 0) AS INT64) as IAP_market_rev,
            CAST(IFNULL(TC.IAP_none_market_revenue, 0) AS INT64) as IAP_none_market_rev,
            CAST(IFNULL(TD.IAA_rev, 0) AS INT64) as IAA_rev,
            ROUND(IFNULL(SAFE_DIVIDE(TC.PU, TA.DAU), 0) * 100, 2) as PUR,
            ROUND(IFNULL(SAFE_DIVIDE(TC.IAP_revenue, TC.PU), 0), 0) as ARPPU,
            ROUND(IFNULL(SAFE_DIVIDE(TC.IAP_revenue, TA.DAU), 0), 0) as ARPDAU, -- 이름 변경
            TE.installs_funnel,
            TF.installs_appsflyer,
            ROUND(IFNULL(SAFE_DIVIDE(TG.NNPU, TC.PU), 0) * 100, 2) as NNPUR,
            TG.NNPU,
            CAST(IFNULL(TG.NNPU_rev, 0) AS INT64) as NNPU_rev
            FROM
            (
                select datekey, joyple_game_code, count(distinct auth_account_name) as DAU
                from `datahub-478802.datahub.f_common_access`
                where datekey >= '{td_str}' and datekey < DATE_ADD('{td_str}', INTERVAL 1 DAY) AND
                access_type_id = 1
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
                where datekey >= '{td_str}' and datekey < DATE_ADD('{td_str}', INTERVAL 1 DAY) and
                reg_datekey = datekey
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



def etl_statics_weekly_kpi(**context):

    # 클라이언트 호출
    client = init_clients()["bq_client"]

    target_date, _ = calc_target_date(context['logical_date'])
    #################### 백필용 데이터 처리
    # target_date = target_date_range("2026-01-24", "2026-01-26")  ## 백필용

    for td_str in target_date:

        td_date = datetime.strptime(td_str, "%Y-%m-%d").date()
        day_of_week = td_date.weekday()  # 월요일=0, 일요일=6
        monday = td_date - timedelta(days=day_of_week)
        sunday = monday + timedelta(days=6)

        start_of_week = monday.strftime("%Y-%m-%d")  # 월요일
        end_of_week = sunday.strftime("%Y-%m-%d")
        
        # 일요일
        print(f"📝 대상주간: {start_of_week} ~ {end_of_week}")
               

        # ETL 작업 수행
        query = f"""
        MERGE INTO datahub-478802.datahub.statics_weekly_kpi as target
        USING (
                SELECT
                TA.datekey,
                TA.joyple_game_code,
                TA.WAU,
                TB.WRU,
                TC.WPU,
                CAST(IFNULL(TC.IAP_revenue, 0) + IFNULL(TD.IAA_rev, 0) AS INT64) as total_rev,
                CAST(IFNULL(TC.IAP_revenue, 0) AS INT64) as IAP_rev,
                CAST(IFNULL(TC.IAP_revenue, 0) - IFNULL(TC.IAP_none_market_revenue, 0) AS INT64) as IAP_market_rev,
                CAST(IFNULL(TC.IAP_none_market_revenue, 0) AS INT64) as IAP_none_market_rev,
                CAST(IFNULL(TD.IAA_rev, 0) AS INT64) as IAA_rev,
                ROUND(IFNULL(SAFE_DIVIDE(TC.WPU, TA.WAU), 0) * 100, 2) as PUR,
                ROUND(IFNULL(SAFE_DIVIDE(TC.IAP_revenue, TC.WPU), 0), 0) as ARPPU,
                ROUND(IFNULL(SAFE_DIVIDE(TC.IAP_revenue, TA.WAU), 0), 0) as ARPWAU,
                TE.installs_funnel,
                TF.installs_appsflyer,
                ROUND(IFNULL(SAFE_DIVIDE(TG.NNPU, TC.WPU), 0) * 100, 2) as NNPUR,
                TG.NNPU,
                CAST(IFNULL(TG.NNPU_rev, 0) AS INT64) as NNPU_rev
                FROM
                (
                    select DATE_TRUNC(datekey, WEEK(MONDAY)) as datekey
                    , joyple_game_code
                    , count(distinct auth_account_name) as WAU
                    from `datahub-478802.datahub.f_common_access`
                    where datekey >= '{start_of_week}' AND datekey <= '{end_of_week}' AND
                    access_type_id = 1
                    group by 1, 2
                ) as TA
                left join
                (
                    select DATE_TRUNC(reg_datekey, WEEK(MONDAY)) as datekey
                    , joyple_game_code
                    , count(distinct auth_account_name) as WRU
                    from `datahub-478802.datahub.f_common_register`
                    where reg_datekey >= '{start_of_week}' AND reg_datekey <= '{end_of_week}'
                    group by 1, 2
                ) as TB
                ON TA.datekey = TB.datekey AND TA.joyple_game_code = TB.joyple_game_code
                left join
                (
                    select DATE_TRUNC(datekey, WEEK(MONDAY)) as datekey
                    , joyple_game_code
                    , count(distinct auth_account_name) as WPU
                    , sum(revenue) as IAP_revenue
                    , sum(
                            CASE WHEN pg_id in (select pg_id from `datahub-478802.datahub.dim_special_pg`) 
                            AND platform_device_type in (select platform_device_type from `datahub-478802.datahub.dim_special_pg`)
                            THEN revenue END
                            ) as IAP_none_market_revenue
                    from `datahub-478802.datahub.f_common_payment`
                    where datekey >= '{start_of_week}' AND datekey <= '{end_of_week}'
                    group by 1, 2 
                ) as TC
                ON TA.datekey = TC.datekey AND TA.joyple_game_code = TC.joyple_game_code
                left join 
                (
                    select  DATE_TRUNC(watch_datekey, WEEK(MONDAY)) as datekey
                    , joyple_game_code
                    , sum(revenue_per_user_KRW) as IAA_rev
                    from `datahub-478802.datahub.f_IAA_auth_account_performance`
                    where watch_datekey >= '{start_of_week}' AND watch_datekey <= '{end_of_week}'
                    group by 1, 2
                ) as TD
                ON TA.datekey = TD.datekey AND TA.joyple_game_code = TD.joyple_game_code
                left join 
                (
                    select DATE_TRUNC(datekey, WEEK(MONDAY)) as datekey
                    , joyple_game_code
                    , count(distinct device_id) as installs_funnel
                    from `datahub-478802.datahub.f_funnel_access_first`
                    where datekey >= '{start_of_week}' AND datekey <= '{end_of_week}'
                    group by 1, 2
                ) as TE
                ON TA.datekey = TE.datekey AND TA.joyple_game_code = TE.joyple_game_code
                left join 
                (
                    select DATE_TRUNC(install_datekey, WEEK(MONDAY)) as datekey
                    , joyple_game_code
                    , count(distinct tracker_account_id) as installs_appsflyer
                    from `datahub-478802.datahub.f_tracker_install`
                    where install_datekey >= '{start_of_week}' AND install_datekey <= '{end_of_week}'
                    group by 1, 2
                ) as TF
                ON TA.datekey = TF.datekey AND TA.joyple_game_code = TF.joyple_game_code
                left join 
                (
                    select DATE_TRUNC(datekey, WEEK(MONDAY)) AS datekey
                    , joyple_game_code
                    , count(distinct auth_account_name) as NNPU
                    , sum(revenue) as NNPU_rev
                    from `datahub-478802.datahub.f_common_payment`
                    where datekey >= '{start_of_week}' AND datekey <= '{end_of_week}' AND
                    datekey = reg_datekey
                    and reg_datediff = 0
                    group by 1,2
                ) as TG
                ON TA.datekey = TG.datekey AND TA.joyple_game_code = TG.joyple_game_code
        ) as source
        ON target.datekey = source.datekey AND target.joyple_game_code = source.joyple_game_code
        WHEN MATCHED THEN 
        UPDATE SET 
            target.WAU = source.WAU,
            target.WRU = source.WRU,
            target.WPU = source.WPU,
            target.total_rev = source.total_rev,
            target.IAP_rev = source.IAP_rev,
            target.IAP_market_rev = source.IAP_market_rev,
            target.IAP_none_market_rev = source.IAP_none_market_rev,
            target.IAA_rev = source.IAA_rev,
            target.PUR = source.PUR,
            target.ARPPU = source.ARPPU,
            target.ARPWAU = source.ARPWAU,
            target.installs_funnel = source.installs_funnel,
            target.installs_appsflyer = source.installs_appsflyer,
            target.NNPUR = source.NNPUR,
            target.NNPU = source.NNPU,
            target.NNPU_rev = source.NNPU_rev
        WHEN NOT MATCHED THEN 
        INSERT
        (
            datekey
            , joyple_game_code
            , WAU
            , WRU
            , WPU
            , total_rev
            , IAP_rev
            , IAP_market_rev
            , IAP_none_market_rev
            , IAA_rev
            , PUR
            , ARPPU
            , ARPWAU
            , installs_funnel
            , installs_appsflyer
            , NNPUR
            , NNPU
            , NNPU_rev
        )
        VALUES
        (
            source.datekey
            , source.joyple_game_code
            , source.WAU
            , source.WRU
            , source.WPU
            , source.total_rev
            , source.IAP_rev
            , source.IAP_market_rev
            , source.IAP_none_market_rev
            , source.IAA_rev
            , source.PUR
            , source.ARPPU
            , source.ARPWAU
            , source.installs_funnel
            , source.installs_appsflyer
            , source.NNPUR
            , source.NNPU
            , source.NNPU_rev
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
            print(f"■ {td_str} statics_weekly_kpi Batch 완료")

        except Exception as e:
            # 4. 실패 시 출력
            print(f"❌ 쿼리 실행 중 에러 발생: {e}")
            raise e
    
    print("✅ statics_weekly_kpi ETL 완료")
    return True




def etl_statics_monthly_kpi(**context):

    # 클라이언트 호출
    client = init_clients()["bq_client"]

    target_date, _ = calc_target_date(context['logical_date'])
    #################### 백필용 데이터 처리
    # target_date = target_date_range("2026-01-24", "2026-01-26")  ## 백필용

    for td_str in target_date:
        # 1. 문자열을 date 객체로 변환
        td_date = datetime.strptime(td_str, "%Y-%m-%d").date()
        
        # 2. 해당 월의 1일 구하기
        start_of_month = td_date.replace(day=1)
        
        # 3. 해당 월의 마지막 날 구하기
        # calendar.monthrange는 (시작요일, 마지막날짜)를 튜플로 반환함
        _, last_day = calendar.monthrange(td_date.year, td_date.month)
        end_of_month = td_date.replace(day=last_day)
        
        # 4. 문자열 포맷팅
        start_str = start_of_month.strftime("%Y-%m-%d")
        end_str = end_of_month.strftime("%Y-%m-%d")
        
        # 일요일
        print(f"📝 대상월: {start_str} ~ {end_str}")
               

        # ETL 작업 수행
        query = f"""
        MERGE INTO datahub-478802.datahub.statics_monthly_kpi as target
        USING (
                SELECT 
                TA.datekey,
                TA.joyple_game_code,
                TA.MAU,
                TB.MRU,
                TC.MPU,
                CAST(IFNULL(TC.IAP_revenue, 0) + IFNULL(TD.IAA_rev, 0) AS INT64) as total_rev,
                CAST(IFNULL(TC.IAP_revenue, 0) AS INT64) as IAP_rev,
                CAST(IFNULL(TC.IAP_revenue, 0) - IFNULL(TC.IAP_none_market_revenue, 0) AS INT64) as IAP_market_rev,
                CAST(IFNULL(TC.IAP_none_market_revenue, 0) AS INT64) as IAP_none_market_rev,
                CAST(IFNULL(TD.IAA_rev, 0) AS INT64) as IAA_rev,
                ROUND(IFNULL(SAFE_DIVIDE(TC.MPU, TA.MAU), 0) * 100, 2) as PUR,
                ROUND(IFNULL(SAFE_DIVIDE(TC.IAP_revenue, TC.MPU), 0), 0) as ARPPU,
                ROUND(IFNULL(SAFE_DIVIDE(TC.IAP_revenue, TA.MAU), 0), 0) as ARPMAU, -- 이름 변경
                TE.installs_funnel,
                TF.installs_appsflyer,
                ROUND(IFNULL(SAFE_DIVIDE(TG.NNPU, TC.MPU), 0) * 100, 2) as NNPUR,
                TG.NNPU,
                CAST(IFNULL(TG.NNPU_rev, 0) AS INT64) as NNPU_rev
                FROM
                (
                    select DATE_TRUNC(datekey, MONTH) as datekey
                    , joyple_game_code
                    , count(distinct auth_account_name) as MAU
                    from `datahub-478802.datahub.f_common_access`
                    where datekey >= '{start_str}' AND datekey <= '{end_str}' AND
                    access_type_id = 1
                    group by 1, 2
                ) as TA
                left join
                (
                    select DATE_TRUNC(reg_datekey, MONTH) as datekey
                    , joyple_game_code
                    , count(distinct auth_account_name) as MRU
                    from `datahub-478802.datahub.f_common_register`
                    where reg_datekey >= '{start_str}' AND reg_datekey <= '{end_str}'
                    group by 1, 2
                ) as TB
                ON TA.datekey = TB.datekey AND TA.joyple_game_code = TB.joyple_game_code
                left join
                (
                    select DATE_TRUNC(datekey, MONTH) as datekey
                    , joyple_game_code
                    , count(distinct auth_account_name) as MPU
                    , sum(revenue) as IAP_revenue
                    , sum(
                            CASE WHEN pg_id in (select pg_id from `datahub-478802.datahub.dim_special_pg`) 
                            AND platform_device_type in (select platform_device_type from `datahub-478802.datahub.dim_special_pg`)
                            THEN revenue END
                            ) as IAP_none_market_revenue
                    from `datahub-478802.datahub.f_common_payment`
                    where datekey >= '{start_str}' AND datekey <= '{end_str}'
                    group by 1, 2 
                ) as TC
                ON TA.datekey = TC.datekey AND TA.joyple_game_code = TC.joyple_game_code
                left join 
                (
                    select  DATE_TRUNC(watch_datekey, MONTH) as datekey
                    , joyple_game_code
                    , sum(revenue_per_user_KRW) as IAA_rev
                    from `datahub-478802.datahub.f_IAA_auth_account_performance`
                    where watch_datekey >= '{start_str}' AND watch_datekey <= '{end_str}'
                    group by 1, 2
                ) as TD
                ON TA.datekey = TD.datekey AND TA.joyple_game_code = TD.joyple_game_code
                left join 
                (
                    select DATE_TRUNC(datekey, MONTH) as datekey
                    , joyple_game_code
                    , count(distinct device_id) as installs_funnel
                    from `datahub-478802.datahub.f_funnel_access_first`
                    where datekey >= '{start_str}' AND datekey <= '{end_str}'
                    group by 1, 2
                ) as TE
                ON TA.datekey = TE.datekey AND TA.joyple_game_code = TE.joyple_game_code
                left join 
                (
                    select DATE_TRUNC(install_datekey, MONTH) as datekey
                    , joyple_game_code
                    , count(distinct tracker_account_id) as installs_appsflyer
                    from `datahub-478802.datahub.f_tracker_install`
                    where install_datekey >= '{start_str}' AND install_datekey <= '{end_str}'
                    group by 1, 2
                ) as TF
                ON TA.datekey = TF.datekey AND TA.joyple_game_code = TF.joyple_game_code
                left join 
                (
                    select DATE_TRUNC(datekey, MONTH) AS datekey
                    , joyple_game_code
                    , count(distinct auth_account_name) as NNPU
                    , sum(revenue) as NNPU_rev
                    from `datahub-478802.datahub.f_common_payment`
                    where datekey >= '{start_str}' AND datekey <= '{end_str}' AND
                    datekey = reg_datekey
                    and reg_datediff = 0
                    group by 1,2
                ) as TG
                ON TA.datekey = TG.datekey AND TA.joyple_game_code = TG.joyple_game_code
        ) as source
        ON target.datekey = source.datekey AND target.joyple_game_code = source.joyple_game_code
        WHEN MATCHED THEN 
        UPDATE SET 
            target.MAU = source.MAU,
            target.MRU = source.MRU,
            target.MPU = source.MPU,
            target.total_rev = source.total_rev,
            target.IAP_rev = source.IAP_rev,
            target.IAP_market_rev = source.IAP_market_rev,
            target.IAP_none_market_rev = source.IAP_none_market_rev,
            target.IAA_rev = source.IAA_rev,
            target.PUR = source.PUR,
            target.ARPPU = source.ARPPU,
            target.ARPMAU = source.ARPMAU,
            target.installs_funnel = source.installs_funnel,
            target.installs_appsflyer = source.installs_appsflyer,
            target.NNPUR = source.NNPUR,
            target.NNPU = source.NNPU,
            target.NNPU_rev = source.NNPU_rev
        WHEN NOT MATCHED THEN 
        INSERT
        (
            datekey
            , joyple_game_code
            , MAU
            , MRU
            , MPU
            , total_rev
            , IAP_rev
            , IAP_market_rev
            , IAP_none_market_rev
            , IAA_rev
            , PUR
            , ARPPU
            , ARPMAU
            , installs_funnel
            , installs_appsflyer
            , NNPUR
            , NNPU
            , NNPU_rev
        )
        VALUES
        (
            source.datekey
            , source.joyple_game_code
            , source.MAU
            , source.MRU
            , source.MPU
            , source.total_rev
            , source.IAP_rev
            , source.IAP_market_rev
            , source.IAP_none_market_rev
            , source.IAA_rev
            , source.PUR
            , source.ARPPU
            , source.ARPMAU
            , source.installs_funnel
            , source.installs_appsflyer
            , source.NNPUR
            , source.NNPU
            , source.NNPU_rev
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
            print(f"■ {td_str} statics_monthly_kpi Batch 완료")

        except Exception as e:
            # 4. 실패 시 출력
            print(f"❌ 쿼리 실행 중 에러 발생: {e}")
            raise e
    
    print("✅ statics_monthly_kpi ETL 완료")
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
    dag_id='ETL_statics',
    default_args=default_args,
    description='statics 데이터에 대해서 집계 처리',
    schedule= '50 21 * * *',  
    start_date=[ETL_ALL_Fact],
    catchup=False,
    tags=['ETL', 'statics', 'bigquery'],
) as dag:

    etl_statics_daily_kpi_task = PythonOperator(
        task_id='etl_statics_daily_kpi',
        python_callable=etl_statics_daily_kpi,
    )

    etl_statics_weekly_kpi_task = PythonOperator(
        task_id='etl_statics_weekly_kpi',
        python_callable=etl_statics_weekly_kpi,
    )

    etl_statics_monthly_kpi_task = PythonOperator(
        task_id='etl_statics_monthly_kpi',
        python_callable=etl_statics_monthly_kpi,
    )

    # 태스크 의존성 설정
    etl_statics_daily_kpi_task >> etl_statics_weekly_kpi_task >> etl_statics_monthly_kpi_task