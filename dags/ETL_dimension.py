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

ETL_dimension = Dataset('ETL_dimension')

#### Dimension table 처리 함수 불러오기
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





def etl_dim_os(**context):

    # 클라이언트 호출
    client = init_clients()["bq_client"]

    logger = logging.getLogger(__name__)
    
    # [수정 1] 함수 내부에서 사용할 타임존 정의
    kst = pytz.timezone('Asia/Seoul')

    # context에서 날짜 계산 함수 호출
    target_date, _ = calc_target_date(context['logical_date'])
    
    #################### 백필용 데이터 처리
    # target_date = target_date_range("2026-01-24", "2026-01-26")  ## 백필용

    for td_str in target_date:
        # [수정 1] 문자열(String)을 datetime 객체로 변환
        # 넘어오는 날짜 형식이 'YYYY-MM-DD'라고 가정합니다.
        try:
            current_date_obj = datetime.strptime(td_str, "%Y-%m-%d")
        except ValueError:
            # 형식이 다를 경우에 대한 예외처리 (예: 시간까지 포함된 경우 등)
            # 필요에 따라 포맷을 수정하세요 ("%Y-%m-%d %H:%M:%S")
            print(f"⚠️ 날짜 형식이 잘못되었습니다: {td_str}")
            continue

        # [수정 2] pytz 라이브러리 사용 시 .replace(tzinfo=...) 보다는 .localize() 권장
        # .replace는 썸머타임이나 역사적 시간대 변경을 제대로 처리 못할 수 있음
        
        # KST 00:00:00 설정 (localize 사용)
        start_kst = kst.localize(current_date_obj)
        
        # KST -> UTC 변환
        start_utc = start_kst.astimezone(pytz.UTC)
        
        # 종료 시간 계산 (하루 뒤)
        end_kst = start_kst + timedelta(days=1)
        end_utc = end_kst.astimezone(pytz.UTC)

        print(f"📝 대상날짜: {td_str}")
        print(f"   ㄴ 시작시간(UTC): {start_utc}")
        print(f"   ㄴ 종료시간(UTC): {end_utc}")

        # ETL 작업 수행
        query = f"""
        MERGE `datahub-478802.datahub.dim_os_id` T
        USING (
            SELECT
                DISTINCT
                os_id, 
                CAST(null AS STRING) as os_name, 
                CAST(null AS STRING) as os_name_lower
            FROM `dataplatform-204306.CommonLog.Access`
            where log_time >= TIMESTAMP('{start_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
            AND log_time < TIMESTAMP('{end_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
        ) S
        on T.os_id = S.os_id
        WHEN MATCHED THEN
        UPDATE SET
            T.os_name = COALESCE(S.os_name, T.os_name),
            T.os_name_lower = COALESCE(S.os_name_lower, T.os_name_lower),
            T.create_timestamp = COALESCE(T.create_timestamp, CURRENT_TIMESTAMP())
        WHEN NOT MATCHED THEN 
        INSERT (os_id, os_name, os_name_lower, create_timestamp)
        VALUES (S.os_id, null, null, CURRENT_TIMESTAMP())
        """

        # 1. 쿼리 실행
        query_job = client.query(query)

        try:
            # 2. 작업 완료 대기 (여기서 쿼리가 끝날 때까지 블로킹됨)
            # 쿼리에 에러가 있다면 이 라인에서 예외(Exception)가 발생합니다.
            query_job.result()

            # 3. 성공 시 출력
            print(f"✅ 쿼리 실행 성공! (Job ID: {query_job.job_id})")
            print(f"■ {td_str} dim_os Batch 완료")

        except Exception as e:
            # 4. 실패 시 출력
            print(f"❌ 쿼리 실행 중 에러 발생: {e}")
            raise e
    
    print("✅ dim_os ETL 완료")
    return True


def etl_dim_AFC_campaign(**context):

    # 클라이언트 호출
    client = init_clients()["bq_client"]

    truncate_query = f"""
    TRUNCATE TABLE `datahub-478802.datahub.dim_AFC_campaign` 
    """

    query = f"""
    INSERT INTO  `datahub-478802.datahub.dim_AFC_campaign`
    (app_id, media_source, init_campaign, Gcat, uptdt_campaign, media_category, product_category, media, media_detail, optim, etc_category, os_cam, geo_cam, date_cam, 
    creative_no, device, setting_title, landing_title, ad_unit, mediation, create_YN, update_YN, rule_YN, target_group, media_group, upload_timestamp)
    
    SELECT DISTINCT
                    app_id                                    
                , NORMALIZE(media_source, NFC)            AS media_source
                , NORMALIZE(init_campaign, NFC)           AS init_campaign
                , gcat                                    AS gcat
                , NORMALIZE(uptdt_campaign, NFC)          AS uptdt_campaign
                , media_category                          AS media_category
                , product_category                        AS product_category
                , media                                   AS media
                , media_detail                            AS media_detail
                , optim                                   AS optim
                , etc_category                            AS etc_category
                , os_cam                                  AS os_cam
                , geo_cam                                 AS geo_cam
                , date_cam                                AS date_cam
                , creative_no                             AS creative_no
                , device                                  AS device
                , setting_title                           AS setting_title
                , landing_title                           AS landing_title
                , ad_unit                                 AS ad_unit
                , mediation                               AS mediation
                , create_yn                               AS create_yn
                , update_yn                               AS update_yn
                , rule_yn                                 AS rule_yn
                , target_group                            AS target_group
                , CASE WHEN media_category in ('Google', 'Google-ACP', 'Google-PC', 'Google-Re') THEN 'Google'
                        WHEN media_category in ('Facebook', 'Facebook-3rd', 'Facebook-Gaming', 'Facebook-PC', 'Facebook-Playable', 'Facebook-Re') THEN 'FB'
                        WHEN media_category in ('ADNW','ADNW-Re')                   THEN 'ADNW'
                        WHEN LOWER(gcat) in ('organic','unknown')   THEN 'Organic'
                        ELSE 'Other' 
                    END AS media_group  -- 각 빅미디어 모든 매체카테고리 추가
                , upload_time                              AS upload_timestamp  
            FROM (
            SELECT app_id
                , CASE WHEN LOWER(TRIM(media_source)) = 'organic' THEN 'Organic' ELSE gcat END AS gcat
                , media_category
                , product_category
                , media
                , media_detail
                , CASE WHEN LOWER(TRIM(media_source)) = 'organic' THEN 'Organic'
                        ELSE TRIM(media_source) END AS media_source
                , optim
                , etc_category
                , os                                  AS os_cam
                , IF(location = 'UK', 'GB', location) AS geo_cam
                , cmpgn_dt                            AS date_cam
                , creative_no 
                , device
                , setting_title
                , landing_title
                , ad_unit
                , mediation
                , create_yn
                , update_yn
                , rule_yn
                , init_campaign
                , uptdt_campaign
                , upload_time
                , case 
                        when etc_category = 'L&F' then '그룹없음'
                        when (media_category = 'Mytarget.Self' and gcat = 'UA' and product_category is null and optim = 'MAIA') then 'UA-Install'
                        when lower(trim(media_source)) = 'organic' then 'Organic'
                        -- 현재 모두 install로 남고 있지만 데이터 수정하면 제대로 남을 예정 - ad_name 컬럼이 없어서 대응을 못함.
                        --    when (media_category = 'Facebook' and gcat = 'UA' and product_category is null and optim = 'NONE' and optim2 = 'VO') then 'UA-HVU'
                        --    when (media_category = 'Facebook' and gcat = 'UA' and product_category is null and optim = 'NONE' and optim2 = 'MAIA') then 'UA-Install'
                        --    when (media_category = 'Facebook' and gcat = 'UA' and product_category is null and optim = 'NONE' and optim2 = 'AEO') then 'UA-VU' 
                        else target_group
                    end as target_group -- 데이터 처리 전까지만 하드코딩 대응 수정된 이후에 하드코딩은 삭제 예정       
            FROM `dataplatform-bdts.mas.v_af_campaign_rule_group`
            ) 
    """
    # 1. 쿼리 실행
    truncate_query_job = client.query(truncate_query)
    truncate_query_job.result()  # 작업 완료 대기
    query_job = client.query(query)

    try:
        # 2. 작업 완료 대기 (여기서 쿼리가 끝날 때까지 블로킹됨)
        # 쿼리에 에러가 있다면 이 라인에서 예외(Exception)가 발생합니다.
        query_job.result()

        # 3. 성공 시 출력
        print(f"✅ 쿼리 실행 성공! (Job ID: {query_job.job_id})")

    except Exception as e:
        # 4. 실패 시 출력
        print(f"❌ 쿼리 실행 중 에러 발생: {e}")
        # Airflow에서 Task를 '실패(Failed)'로 처리하려면 에러를 다시 던져줘야 합니다.
        raise e
    
    print("✅ dim_AFC_campaign ETL 완료")
    
    return True


def etl_dim_auth_method_id(**context):

    # 클라이언트 호출
    client = init_clients()["bq_client"]

    logger = logging.getLogger(__name__)
    
    # [수정 1] 함수 내부에서 사용할 타임존 정의
    kst = pytz.timezone('Asia/Seoul')

    # context에서 날짜 계산 함수 호출
    target_date, _ = calc_target_date(context['logical_date'])

    ####################
    # target_date = target_date_range("2026-01-24", "2026-01-26")  ## 백필용
    # run_kst = None

    for td_str in target_date:
        # [수정 1] 문자열(String)을 datetime 객체로 변환
        # 넘어오는 날짜 형식이 'YYYY-MM-DD'라고 가정합니다.
        try:
            current_date_obj = datetime.strptime(td_str, "%Y-%m-%d")
        except ValueError:
            # 형식이 다를 경우에 대한 예외처리 (예: 시간까지 포함된 경우 등)
            # 필요에 따라 포맷을 수정하세요 ("%Y-%m-%d %H:%M:%S")
            print(f"⚠️ 날짜 형식이 잘못되었습니다: {td_str}")
            continue

        # [수정 2] pytz 라이브러리 사용 시 .replace(tzinfo=...) 보다는 .localize() 권장
        # .replace는 썸머타임이나 역사적 시간대 변경을 제대로 처리 못할 수 있음
        
        # KST 00:00:00 설정 (localize 사용)
        start_kst = kst.localize(current_date_obj)
        
        # KST -> UTC 변환
        start_utc = start_kst.astimezone(pytz.UTC)
        
        # 종료 시간 계산 (하루 뒤)
        end_kst = start_kst + timedelta(days=1)
        end_utc = end_kst.astimezone(pytz.UTC)

        print(f"📝 대상날짜: {td_str}")
        print(f"   ㄴ 시작시간(UTC): {start_utc}")
        print(f"   ㄴ 종료시간(UTC): {end_utc}")

        query = f"""
        MERGE `datahub-478802.datahub.dim_auth_method_id` T
        USING (
            SELECT
                DISTINCT
                auth_method_id, 
                CAST(null as STRING) as auth_type_id_KR, 
                CAST(null as STRING) as auth_type_id_EN
            FROM `dataplatform-204306.CommonLog.Access`
            where log_time >= TIMESTAMP('{start_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
            AND log_time < TIMESTAMP('{end_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
        ) S
        on T.auth_method_id = S.auth_method_id
        WHEN MATCHED THEN
        UPDATE SET
            T.auth_method_id = COALESCE(S.auth_method_id, T.auth_method_id),
            T.auth_method_id_KR = COALESCE(S.auth_type_id_KR, T.auth_method_id_KR),
            T.auth_method_id_EN = COALESCE(S.auth_type_id_EN, T.auth_method_id_EN),
            T.create_timestamp = COALESCE(T.create_timestamp, CURRENT_TIMESTAMP())
        WHEN NOT MATCHED THEN 
        INSERT (auth_method_id, auth_method_id_KR, auth_method_id_EN, create_timestamp)
        VALUES (S.auth_method_id, null, null, CURRENT_TIMESTAMP())
        """

        query_job = client.query(query)

        try:
            # 2. 작업 완료 대기 (여기서 쿼리가 끝날 때까지 블로킹됨)
            query_job.result()

            # 3. 성공 시 출력
            print(f"✅ 쿼리 실행 성공! (Job ID: {query_job.job_id})")
            print(f"■ {td_str} dim_auth_method_id Batch 완료")

        except Exception as e:
            # 4. 실패 시 출력
            print(f"❌ 쿼리 실행 중 에러 발생: {e}")
            raise e

    print("✅ dim_AFC_campaign ETL 완료")
    
    return True


def etl_dim_product_code(**context):

    # 클라이언트 호출
    client = init_clients()["bq_client"]

    logger = logging.getLogger(__name__)
    
    # [수정 1] 함수 내부에서 사용할 타임존 정의
    kst = pytz.timezone('Asia/Seoul')

    # context에서 날짜 계산 함수 호출
    target_date, _ = calc_target_date(context['logical_date'])

    ####################
    # target_date = target_date_range("2026-01-24", "2026-01-26")  ## 백필용
    # run_kst = None


    for td_str in target_date:
        # [수정 1] 문자열(String)을 datetime 객체로 변환
        # 넘어오는 날짜 형식이 'YYYY-MM-DD'라고 가정합니다.
        try:
            current_date_obj = datetime.strptime(td_str, "%Y-%m-%d")
        except ValueError:
            # 형식이 다를 경우에 대한 예외처리 (예: 시간까지 포함된 경우 등)
            # 필요에 따라 포맷을 수정하세요 ("%Y-%m-%d %H:%M:%S")
            print(f"⚠️ 날짜 형식이 잘못되었습니다: {td_str}")
            continue

        # [수정 2] pytz 라이브러리 사용 시 .replace(tzinfo=...) 보다는 .localize() 권장
        # .replace는 썸머타임이나 역사적 시간대 변경을 제대로 처리 못할 수 있음
        
        # KST 00:00:00 설정 (localize 사용)
        start_kst = kst.localize(current_date_obj)
        
        # KST -> UTC 변환
        start_utc = start_kst.astimezone(pytz.UTC)
        
        # 종료 시간 계산 (하루 뒤)
        end_kst = start_kst + timedelta(days=1)
        end_utc = end_kst.astimezone(pytz.UTC)

        print(f"📝 대상날짜: {td_str}")
        print(f"   ㄴ 시작시간(UTC): {start_utc}")
        print(f"   ㄴ 종료시간(UTC): {end_utc}")

        query = f"""
        MERGE `datahub-478802.datahub.dim_product_code` AS target
        USING(
        SELECT distinct joyple_game_code, product_code
        FROM `dataplatform-204306.CommonLog.Payment`
        WHERE log_time >= TIMESTAMP('{start_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
        AND log_time < TIMESTAMP('{end_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
        ) as source
        ON target.joyple_game_code = source.joyple_game_code AND target.product_code = source.product_code
        WHEN NOT MATCHED THEN
        INSERT (
        joyple_game_code,
        product_code,
        update_timestamp
        )
        VALUES(
        source.joyple_game_code,
        source.product_code,
        CURRENT_TIMESTAMP()
        )
        """
        query_job = client.query(query)

        try:
            # 2. 작업 완료 대기 (여기서 쿼리가 끝날 때까지 블로킹됨)
            # 쿼리에 에러가 있다면 이 라인에서 예외(Exception)가 발생합니다.
            query_job.result()

            # 3. 성공 시 출력
            print(f"✅ 쿼리 실행 성공! (Job ID: {query_job.job_id})")
            print(f"■ {td_str} dim_product_code Batch 완료")

        except Exception as e:
            # 4. 실패 시 출력
            print(f"❌ 쿼리 실행 중 에러 발생: {e}")
            # Airflow에서 Task를 '실패(Failed)'로 처리하려면 에러를 다시 던져줘야 합니다.
            raise e
        
    print("✅ dim_product_code ETL 완료")
    return True


def adjust_dim_product_code(**context):

    # 클라이언트 호출
    client = init_clients()["bq_client"]

    logger = logging.getLogger(__name__)
    
    # [수정 1] 함수 내부에서 사용할 타임존 정의
    kst = pytz.timezone('Asia/Seoul')

    # context에서 날짜 계산 함수 호출
    target_date, _ = calc_target_date(context['logical_date'])

    ####################
    # target_date = target_date_range("2026-01-24", "2026-01-26")  ## 백필용
    # run_kst = None

    query = f"""
    MERGE `datahub-478802.datahub.dim_product_code` AS target
    USING (
        SELECT * FROM (
            SELECT 
            139 as joyple_game_code
            , CAST(PKind AS STRING) as product_code
            , CAST(null AS STRING) as goods_type
            , CAST(Category AS STRING) as shop_category
            , CAST(null AS STRING) as package_category
            , CAST(Price AS STRING) as price
            , CAST(PackageName AS STRING) as product_name
            , CAST(null AS STRING) as product_name_EN
            , CAST(null AS STRING) as product_name_JP
            FROM data-science-division-216308.PackageInfo.CFWZ_PackageInfo

            UNION ALL

            SELECT 
            129 as joyple_game_code
            , CAST(PACKAGE_KIND  AS STRING) as product_code
            , CAST(null AS STRING) as goods_type
            , CAST(Category AS STRING) as shop_category
            , CAST(CATEGORY AS STRING) as package_category
            , CAST(Price AS STRING) as price
            , CAST(PACKAGE_NAME AS STRING) as product_name
            , CAST(null AS STRING) as product_name_EN
            , CAST(null AS STRING) as product_name_JP
            FROM data-science-division-216308.PackageInfo.GNSS_PackageInfo

            UNION ALL

            SELECT 
            147 as joyple_game_code
            , CAST(package_kind AS STRING) as product_code
            , CAST(goods_type AS STRING) as goods_type
            , CAST(category_shop AS STRING) as shop_category
            , CAST(category_package AS STRING) as package_category
            , CAST(price AS STRING) as price
            , CAST(package_name AS STRING) as product_name
            , CAST(null AS STRING) as product_name_EN
            , CAST(null AS STRING) as product_name_JP
            FROM data-science-division-216308.PackageInfo.PackageInfo_BLS

            UNION ALL

            SELECT 
            140 as joyple_game_code
            , CAST(package_kind AS STRING) as product_code
            , CAST(goods_type AS STRING) as goods_type
            , CAST(category_shop AS STRING) as shop_category
            , CAST(category_package AS STRING) as package_category
            , CAST(price AS STRING) as price
            , CAST(package_name AS STRING) as product_name
            , CAST(null AS STRING) as product_name_EN
            , CAST(null AS STRING) as product_name_JP
            FROM data-science-division-216308.PackageInfo.PackageInfo_BLSKR

            UNION ALL

            SELECT 
            154 as joyple_game_code
            , CAST(package_kind AS STRING) as product_code
            , CAST(goods_type AS STRING) as goods_type
            , CAST(category_shop AS STRING) as shop_category
            , CAST(category_package AS STRING) as package_category
            , CAST(price AS STRING) as price
            , CAST(package_name AS STRING) as product_name
            , CAST(null AS STRING) as product_name_EN
            , CAST(null AS STRING) as product_name_JP
            FROM data-science-division-216308.PackageInfo.PackageInfo_C4

            UNION ALL

            SELECT 
            139 as joyple_game_code
            , CAST(package_kind AS STRING) as product_code
            , CAST(goods_type AS STRING) as goods_type
            , CAST(category_shop AS STRING) as shop_category
            , CAST(category_package AS STRING) as package_category
            , CAST(price AS STRING) as price
            , CAST(package_name AS STRING) as product_name
            , CAST(null AS STRING) as product_name_EN
            , CAST(null AS STRING) as product_name_JP
            FROM data-science-division-216308.PackageInfo.PackageInfo_CFWZ

            UNION ALL

            SELECT 
            155 as joyple_game_code
            , CAST(package_kind AS STRING) as product_code
            , CAST(goods_type AS STRING) as goods_type
            , CAST(category_shop AS STRING) as shop_category
            , CAST(category_package AS STRING) as package_category
            , CAST(price AS STRING) as price
            , CAST(package_name AS STRING) as product_name
            , CAST(null AS STRING) as product_name_EN
            , CAST(null AS STRING) as product_name_JP
            FROM data-science-division-216308.PackageInfo.PackageInfo_DRB

            UNION ALL

            SELECT 
            30003 as joyple_game_code
            , CAST(package_kind AS STRING) as product_code
            , CAST(goods_type AS STRING) as goods_type
            , CAST(category_shop AS STRING) as shop_category
            , CAST(category_package AS STRING) as package_category
            , CAST(price AS STRING) as price
            , CAST(package_name AS STRING) as product_name
            , CAST(null AS STRING) as product_name_EN
            , CAST(null AS STRING) as product_name_JP
            FROM data-science-division-216308.PackageInfo.PackageInfo_DS

            UNION ALL

            SELECT 
            153 as joyple_game_code
            , CAST(package_kind AS STRING) as product_code
            , CAST(goods_type AS STRING) as goods_type
            , CAST(category_shop AS STRING) as shop_category
            , CAST(category_package AS STRING) as package_category
            , CAST(price AS STRING) as price
            , CAST(package_name AS STRING) as product_name
            , CAST(null AS STRING) as product_name_EN
            , CAST(null AS STRING) as product_name_JP
            FROM data-science-division-216308.PackageInfo.PackageInfo_GBCC

            UNION ALL

            SELECT 
            133 as joyple_game_code
            , CAST(package_kind AS STRING) as product_code
            , CAST(goods_type AS STRING) as goods_type
            , CAST(category_shop AS STRING) as shop_category
            , CAST(category_package AS STRING) as package_category
            , CAST(price AS STRING) as price
            , CAST(package_name AS STRING) as product_name
            , CAST(null AS STRING) as product_name_EN
            , CAST(null AS STRING) as product_name_JP
            FROM data-science-division-216308.PackageInfo.PackageInfo_GBTW

            UNION ALL

            SELECT 
            156 as joyple_game_code
            , CAST(package_kind AS STRING) as product_code
            , CAST(goods_type AS STRING) as goods_type
            , CAST(category_shop AS STRING) as shop_category
            , CAST(category_package AS STRING) as package_category
            , CAST(price AS STRING) as price
            , CAST(package_name AS STRING) as product_name
            , CAST(null AS STRING) as product_name_EN
            , CAST(null AS STRING) as product_name_JP
            FROM data-science-division-216308.PackageInfo.PackageInfo_JTWN

            UNION ALL

            SELECT 
            148 as joyple_game_code
            , CAST(package_kind AS STRING) as product_code
            , CAST(goods_type AS STRING) as goods_type
            , CAST(category_shop AS STRING) as shop_category
            , CAST(category_package AS STRING) as package_category
            , CAST(price AS STRING) as price
            , CAST(package_name AS STRING) as product_name
            , CAST(null AS STRING) as product_name_EN
            , CAST(null AS STRING) as product_name_JP
            FROM data-science-division-216308.PackageInfo.PackageInfo_KOFS

            UNION ALL

            SELECT 
            151 as joyple_game_code
            , CAST(package_kind AS STRING) as product_code
            , CAST(goods_type AS STRING) as goods_type
            , CAST(category_shop AS STRING) as shop_category
            , CAST(category_package AS STRING) as package_category
            , CAST(price AS STRING) as price
            , CAST(package_name AS STRING) as product_name
            , CAST(null AS STRING) as product_name_EN
            , CAST(null AS STRING) as product_name_JP
            FROM data-science-division-216308.PackageInfo.PackageInfo_LOL

            UNION ALL

            SELECT 
            159 as joyple_game_code
            , CAST(A.Package_Kind AS STRING) as product_code
            , CAST(A.goods_type AS STRING) as goods_type
            , CAST(A.Cat_Shop AS STRING) as shop_category
            , CAST(A.Cat_Package AS STRING) as package_category
            , CAST(A.Price AS STRING) as price
            , CAST(A.Package_Name AS STRING) as product_name
            , A.Package_Name_ENG as product_name_EN
            , CAST(B.package_name_jp AS STRING) as product_name_JP
            FROM data-science-division-216308.PackageInfo.PackageInfo_Notion_RESU as A
            left join 
            data-science-division-216308.PackageInfo.PackageInfo_Notion_RESU_JP as B
            ON A.Package_Kind = B.Package_Kind

            UNION ALL

            SELECT 
            1590 as joyple_game_code
            , CAST(A.Package_Kind AS STRING) as product_code
            , CAST(A.goods_type AS STRING) as goods_type
            , CAST(A.Cat_Shop AS STRING) as shop_category
            , CAST(A.Cat_Package AS STRING) as package_category
            , CAST(A.Price AS STRING) as price
            , CAST(A.Package_Name AS STRING) as product_name
            , A.Package_Name_ENG as product_name_EN
            , CAST(B.package_name_jp AS STRING) as product_name_JP
            FROM data-science-division-216308.PackageInfo.PackageInfo_Notion_RESU as A
            left join 
            data-science-division-216308.PackageInfo.PackageInfo_Notion_RESU_JP as B
            ON A.Package_Kind = B.Package_Kind

            UNION ALL

            SELECT 
            131 as joyple_game_code
            , CAST(package_kind AS STRING) as product_code
            , CAST(goods_type AS STRING) as goods_type
            , CAST(category_shop AS STRING) as shop_category
            , CAST(category_package AS STRING) as package_category
            , CAST(price AS STRING) as price
            , CAST(package_name AS STRING) as product_name
            , CAST(null AS STRING) as product_name_EN
            , CAST(null AS STRING) as product_name_JP
            FROM data-science-division-216308.PackageInfo.PackageInfo_POTC

            UNION ALL

            SELECT 
            142 as joyple_game_code
            , CAST(package_kind AS STRING) as product_code
            , CAST(goods_type AS STRING) as goods_type
            , CAST(category_shop AS STRING) as shop_category
            , CAST(category_package AS STRING) as package_category
            , CAST(price AS STRING) as price
            , CAST(package_name AS STRING) as product_name
            , CAST(null AS STRING) as product_name_EN
            , CAST(null AS STRING) as product_name_JP
            FROM data-science-division-216308.PackageInfo.PackageInfo_TERA

            UNION ALL

            SELECT 
            30001 as joyple_game_code
            , CAST(package_kind AS STRING) as product_code
            , CAST(goods_type AS STRING) as goods_type
            , CAST(category_shop AS STRING) as shop_category
            , CAST(category_package AS STRING) as package_category
            , CAST(price AS STRING) as price
            , CAST(package_name AS STRING) as product_name
            , CAST(null AS STRING) as product_name_EN
            , CAST(null AS STRING) as product_name_JP
            FROM data-science-division-216308.PackageInfo.PackageInfo_WWM
        )
        -- [핵심 수정] 중복 제거 로직 추가
        QUALIFY ROW_NUMBER() OVER(PARTITION BY joyple_game_code, product_code ORDER BY product_name DESC) = 1
    ) as source
    ON target.joyple_game_code = source.joyple_game_code AND target.product_code = source.product_code
    WHEN MATCHED AND target.product_name is null THEN
    UPDATE SET 
    target.product_name = source.product_name
    , target.goods_type = source.goods_type
    , target.shop_category = source.shop_category
    , target.package_category = source.package_category
    , target.price = source.price
    , target.product_name_EN = source.product_name_EN
    , target.product_name_JP = source.product_name_JP
    , target.update_timestamp = CURRENT_TIMESTAMP()
    """

    query_job = client.query(query)

    try:
        # 2. 작업 완료 대기 (여기서 쿼리가 끝날 때까지 블로킹됨)
        # 쿼리에 에러가 있다면 이 라인에서 예외(Exception)가 발생합니다.
        query_job.result()

        # 3. 성공 시 출력
        print(f"✅ 쿼리 실행 성공! (Job ID: {query_job.job_id})")
        print(f"■ {target_date} dim_package_kind Batch 완료")

    except Exception as e:
        # 4. 실패 시 출력
        print(f"❌ 쿼리 실행 중 에러 발생: {e}")
        # Airflow에서 Task를 '실패(Failed)'로 처리하려면 에러를 다시 던져줘야 합니다.
        raise e
    
    print("✅ dim_package_kind 조정 완료")
    return True


def etl_dim_exchange_rate(**context):

    # 클라이언트 호출
    client = init_clients()["bq_client"]

    logger = logging.getLogger(__name__)
    
    # [수정 1] 함수 내부에서 사용할 타임존 정의
    kst = pytz.timezone('Asia/Seoul')

    # context에서 날짜 계산 함수 호출
    target_date, _ = calc_target_date(context['logical_date'])

    #################### 백필용 데이터 처리
    # target_date = target_date_range("2026-01-01", "2026-01-26")  ## 백필용
    # run_kst = None

    for td_str in target_date:
        # [수정 1] 문자열(String)을 datetime 객체로 변환
        # 넘어오는 날짜 형식이 'YYYY-MM-DD'라고 가정합니다.
        try:
            current_date_obj = datetime.strptime(td_str, "%Y-%m-%d")
        except ValueError:
            # 형식이 다를 경우에 대한 예외처리 (예: 시간까지 포함된 경우 등)
            # 필요에 따라 포맷을 수정하세요 ("%Y-%m-%d %H:%M:%S")
            print(f"⚠️ 날짜 형식이 잘못되었습니다: {td_str}")
            continue

        # [수정 2] pytz 라이브러리 사용 시 .replace(tzinfo=...) 보다는 .localize() 권장
        # .replace는 썸머타임이나 역사적 시간대 변경을 제대로 처리 못할 수 있음
        
        # KST 00:00:00 설정 (localize 사용)
        start_kst = kst.localize(current_date_obj)
        
        # KST -> UTC 변환
        start_utc = start_kst.astimezone(pytz.UTC)
        
        # 종료 시간 계산 (하루 뒤)
        end_kst = start_kst + timedelta(days=1)
        end_utc = end_kst.astimezone(pytz.UTC)

        print(f"📝 대상날짜: {td_str}")
        print(f"   ㄴ 시작시간(UTC): {start_utc}")
        print(f"   ㄴ 종료시간(UTC): {end_utc}")

        query = f"""
        MERGE `datahub-478802.datahub.dim_exchange` T
        USING (
            WITH
            -- 1. 오늘 날짜의 환율 정보를 가져옵니다.
            today_exchange AS (
                SELECT
                    DATE(BaseDate, "Asia/Seoul") AS start_date,
                    FromCurrencyCode AS currency_code,
                    ARRAY_AGG(ExchangeRate ORDER BY BaseDate DESC LIMIT 1)[OFFSET(0)] AS exchange_rate
                FROM `dataplatform-204306.PublicInformation.Exchange`
                WHERE DATE(BaseDate, "Asia/Seoul") = DATE('{start_utc.strftime("%Y-%m-%d")}')
                AND ToCurrencyCode = "KRW"
                GROUP BY 1, 2
            ),
            -- 2. 오늘 Payment 로그에 있는 모든 통화 코드를 가져옵니다.
            all_currencies AS (
                SELECT DISTINCT currency_code
                FROM `dataplatform-204306.CommonLog.Payment`
                WHERE log_time >= TIMESTAMP('{start_utc.strftime("%Y-%m-%d %H:%M:%S")}')
                AND log_time < TIMESTAMP('{end_utc.strftime("%Y-%m-%d %H:%M:%S")}')
            ),
            -- 3. 오늘 환율 정보가 없는 통화에 대해, 가장 최근의 환율을 가져옵니다.
            latest_known_exchange AS (
                SELECT
                    currency as currency_code,
                    ARRAY_AGG(exchange_rate ORDER BY datekey DESC LIMIT 1)[OFFSET(0)] AS exchange_rate
                FROM `datahub-478802.datahub.dim_exchange`
                WHERE currency IN (SELECT currency_code FROM all_currencies)
                AND currency NOT IN (SELECT currency_code FROM today_exchange)
                GROUP BY currency
            )
            -- 4. 오늘 환율 정보와, 부족분을 채운 최근 환율 정보를 합칩니다.
            SELECT
                DATE('{td_str}') AS datekey,
                currency_code AS currency,
                exchange_rate
            FROM today_exchange
            UNION ALL
            SELECT
                DATE('{td_str}') AS datekey,
                currency_code AS currency,
                exchange_rate
            FROM latest_known_exchange
        ) S
        ON T.datekey = S.datekey AND T.currency = S.currency
        WHEN MATCHED AND T.exchange_rate IS DISTINCT FROM S.exchange_rate THEN
            UPDATE SET
                T.exchange_rate = S.exchange_rate,
                T.create_timestamp = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (datekey, currency, exchange_rate, create_timestamp)
            VALUES (S.datekey, S.currency, IF(S.currency = 'KRW', 1, S.exchange_rate), CURRENT_TIMESTAMP())
        """

        query_job = client.query(query)

        try:
            # 2. 작업 완료 대기 (여기서 쿼리가 끝날 때까지 블로킹됨)
            # 쿼리에 에러가 있다면 이 라인에서 예외(Exception)가 발생합니다.
            query_job.result()

            # 3. 성공 시 출력
            print(f"✅ 쿼리 실행 성공! (Job ID: {query_job.job_id})")
            print(f"■ {td_str} dim_exchange_rate Batch 완료")

        except Exception as e:
            # 4. 실패 시 출력
            print(f"❌ 쿼리 실행 중 에러 발생: {e}")
            # Airflow에서 Task를 '실패(Failed)'로 처리하려면 에러를 다시 던져줘야 합니다.
            raise e
        
    print("✅ dim_exchange_rate ETL 완료")
    
    return True

    
def etl_dim_game_id(**context):

    # 클라이언트 호출
    client = init_clients()["bq_client"]

    logger = logging.getLogger(__name__)
    
    # [수정 1] 함수 내부에서 사용할 타임존 정의
    kst = pytz.timezone('Asia/Seoul')

    # context에서 날짜 계산 함수 호출
    target_date, _ = calc_target_date(context['logical_date'])

    #################### 백필용 데이터 처리 
    # target_date = target_date_range("2026-01-24", "2026-01-26")  ## 백필용
    # run_kst = None


    for td_str in target_date:
        # [수정 1] 문자열(String)을 datetime 객체로 변환
        # 넘어오는 날짜 형식이 'YYYY-MM-DD'라고 가정합니다.
        try:
            current_date_obj = datetime.strptime(td_str, "%Y-%m-%d")
        except ValueError:
            # 형식이 다를 경우에 대한 예외처리 (예: 시간까지 포함된 경우 등)
            # 필요에 따라 포맷을 수정하세요 ("%Y-%m-%d %H:%M:%S")
            print(f"⚠️ 날짜 형식이 잘못되었습니다: {td_str}")
            continue

        # [수정 2] pytz 라이브러리 사용 시 .replace(tzinfo=...) 보다는 .localize() 권장
        # .replace는 썸머타임이나 역사적 시간대 변경을 제대로 처리 못할 수 있음
        
        # KST 00:00:00 설정 (localize 사용)
        start_kst = kst.localize(current_date_obj)
        
        # KST -> UTC 변환
        start_utc = start_kst.astimezone(pytz.UTC)
        
        # 종료 시간 계산 (하루 뒤)
        end_kst = start_kst + timedelta(days=1)
        end_utc = end_kst.astimezone(pytz.UTC)

        print(f"📝 대상날짜: {td_str}")
        print(f"   ㄴ 시작시간(UTC): {start_utc}")
        print(f"   ㄴ 종료시간(UTC): {end_utc}")

        query = f"""
        MERGE `datahub-478802.datahub.dim_game_id` T
        USING (
            SELECT
                DISTINCT
                game_id, CAST(null AS STRING) as game_name
            FROM `dataplatform-204306.CommonLog.Access`
            where log_time >= TIMESTAMP('{start_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
            AND log_time < TIMESTAMP('{end_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
        ) S
        on T.game_id = S.game_id
        WHEN MATCHED THEN
        UPDATE SET
            T.game_id = COALESCE(S.game_id, T.game_id),
            T.game_name = COALESCE(S.game_name, T.game_name),
            T.create_timestamp = COALESCE(T.create_timestamp, CURRENT_TIMESTAMP())
        WHEN NOT MATCHED THEN 
        INSERT (game_id, game_name, create_timestamp)
        VALUES (S.game_id, null, CURRENT_TIMESTAMP())
        """

        query_job = client.query(query)

        try:
            # 2. 작업 완료 대기 (여기서 쿼리가 끝날 때까지 블로킹됨)
            # 쿼리에 에러가 있다면 이 라인에서 예외(Exception)가 발생합니다.
            query_job.result()

            # 3. 성공 시 출력
            print(f"✅ 쿼리 실행 성공! (Job ID: {query_job.job_id})")
            print(f"■ {td_str} dim_game Batch 완료")

        except Exception as e:
            # 4. 실패 시 출력
            print(f"❌ 쿼리 실행 중 에러 발생: {e}")
            # Airflow에서 Task를 '실패(Failed)'로 처리하려면 에러를 다시 던져줘야 합니다.
            raise e

    print("✅ dim_game ETL 완료")
    
    return True


def etl_dim_app_id(**context):

    # 클라이언트 호출
    client = init_clients()["bq_client"]

    logger = logging.getLogger(__name__)
    
    # [수정 1] 함수 내부에서 사용할 타임존 정의
    kst = pytz.timezone('Asia/Seoul')

    # context에서 날짜 계산 함수 호출
    target_date, _ = calc_target_date(context['logical_date'])

    #################### 백필용 데이터 처리
    # target_date = target_date_range("2026-01-24", "2026-01-26")  ## 백필용
    # run_kst = None


    for td_str in target_date:
        # [수정 1] 문자열(String)을 datetime 객체로 변환
        # 넘어오는 날짜 형식이 'YYYY-MM-DD'라고 가정합니다.
        try:
            current_date_obj = datetime.strptime(td_str, "%Y-%m-%d")
        except ValueError:
            # 형식이 다를 경우에 대한 예외처리 (예: 시간까지 포함된 경우 등)
            # 필요에 따라 포맷을 수정하세요 ("%Y-%m-%d %H:%M:%S")
            print(f"⚠️ 날짜 형식이 잘못되었습니다: {td_str}")
            continue

        # [수정 2] pytz 라이브러리 사용 시 .replace(tzinfo=...) 보다는 .localize() 권장
        # .replace는 썸머타임이나 역사적 시간대 변경을 제대로 처리 못할 수 있음
        
        # KST 00:00:00 설정 (localize 사용)
        start_kst = kst.localize(current_date_obj)
        
        # KST -> UTC 변환
        start_utc = start_kst.astimezone(pytz.UTC)
        
        # 종료 시간 계산 (하루 뒤)
        end_kst = start_kst + timedelta(days=1)
        end_utc = end_kst.astimezone(pytz.UTC)

        print(f"📝 대상날짜: {td_str}")
        print(f"   ㄴ 시작시간(UTC): {start_utc}")
        print(f"   ㄴ 종료시간(UTC): {end_utc}")

        query = f"""
        MERGE `datahub-478802.datahub.dim_app_id` T
        USING (
            SELECT app_id, joyple_game_code, market_id 
            FROM dataplatform-204306.CommonLog.Access 
            WHERE log_time >= TIMESTAMP('{start_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
            AND log_time < TIMESTAMP('{end_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
            AND app_id IS NOT NULL 
            AND joyple_game_code IS NOT NULL 
            AND market_id IS NOT NULL
            -- [수정 핵심] app_id 별로 가장 최신(log_time DESC) 1건만 남김
            QUALIFY ROW_NUMBER() OVER(PARTITION BY app_id ORDER BY log_time DESC) = 1
        ) S
        ON T.app_id = S.app_id
        WHEN MATCHED THEN
        UPDATE SET
            -- T.app_id는 이미 같으므로 업데이트 불필요
            T.joyple_game_code = COALESCE(S.joyple_game_code, T.joyple_game_code),
            T.market_id = COALESCE(S.market_id, T.market_id)
            -- create_datetime 최초 생성일이므로 업데이트 하지 않음 (기존 유지)
        WHEN NOT MATCHED THEN 
        INSERT (app_id, joyple_game_code, market_id, create_datetime)
        VALUES (S.app_id, S.joyple_game_code, S.market_id, CURRENT_DATETIME("Asia/Seoul"))
        """

        query_job = client.query(query)

        try:
            # 2. 작업 완료 대기 (여기서 쿼리가 끝날 때까지 블로킹됨)
            # 쿼리에 에러가 있다면 이 라인에서 예외(Exception)가 발생합니다.git 
            query_job.result()

            # 3. 성공 시 출력
            print(f"✅ 쿼리 실행 성공! (Job ID: {query_job.job_id})")
            print(f"■ {td_str} dim_app_id Batch 완료")

        except Exception as e:
            # 4. 실패 시 출력
            print(f"❌ 쿼리 실행 중 에러 발생: {e}")
            # Airflow에서 Task를 '실패(Failed)'로 처리하려면 에러를 다시 던져줘야 합니다.
            raise e

    print("✅ dim_app_id ETL 완료")
    
    return True


def etl_dim_google_campaign(**context):

    # 클라이언트 호출
    client = init_clients()["bq_client"]

    logger = logging.getLogger(__name__)
    
    # [수정 1] 함수 내부에서 사용할 타임존 정의
    kst = pytz.timezone('Asia/Seoul')

    # context에서 날짜 계산 함수 호출
    target_date, _ = calc_target_date(context['logical_date'])

    #################### 백필용 데이터 처리 
    # target_date = target_date_range("2026-01-24", "2026-01-26")  ## 백필용
    # run_kst = None


    for td_str in target_date:
        # [수정 1] 문자열(String)을 datetime 객체로 변환
        # 넘어오는 날짜 형식이 'YYYY-MM-DD'라고 가정합니다.
        try:
            current_date_obj = datetime.strptime(td_str, "%Y-%m-%d")
        except ValueError:
            # 형식이 다를 경우에 대한 예외처리 (예: 시간까지 포함된 경우 등)
            # 필요에 따라 포맷을 수정하세요 ("%Y-%m-%d %H:%M:%S")
            print(f"⚠️ 날짜 형식이 잘못되었습니다: {td_str}")
            continue

        # [수정 2] pytz 라이브러리 사용 시 .replace(tzinfo=...) 보다는 .localize() 권장
        # .replace는 썸머타임이나 역사적 시간대 변경을 제대로 처리 못할 수 있음
        
        # KST 00:00:00 설정 (localize 사용)
        start_kst = kst.localize(current_date_obj)
        
        # KST -> UTC 변환
        start_utc = start_kst.astimezone(pytz.UTC)
        
        # 종료 시간 계산 (하루 뒤)
        end_kst = start_kst + timedelta(days=1)
        end_utc = end_kst.astimezone(pytz.UTC)

        print(f"📝 대상날짜: {td_str}")
        print(f"   ㄴ 시작시간(UTC): {start_utc}")
        print(f"   ㄴ 종료시간(UTC): {end_utc}")

        query = f"""
        MERGE `datahub-478802.datahub.dim_google_campaign` AS target
        USING
        (
        SELECT a.CampaignID    AS CampaignID
            , a.Info.cmpgn_nm AS CampaignName
            , a.Info.uptdt_dt AS UpdatedTimestamp
        FROM (
                SELECT REGEXP_REPLACE(a.cmpgn_id, '[^0-9]', '') AS CampaignID
                    , ARRAY_AGG(STRUCT(cmpgn_nm, a.uptdt_dt) ORDER BY a.cmpgn_dt DESC LIMIT 1)[OFFSET(0)] AS Info 
                FROM `dataplatform-bdts.mas.cost_campaign_rule_game` AS a
                WHERE a.cmpgn_dt >= DATE('{start_utc.strftime("%Y-%m-%d")}')  -- 수정됨 (TIMESTAMP -> DATE)
                AND a.cmpgn_dt < DATE('{end_utc.strftime("%Y-%m-%d")}')      -- 수정됨 (TIMESTAMP -> DATE)
                AND a.media_category LIKE '%Google%' 
                AND a.cmpgn_id IS NOT NULL 
                AND a.cmpgn_id NOT LIKE 'f%'
                GROUP BY REGEXP_REPLACE(a.cmpgn_id, '[^0-9]', '')
        ) AS a 
        ) AS source ON target.Campaign_id = source.CampaignID AND target.Campaign_name = source.CampaignName
        WHEN NOT MATCHED BY target THEN
        INSERT(Campaign_id, Campaign_name, create_timestamp)
        VALUES(
            source.CampaignID
            , source.CampaignName
            , CURRENT_TIMESTAMP()
        )
        """

        query_job = client.query(query)

        try:
            # 2. 작업 완료 대기 (여기서 쿼리가 끝날 때까지 블로킹됨)
            # 쿼리에 에러가 있다면 이 라인에서 예외(Exception)가 발생합니다.
            query_job.result()

            # 3. 성공 시 출력
            print(f"✅ 쿼리 실행 성공! (Job ID: {query_job.job_id})")
            print(f"■ {td_str} dim_google_campaign Batch 완료")

        except Exception as e:
            # 4. 실패 시 출력
            print(f"❌ 쿼리 실행 중 에러 발생: {e}")
            # Airflow에서 Task를 '실패(Failed)'로 처리하려면 에러를 다시 던져줘야 합니다.
            raise e

    print("✅ dim_google_campaign ETL 완료")
    
    return True


### etl_dim_ip4_country_code 보다는 앞에서 처리 되어야 함
def etl_dim_ip_range(**context):

    # 클라이언트 호출
    client = init_clients()["bq_client"]

    truncate_query = f"""
    TRUNCATE TABLE `datahub-478802.datahub.dim_ip_range`
    """

    query = f"""
    INSERT INTO `datahub-478802.datahub.dim_ip_range`
    (start_ip, end_ip, country_code, create_timestamp)
    SELECT StartIP, EndIP, CountryCode, CURRENT_TIMESTAMP()
    FROM dataplatform-204306.PublicInformation.IP2Location
    """

    # 1. 쿼리 실행
    truncate_query_job = client.query(truncate_query)
    truncate_query_job.result()  # 작업 완료 대기
    query_job = client.query(query)

    try:
        # 2. 작업 완료 대기 (여기서 쿼리가 끝날 때까지 블로킹됨)
        # 쿼리에 에러가 있다면 이 라인에서 예외(Exception)가 발생합니다.
        query_job.result()

        # 3. 성공 시 출력
        print(f"✅ 쿼리 실행 성공! (Job ID: {query_job.job_id})")

    except Exception as e:
        # 4. 실패 시 출력
        print(f"❌ 쿼리 실행 중 에러 발생: {e}")
        # Airflow에서 Task를 '실패(Failed)'로 처리하려면 에러를 다시 던져줘야 합니다.
        raise e
    
    print("✅ dim_ip_range ETL 완료")


### etl_dim_ip4_country_code 보다는 앞에서 처리 되어야 함
def etl_dim_ip_proxy(**context):

    # 클라이언트 호출
    client = init_clients()["bq_client"]

    truncate_query = f"""
    TRUNCATE TABLE `datahub-478802.datahub.dim_proxy`
    """

    query = f"""
    INSERT INTO `datahub-478802.datahub.dim_proxy`
    (proxy_ip, country_code, create_timestamp)
    SELECT ProxyIP, CountryCode, CURRENT_TIMESTAMP()
    FROM dataplatform-204306.PublicInformation.Proxy
    """

    # 1. 쿼리 실행
    truncate_query_job = client.query(truncate_query)
    truncate_query_job.result()  # 작업 완료 대기
    query_job = client.query(query)

    try:
        # 2. 작업 완료 대기 (여기서 쿼리가 끝날 때까지 블로킹됨)
        # 쿼리에 에러가 있다면 이 라인에서 예외(Exception)가 발생합니다.
        query_job.result()

        # 3. 성공 시 출력
        print(f"✅ 쿼리 실행 성공! (Job ID: {query_job.job_id})")

    except Exception as e:
        # 4. 실패 시 출력
        print(f"❌ 쿼리 실행 중 에러 발생: {e}")
        # Airflow에서 Task를 '실패(Failed)'로 처리하려면 에러를 다시 던져줘야 합니다.
        raise e
    
    print("✅ dim_ip_proxy ETL 완료")


def etl_dim_ip4_country_code(**context):

    # 클라이언트 호출
    client = init_clients()["bq_client"]

    logger = logging.getLogger(__name__)
    
    # [수정 1] 함수 내부에서 사용할 타임존 정의
    kst = pytz.timezone('Asia/Seoul')

    # context에서 날짜 계산 함수 호출
    target_date, _ = calc_target_date(context['logical_date'])

    #################### 백필용 데이터 처리
    # target_date = target_date_range("2026-01-24", "2026-01-26")  ## 백필용
    # run_kst = None

    for td_str in target_date:
        # [수정 1] 문자열(String)을 datetime 객체로 변환
        # 넘어오는 날짜 형식이 'YYYY-MM-DD'라고 가정합니다.
        try:
            current_date_obj = datetime.strptime(td_str, "%Y-%m-%d")
        except ValueError:
            # 형식이 다를 경우에 대한 예외처리 (예: 시간까지 포함된 경우 등)
            # 필요에 따라 포맷을 수정하세요 ("%Y-%m-%d %H:%M:%S")
            print(f"⚠️ 날짜 형식이 잘못되었습니다: {td_str}")
            continue

        # [수정 2] pytz 라이브러리 사용 시 .replace(tzinfo=...) 보다는 .localize() 권장
        # .replace는 썸머타임이나 역사적 시간대 변경을 제대로 처리 못할 수 있음
        
        # KST 00:00:00 설정 (localize 사용)
        start_kst = kst.localize(current_date_obj)
        
        # KST -> UTC 변환
        start_utc = start_kst.astimezone(pytz.UTC)
        
        # 종료 시간 계산 (하루 뒤)
        end_kst = start_kst + timedelta(days=1)
        end_utc = end_kst.astimezone(pytz.UTC)

        print(f"📝 대상날짜: {td_str}")
        print(f"   ㄴ 시작시간(UTC): {start_utc}")
        print(f"   ㄴ 종료시간(UTC): {end_utc}")

        query = f"""
            MERGE `datahub-478802.datahub.dim_ip4_country_code` AS a
            USING
            (
            SELECT a.IP as ip, IFNULL(c.country_code, b.country_code) AS country_code, UpdatedTimestamp AS create_timestamp
            FROM (
                SELECT a.IP
                    , TO_HEX(NET.SAFE_IP_FROM_STRING(CASE LENGTH(NET.SAFE_IP_FROM_STRING(a.IP)) WHEN 4 THEN CONCAT("::ffff:", a.IP) ELSE a.IP END)) AS HexIP
                    , MAX(UpdatedTimestamp) AS UpdatedTimestamp
                FROM (
                SELECT a.ip, MAX(a.event_time) AS UpdatedTimestamp
                FROM `dataplatform-204306.AppsflyerLog.LogsV2` AS a
                WHERE a.event_name = 'install'
                    AND a.event_time >= TIMESTAMP('{start_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
                    AND a.event_time < TIMESTAMP('{end_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
                GROUP BY a.ip  
                UNION ALL
                SELECT a.login_ip AS ip, MAX(a.idx) AS UpdatedTimestamp
                FROM `dataplatform-204306.JoypleLog.user_login_log` AS a
                WHERE a.idx >= TIMESTAMP('{start_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
                    AND a.idx < TIMESTAMP('{end_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
                GROUP BY a.login_ip
                UNION ALL
                SELECT a.ip, MAX(a.timestamp) AS UpdatedTimestamp
                FROM `dataplatform-204306.JoypleLog.payment_log` AS a
                WHERE a.timestamp >= TIMESTAMP('{start_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
                    AND a.timestamp < TIMESTAMP('{end_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
                GROUP BY a.ip
                UNION ALL
                SELECT a.ip, MAX(a.log_time) AS UpdatedTimestamp
                FROM `dataplatform-204306.CommonLog.Access` AS a
                WHERE a.log_time >= TIMESTAMP('{start_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
                    AND a.log_time < TIMESTAMP('{end_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
                GROUP BY a.ip
                UNION ALL
                SELECT a.ip, MAX(a.log_time) AS UpdatedTimestamp
                FROM `dataplatform-204306.CommonLog.Payment` AS a
                WHERE a.log_time >= TIMESTAMP('{start_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
                    AND a.log_time < TIMESTAMP('{end_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
                GROUP BY a.ip
                UNION ALL
                SELECT a.ip, MAX(a.log_time) AS UpdatedTimestamp
                FROM `dataplatform-204306.CommonLog.Funnel` AS a
                WHERE a.log_time >= TIMESTAMP('{start_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
                    AND a.log_time < TIMESTAMP('{end_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
                GROUP BY a.ip
                ) AS a
                GROUP BY a.IP
                , TO_HEX(NET.SAFE_IP_FROM_STRING(CASE LENGTH(NET.SAFE_IP_FROM_STRING(a.IP)) WHEN 4 THEN CONCAT("::ffff:", a.IP) ELSE a.IP END))
            ) AS a
            INNER JOIN `datahub-478802.datahub.dim_ip_range` AS b ON a.HexIP BETWEEN b.start_ip AND b.end_ip
            LEFT OUTER JOIN `datahub-478802.datahub.dim_proxy` AS c ON a.HexIP = TO_HEX(NET.SAFE_IP_FROM_STRING(CASE LENGTH(NET.SAFE_IP_FROM_STRING(c.proxy_ip)) WHEN 4 THEN  CONCAT("::ffff:", c.proxy_ip) ELSE c.proxy_ip END)) 
            ) AS b ON a.ip = b.ip
            WHEN MATCHED THEN
            UPDATE SET a.country_code = b.country_code, a.create_timestamp = GREATEST(a.create_timestamp, b.create_timestamp)
            WHEN NOT MATCHED THEN
            INSERT (ip, country_code, create_timestamp)
            VALUES (b.ip, b.country_code, b.create_timestamp);
            """
        
        # 1. 쿼리 실행
        query_job = client.query(query)

        try:
            # 2. 작업 완료 대기 (여기서 쿼리가 끝날 때까지 블로킹됨)
            # 쿼리에 에러가 있다면 이 라인에서 예외(Exception)가 발생합니다.
            query_job.result()

            # 3. 성공 시 출력
            print(f"✅ 쿼리 실행 성공! (Job ID: {query_job.job_id})")
            print(f"■ {td_str} dim_ip4_country_code Batch 완료")

        except Exception as e:
            # 4. 실패 시 출력
            print(f"❌ 쿼리 실행 중 에러 발생: {e}")
            # Airflow에서 Task를 '실패(Failed)'로 처리하려면 에러를 다시 던져줘야 합니다.
            raise e

    print("✅ dim_ip4_country_code ETL 완료")
    
    return True


def etl_dim_joyple_game_code(**context):

    # 클라이언트 호출
    client = init_clients()["bq_client"]

    logger = logging.getLogger(__name__)
    
    # [수정 1] 함수 내부에서 사용할 타임존 정의
    kst = pytz.timezone('Asia/Seoul')

    # context에서 날짜 계산 함수 호출
    target_date, _ = calc_target_date(context['logical_date'])

    #################### 백필용 데이터 처리
    # target_date = target_date_range("2026-01-24", "2026-01-26")  ## 백필용
    # run_kst = None

    for td_str in target_date:
        # [수정 1] 문자열(String)을 datetime 객체로 변환
        # 넘어오는 날짜 형식이 'YYYY-MM-DD'라고 가정합니다.
        try:
            current_date_obj = datetime.strptime(td_str, "%Y-%m-%d")
        except ValueError:
            # 형식이 다를 경우에 대한 예외처리 (예: 시간까지 포함된 경우 등)
            # 필요에 따라 포맷을 수정하세요 ("%Y-%m-%d %H:%M:%S")
            print(f"⚠️ 날짜 형식이 잘못되었습니다: {td_str}")
            continue

        # [수정 2] pytz 라이브러리 사용 시 .replace(tzinfo=...) 보다는 .localize() 권장
        # .replace는 썸머타임이나 역사적 시간대 변경을 제대로 처리 못할 수 있음
        
        # KST 00:00:00 설정 (localize 사용)
        start_kst = kst.localize(current_date_obj)
        
        # KST -> UTC 변환
        start_utc = start_kst.astimezone(pytz.UTC)
        
        # 종료 시간 계산 (하루 뒤)
        end_kst = start_kst + timedelta(days=1)
        end_utc = end_kst.astimezone(pytz.UTC)

        print(f"📝 대상날짜: {td_str}")
        print(f"   ㄴ 시작시간(UTC): {start_utc}")
        print(f"   ㄴ 종료시간(UTC): {end_utc}")

        query = f"""
        MERGE `datahub-478802.datahub.dim_joyple_game_code` AS a
        USING
        (
            SELECT a.joyple_game_code, a.game_id
                , MAX(UpdatedTimestamp) AS UpdatedTimestamp
            FROM (
                    SELECT a.joyple_game_code, a.game_id, MAX(a.log_time) AS UpdatedTimestamp
                    FROM `dataplatform-204306.CommonLog.Access` AS a
                    WHERE a.log_time >= TIMESTAMP('{start_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
                        AND a.log_time < TIMESTAMP('{end_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
                    GROUP BY a.joyple_game_code, a.game_id
                    UNION ALL
                    SELECT a.joyple_game_code, a.game_id, MAX(a.log_time) AS UpdatedTimestamp
                    FROM `dataplatform-204306.CommonLog.Payment` AS a
                    WHERE a.log_time >= TIMESTAMP('{start_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
                        AND a.log_time < TIMESTAMP('{end_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
                    GROUP BY a.joyple_game_code, a.game_id
                    UNION ALL
                    SELECT a.joyple_game_code, a.game_id, MAX(a.log_time) AS UpdatedTimestamp
                    FROM `dataplatform-204306.CommonLog.Funnel` AS a
                    WHERE a.log_time >= TIMESTAMP('{start_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
                        AND a.log_time < TIMESTAMP('{end_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
                    GROUP BY a.joyple_game_code, a.game_id
            ) as a
            GROUP BY a.joyple_game_code, a.game_id
        ) as t
        ON a.joyple_game_code = t.joyple_game_code
        WHEN MATCHED THEN
        UPDATE SET a.create_timestamp = GREATEST(a.create_timestamp, t.UpdatedTimestamp)
        WHEN NOT MATCHED THEN
        INSERT (joyple_game_code, game_id, game_code_name, game_name_KR, game_name_EN, IAA_use, create_timestamp, game_group_name)
        VALUES (t.joyple_game_code, game_id, null, null, null, null, t.UpdatedTimestamp, null);
        """
        
        # 1. 쿼리 실행
        query_job = client.query(query)

        try:
            # 2. 작업 완료 대기 (여기서 쿼리가 끝날 때까지 블로킹됨)
            # 쿼리에 에러가 있다면 이 라인에서 예외(Exception)가 발생합니다.
            query_job.result()

            # 3. 성공 시 출력
            print(f"✅ 쿼리 실행 성공! (Job ID: {query_job.job_id})")
            print(f"■ {td_str} dim_joyple_game_code Batch 완료")

        except Exception as e:
            # 4. 실패 시 출력
            print(f"❌ 쿼리 실행 중 에러 발생: {e}")
            # Airflow에서 Task를 '실패(Failed)'로 처리하려면 에러를 다시 던져줘야 합니다.
            raise e
        
    print("✅ dim_joyple_game_code ETL 완료")
    
    return True


def etl_dim_market_id(**context):

    # 클라이언트 호출
    client = init_clients()["bq_client"]

    logger = logging.getLogger(__name__)
    
    # [수정 1] 함수 내부에서 사용할 타임존 정의
    kst = pytz.timezone('Asia/Seoul')

    # context에서 날짜 계산 함수 호출
    target_date, _ = calc_target_date(context['logical_date'])

    ####################    백필용 데이터 처리
    # target_date = target_date_range("2026-01-24", "2026-01-26")  ## 백필용
    # run_kst = None

    for td_str in target_date:
        # [수정 1] 문자열(String)을 datetime 객체로 변환
        # 넘어오는 날짜 형식이 'YYYY-MM-DD'라고 가정합니다.
        try:
            current_date_obj = datetime.strptime(td_str, "%Y-%m-%d")
        except ValueError:
            # 형식이 다를 경우에 대한 예외처리 (예: 시간까지 포함된 경우 등)
            # 필요에 따라 포맷을 수정하세요 ("%Y-%m-%d %H:%M:%S")
            print(f"⚠️ 날짜 형식이 잘못되었습니다: {td_str}")
            continue

        # [수정 2] pytz 라이브러리 사용 시 .replace(tzinfo=...) 보다는 .localize() 권장
        # .replace는 썸머타임이나 역사적 시간대 변경을 제대로 처리 못할 수 있음
        
        # KST 00:00:00 설정 (localize 사용)
        start_kst = kst.localize(current_date_obj)
        
        # KST -> UTC 변환
        start_utc = start_kst.astimezone(pytz.UTC)
        
        # 종료 시간 계산 (하루 뒤)
        end_kst = start_kst + timedelta(days=1)
        end_utc = end_kst.astimezone(pytz.UTC)

        print(f"📝 대상날짜: {td_str}")
        print(f"   ㄴ 시작시간(UTC): {start_utc}")
        print(f"   ㄴ 종료시간(UTC): {end_utc}")

        query = f"""
        MERGE `datahub-478802.datahub.dim_market_id` AS a
        USING
        (
            SELECT a.market_id
                , MAX(UpdatedTimestamp) AS UpdatedTimestamp
            FROM (
            SELECT a.market_id, MAX(a.log_time) AS UpdatedTimestamp
            FROM `dataplatform-204306.CommonLog.Access` AS a
            WHERE a.log_time >= TIMESTAMP('{start_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
                AND a.log_time < TIMESTAMP('{end_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
            GROUP BY 1
            UNION ALL
            SELECT a.market_id, MAX(a.log_time) AS UpdatedTimestamp
            FROM `dataplatform-204306.CommonLog.Payment` AS a
            WHERE a.log_time >= TIMESTAMP('{start_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
                AND a.log_time < TIMESTAMP('{end_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
            GROUP BY 1
            UNION ALL
            SELECT a.market_id, MAX(a.log_time) AS UpdatedTimestamp
            FROM `dataplatform-204306.CommonLog.Funnel` AS a
            WHERE a.log_time >= TIMESTAMP('{start_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
                AND a.log_time < TIMESTAMP('{end_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
            GROUP BY 1
            ) as a
            GROUP BY 1
        ) as t
        ON a.market_id = t.market_id
        WHEN MATCHED THEN
        UPDATE SET a.create_timestamp = GREATEST(a.create_timestamp, t.UpdatedTimestamp)
        WHEN NOT MATCHED THEN
        INSERT (market_id)
        VALUES (t.market_id);
        """

        # 1. 쿼리 실행
        query_job = client.query(query)

        try:
            # 2. 작업 완료 대기 (여기서 쿼리가 끝날 때까지 블로킹됨)
            # 쿼리에 에러가 있다면 이 라인에서 예외(Exception)가 발생합니다.
            query_job.result()

            # 3. 성공 시 출력
            print(f"✅ 쿼리 실행 성공! (Job ID: {query_job.job_id})")
            print(f"■ {td_str} dim_market_id Batch 완료")

        except Exception as e:
            # 4. 실패 시 출력
            print(f"❌ 쿼리 실행 중 에러 발생: {e}")
            # Airflow에서 Task를 '실패(Failed)'로 처리하려면 에러를 다시 던져줘야 합니다.
            raise e

    print("✅ dim_market_id ETL 완료")
    
    return True



def etl_dim_package_kind(**context):
    # 클라이언트 호출
    client = init_clients()["bq_client"]
    
    truncate_query = f"""
    TRUNCATE TABLE `datahub-478802.datahub.dim_package_kind`
    """

    query = f"""
    INSERT INTO `datahub-478802.datahub.dim_package_kind`
    (UUID, joyple_game_code, package_kind, package_name_KR, package_name_JP, create_datetime)

    SELECT UUID, joyple_game_code, package_kind
    , max(package_name_KR) as package_name_KR, max(package_name_JP) as package_name_JP, max(create_datetime) as create_datetime
    FROM 
    (
    SELECT 
        CONCAT(CAST(159 AS STRING), "|", Package_Kind) AS UUID
        , 159          AS joyple_game_code
        , CAST(Package_Kind AS STRING) AS package_kind
        , CAST(Package_Name AS STRING) AS package_name_KR
        , CAST(null AS STRING) AS package_name_JP
        , CURRENT_DATETIME("Asia/Seoul") AS create_datetime
    FROM `data-science-division-216308.PackageInfo.PackageInfo_Notion_RESU`
    WHERE Package_Kind IS NOT NULL 
    AND Package_Name IS NOT NULL
    UNION ALL
    SELECT CONCAT(CAST(159 AS STRING), "|", Package_Kind) AS UUID
        , 159             AS joyple_game_code
        , CAST(Package_Kind AS STRING) AS package_kind
        , CAST(null AS STRING) AS package_name_KR
        , CAST(package_name_jp AS STRING) AS package_name_JP
        , CURRENT_DATETIME("Asia/Seoul") AS create_datetime
    FROM `data-science-division-216308.PackageInfo.PackageInfo_Notion_RESU_JP`
    WHERE Package_Kind IS NOT NULL 
    AND package_name_jp IS NOT NULL
    ) TA
    group by 1,2,3
    
    UNION ALL
    
    SELECT CONCAT(CAST(30001 AS STRING), "|", Package_Kind) AS UUID
        , 30001        AS joyple_game_code
        , CAST(Package_Kind AS STRING) AS package_kind
        , CAST(Package_Name AS STRING) AS package_name_KR
        , CAST(null AS STRING) AS package_name_JP
        , CURRENT_DATETIME("Asia/Seoul") AS create_datetime
    FROM `data-science-division-216308.PackageInfo.PackageInfo_WWM`
    WHERE Package_Kind IS NOT NULL 
    AND Package_Name IS NOT NULL
    
    UNION ALL
    
    SELECT CONCAT(CAST(30003 AS STRING), "|", Package_Kind) AS UUID
        , 30003        AS joyple_game_code
        , CAST(Package_Kind AS STRING) AS package_kind
        , CAST(Package_Name AS STRING) AS package_name_KR
        , CAST(null AS STRING) AS package_name_JP
        , CURRENT_DATETIME("Asia/Seoul") AS create_datetime
    FROM `data-science-division-216308.PackageInfo.PackageInfo_DS`
    WHERE Package_Kind IS NOT NULL 
    AND Package_Name IS NOT NULL
    
    UNION ALL
    
    SELECT CONCAT(CAST(131 AS STRING), "|", Package_Kind) AS UUID
        , 131          AS joyple_game_code
        , CAST(Package_Kind AS STRING) AS package_kind
        , CAST(Package_Name AS STRING) AS package_name_KR
        , CAST(null AS STRING) AS package_name_JP
        , CURRENT_DATETIME("Asia/Seoul") AS create_datetime
    FROM `data-science-division-216308.PackageInfo.PackageInfo_POTC`
    WHERE Package_Kind IS NOT NULL 
    AND Package_Name IS NOT NULL
    
    UNION ALL
    
    SELECT CONCAT(CAST(133 AS STRING), "|", PKind) AS UUID
        , 133          AS joyple_game_code
        , CAST(PKind AS STRING) AS package_kind
        , CAST(PackageName AS STRING) AS package_name_KR
        , CAST(null AS STRING) AS package_name_JP
        , CURRENT_DATETIME("Asia/Seoul") AS create_datetime
    FROM `data-science-division-216308.PackageInfo.GW_PackageInfo`
    WHERE PKind IS NOT NULL 
    AND PackageName IS NOT NULL
    
    UNION ALL
    
    SELECT CONCAT(CAST(155 AS STRING), "|", Package_Kind) AS UUID
        , 155          AS joyple_game_code
        , CAST(Package_Kind AS STRING) AS package_kind
        , CAST(Package_Name AS STRING) AS package_name_KR
        , CAST(null AS STRING) AS package_name_JP
        , CURRENT_DATETIME("Asia/Seoul") AS create_datetime
    FROM `data-science-division-216308.PackageInfo.PackageInfo_DRB`
    WHERE Package_Kind IS NOT NULL 
    AND Package_Name IS NOT NULL
    
    UNION ALL
    
    SELECT CONCAT(CAST(154 AS STRING), "|", Package_Kind) AS UUID
        , 154          AS joyple_game_code
        , CAST(Package_Kind AS STRING) AS package_kind
        , CAST(Package_Name AS STRING) AS package_name_KR
        , CAST(null AS STRING) AS package_name_JP
        , CURRENT_DATETIME("Asia/Seoul") AS create_datetime
    FROM `data-science-division-216308.PackageInfo.PackageInfo_C4`
    WHERE Package_Kind IS NOT NULL 
    AND Package_Name IS NOT NULL
    
    UNION ALL
    
    SELECT CONCAT(CAST(156 AS STRING), "|", Package_Kind) AS UUID
        , 156          AS joyple_game_code
        , CAST(Package_Kind AS STRING) AS package_kind
        , CAST(Package_Name AS STRING) AS package_name_KR
        , CAST(null AS STRING) AS package_name_JP
        , CURRENT_DATETIME("Asia/Seoul") AS create_datetime
    FROM `data-science-division-216308.PackageInfo.PackageInfo_JTWN`
    WHERE Package_Kind IS NOT NULL 
    AND Package_Name IS NOT NULL
    
    UNION ALL
    
    SELECT CONCAT(CAST(129 AS STRING), "|", Package_Kind) AS UUID
        , 129          AS joyple_game_code
        , CAST(Package_Kind AS STRING) AS package_kind
        , CAST(Package_Name AS STRING) AS package_name_KR
        , CAST(null AS STRING) AS package_name_JP
        , CURRENT_DATETIME("Asia/Seoul") AS create_datetime
    FROM `data-science-division-216308.PackageInfo.GNSS_PackageInfo`
    WHERE Package_Kind IS NOT NULL 
    AND Package_Name IS NOT NULL
    """

    # 1. 쿼리 실행
    truncate_query_job = client.query(truncate_query)
    truncate_query_job.result()  # 작업 완료 대기
    query_job = client.query(query)

    try:
        # 2. 작업 완료 대기 (여기서 쿼리가 끝날 때까지 블로킹됨)
        # 쿼리에 에러가 있다면 이 라인에서 예외(Exception)가 발생합니다.
        query_job.result()

        # 3. 성공 시 출력
        print(f"✅ 쿼리 실행 성공! (Job ID: {query_job.job_id})")

    except Exception as e:
        # 4. 실패 시 출력
        print(f"❌ 쿼리 실행 중 에러 발생: {e}")
        # Airflow에서 Task를 '실패(Failed)'로 처리하려면 에러를 다시 던져줘야 합니다.
        raise e
    
    print("✅ dim_package_kind ETL 완료")



def etl_dim_pg_id(**context):

    # 클라이언트 호출
    client = init_clients()["bq_client"]

    logger = logging.getLogger(__name__)
    
    # [수정 1] 함수 내부에서 사용할 타임존 정의
    kst = pytz.timezone('Asia/Seoul')

    # context에서 날짜 계산 함수 호출
    target_date, _ = calc_target_date(context['logical_date'])

    #################### 백필용 데이터 처리
    # target_date = target_date_range("2026-01-24", "2026-01-26")  ## 백필용
    # run_kst = None

    for td_str in target_date:
        # [수정 1] 문자열(String)을 datetime 객체로 변환
        # 넘어오는 날짜 형식이 'YYYY-MM-DD'라고 가정합니다.
        try:
            current_date_obj = datetime.strptime(td_str, "%Y-%m-%d")
        except ValueError:
            # 형식이 다를 경우에 대한 예외처리 (예: 시간까지 포함된 경우 등)
            # 필요에 따라 포맷을 수정하세요 ("%Y-%m-%d %H:%M:%S")
            print(f"⚠️ 날짜 형식이 잘못되었습니다: {td_str}")
            continue

        # [수정 2] pytz 라이브러리 사용 시 .replace(tzinfo=...) 보다는 .localize() 권장
        # .replace는 썸머타임이나 역사적 시간대 변경을 제대로 처리 못할 수 있음
        
        # KST 00:00:00 설정 (localize 사용)
        start_kst = kst.localize(current_date_obj)
        
        # KST -> UTC 변환
        start_utc = start_kst.astimezone(pytz.UTC)
        
        # 종료 시간 계산 (하루 뒤)
        end_kst = start_kst + timedelta(days=1)
        end_utc = end_kst.astimezone(pytz.UTC)

        print(f"📝 대상날짜: {td_str}")
        print(f"   ㄴ 시작시간(UTC): {start_utc}")
        print(f"   ㄴ 종료시간(UTC): {end_utc}")

        query = f"""
        MERGE `datahub-478802.datahub.dim_pg_id` AS target
        USING
        (
            SELECT DISTINCT pg_id
            FROM  `dataplatform-204306.CommonLog.Payment`
            WHERE log_time >= TIMESTAMP('{start_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
            AND log_time < TIMESTAMP('{end_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
        ) AS source ON target.pg_id = source.pg_id
        WHEN NOT MATCHED BY target THEN
        INSERT(pg_id, pg_name_KR, pg_name_EN, create_timestamp)
        VALUES(
            source.pg_id
            , NULL
            , NULL
            , CURRENT_TIMESTAMP()
        )
        WHEN NOT MATCHED BY source AND target.pg_id >= 0 THEN
        DELETE
        """

        # 1. 쿼리 실행
        query_job = client.query(query)

        try:
            # 2. 작업 완료 대기 (여기서 쿼리가 끝날 때까지 블로킹됨)
            # 쿼리에 에러가 있다면 이 라인에서 예외(Exception)가 발생합니다.
            query_job.result()

            # 3. 성공 시 출력
            print(f"✅ 쿼리 실행 성공! (Job ID: {query_job.job_id})")
            print(f"■ {td_str} dim_pg_id Batch 완료")

        except Exception as e:
            # 4. 실패 시 출력
            print(f"❌ 쿼리 실행 중 에러 발생: {e}")
            # Airflow에서 Task를 '실패(Failed)'로 처리하려면 에러를 다시 던져줘야 합니다.
            raise e

    print("✅ dim_pg_id ETL 완료")


def etl_dim_IAA_app_name(**context):

    # 클라이언트 호출
    client = init_clients()["bq_client"]

    logger = logging.getLogger(__name__)
    
    # [수정 1] 함수 내부에서 사용할 타임존 정의
    kst = pytz.timezone('Asia/Seoul')

    # context에서 날짜 계산 함수 호출
    target_date, _ = calc_target_date(context['logical_date'])

    # #################### 백필용 데이터 처리
#     target_date = target_date_range("2026-01-24", "2026-01-26")  ## 백필용
#     run_kst = None

    for td in target_date:
        target_date = td

        query=f"""
        MERGE `datahub-478802.datahub.dim_IAA_app_name` AS target
        USING
        (
        SELECT DISTINCT CASE WHEN APP.value IN ('ca-app-pub-9222823336006969~4823674397','ca-app-pub-9222823336006969~8860386047') THEN 'Heroball Z(Mojito)' 
            WHEN APP.displayLabel IN ('HeroBall Z', 'Heroball Z') THEN 'HeroBall Z'
            ELSE APP.displayLabel 
            END AS app_name 
        FROM `dataplatform-bdts.ads_admob.mediation_ads` 
        WHERE date > DATE_SUB(DATE('{target_date}'), INTERVAL 7 DAY)
        AND APP.displayLabel != 'BLESS MOBILE'
        AND AD_UNIT.displayLabel NOT IN ('DRB_MAX_AOS_RB', 'DRB_MAX_AOS_f50', 'DBR_MAX_AOS_f50') 

        UNION ALL
        
        SELECT DISTINCT app AS app_name
        FROM `dataplatform-bdts.ads_adx.adx_ads` 
        WHERE date > DATE_SUB(DATE('{target_date}'), INTERVAL 7 DAY)
        
        UNION ALL

        SELECT DISTINCT max_ad_unit AS app_name
        FROM `dataplatform-bdts.ads_applovin.max_revenue_responses`
        WHERE day > DATE_SUB(DATE('{target_date}'), INTERVAL 7 DAY)
        ) AS source ON target.app_name = source.app_name 
        WHEN NOT MATCHED BY target THEN
        INSERT(joyple_game_code, app_name, is_use_yn)
        VALUES(
            null
            , source.app_name
            , 'N'
        )
        ;
        """

        # 1. 쿼리 실행
        query_job = client.query(query)

        try:
            # 2. 작업 완료 대기 (여기서 쿼리가 끝날 때까지 블로킹됨)
            # 쿼리에 에러가 있다면 이 라인에서 예외(Exception)가 발생합니다.
            query_job.result()

            # 3. 성공 시 출력
            print(f"✅ 쿼리 실행 성공! (Job ID: {query_job.job_id})")
            print(f"■ {target_date} dim_IAA_app_name Batch 완료")

        except Exception as e:
            # 4. 실패 시 출력
            print(f"❌ 쿼리 실행 중 에러 발생: {e}")
            # Airflow에서 Task를 '실패(Failed)'로 처리하려면 에러를 다시 던져줘야 합니다.
            raise e

    print("✅ dim_IAA_app_name ETL 완료")

############ Platform Device는 별도 ETL 작업 없음
############ T_0265_0000_CostCampaignRulePreBook_V 는 필요 시 직접 입력하는 형태 (DB insert 처리)
############ special_pg 는 별도 ETL 작업 없음


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
    dag_id='ETL_dimension',
    default_args=default_args,
    description='전체 dimension table에 대해서 OLAP 처리 (KST D-1 기준)',
    schedule= '01 0 * * *',  ## KST 09:10 AM 매일 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['ETL', 'dim', 'bigquery'],
) as dag:

    etl_dim_os_task = PythonOperator(
        task_id='etl_dim_os',
        python_callable=etl_dim_os,
    )

    etl_dim_AFC_campaign_task = PythonOperator(
        task_id='etl_dim_AFC_campaign',
        python_callable=etl_dim_AFC_campaign,
    )

    etl_dim_auth_method_id_task = PythonOperator(
        task_id='etl_dim_auth_method_id',
        python_callable=etl_dim_auth_method_id,
    )

    etl_dim_exchange_rate_task = PythonOperator(
        task_id='etl_dim_exchange_rate',
        python_callable=etl_dim_exchange_rate,
    )

    etl_dim_game_id_task = PythonOperator(
        task_id='etl_dim_game_id',
        python_callable=etl_dim_game_id,
    )

    etl_dim_app_id_task = PythonOperator(
        task_id='etl_dim_app_id',
        python_callable=etl_dim_app_id,
    )

    etl_dim_google_campaign_task = PythonOperator(
        task_id='etl_dim_google_campaign',
        python_callable=etl_dim_google_campaign,
    )

    etl_dim_ip_range_task = PythonOperator(
        task_id='etl_dim_ip_range',
        python_callable=etl_dim_ip_range,
    )

    etl_dim_ip_proxy_task = PythonOperator(
        task_id='etl_dim_ip_proxy',
        python_callable=etl_dim_ip_proxy,
    )
    
    etl_dim_ip4_country_code_task = PythonOperator(
        task_id='etl_dim_ip4_country_code',
        python_callable=etl_dim_ip4_country_code,
    )

    etl_dim_joyple_game_code_task = PythonOperator(
        task_id='etl_dim_joyple_game_code',
        python_callable=etl_dim_joyple_game_code,
    )

    etl_dim_market_id_task = PythonOperator(
        task_id='etl_dim_market_id',
        python_callable=etl_dim_market_id,
    )

    etl_dim_package_kind_task = PythonOperator(
        task_id='etl_dim_package_kind',
        python_callable=etl_dim_package_kind,
    )

    etl_dim_pg_id_task = PythonOperator(
        task_id='etl_dim_pg_id',
        python_callable=etl_dim_pg_id,
    )

    etl_dim_IAA_app_name_task = PythonOperator(
        task_id='etl_dim_IAA_app_name',
        python_callable=etl_dim_IAA_app_name,
    )

    etl_dim_product_code_task = PythonOperator(
        task_id='etl_dim_product_code',
        python_callable=etl_dim_product_code,
    )

    adjust_dim_product_code_task = PythonOperator(
        task_id='adjust_dim_product_code',
        python_callable=adjust_dim_product_code,
    )

    bash_task = BashOperator(
        task_id = 'bash_task',
        outlets = [ETL_dimension],
        bash_command = 'echo "ETL_dimension 수행 완료"'
    )

chain(
    etl_dim_os_task,
    etl_dim_AFC_campaign_task,
    etl_dim_auth_method_id_task,
    etl_dim_exchange_rate_task,
    etl_dim_game_id_task,
    etl_dim_app_id_task,
    etl_dim_google_campaign_task,
    etl_dim_ip_range_task,
    etl_dim_ip_proxy_task,
    etl_dim_ip4_country_code_task,
    etl_dim_joyple_game_code_task,
    etl_dim_market_id_task,
    etl_dim_package_kind_task,
    etl_dim_pg_id_task,
    etl_dim_IAA_app_name_task,
    etl_dim_product_code_task,
    adjust_dim_product_code_task,
    bash_task
)