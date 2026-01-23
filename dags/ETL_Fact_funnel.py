from google.cloud import bigquery
from datetime import datetime, timezone, timedelta
import time
import os
import pytz


def etl_f_funnel_access_first(target_date:list, client):   ### Device_id 기준 최초 funnel 데이터는 현재사용하고 있는 case 없음. -> DE팀 install 메일링 데이터에 활용.

    kst = pytz.timezone('Asia/Seoul')

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

        ### 해당 쿼리에 대해서는 확실히 확인이 필요함
        query = f"""
        MERGE `datahub-478802`.datahub.f_funnel_access_first AS target
        USING
        (
            SELECT Info.game_id
                , joyple_game_code
                , device_id
                , tracker_account_id
                , IFNULL(Info.tracker_type_id, 1)  AS tracker_type_id
                , b.ip
                , b.country_code
                , Info.market_id
                , Info.os_id
                , Info.platform_device_type
                , DATE(Info.log_time, "Asia/Seoul") AS datekey
                , Info.log_time AS access_timestamp
            FROM (
                SELECT a.joyple_game_code
                    , a.device_id
                    , ARRAY_AGG(STRUCT(a.game_id, a.market_id, a.os_id, a.ip, a.platform_device_type, a.tracker_type_id, a.log_time) ORDER BY a.log_time ASC LIMIT 1)[OFFSET(0)] AS Info
                    , ARRAY_AGG(a.tracker_account_id IGNORE NULLS ORDER BY a.log_time ASC LIMIT 1)[OFFSET(0)] AS tracker_account_id
                FROM (
                    SELECT a.game_id
                        , a.joyple_game_code
                        , a.device_id
                        , a.tracker_account_id
                        , a.mmp_type AS tracker_type_id
                        , a.market_id
                        , a.os_id
                        , a.ip
                        , a.platform_device_type
                        , a.log_time
                    FROM `dataplatform-204306.CommonLog.Funnel` AS a
                    WHERE a.log_time >= '{start_utc}'
                    AND a.log_time < '{end_utc}'
                    AND a.game_id            IS NOT NULL
                    AND a.device_id          IS NOT NULL
                    AND a.market_id          IS NOT NULL
                    AND a.os_id              IS NOT NULL
                    AND a.ip                 IS NOT NULL
                    AND a.step_id            IS NOT NULL
                    AND a.step_name          IS NOT NULL
                    AND a.log_time           IS NOT NULL
                ) AS a
                GROUP BY a.joyple_game_code, a.device_id
            ) AS a
            LEFT OUTER JOIN `datahub-478802`.datahub.dim_ip4_country_code AS b
            ON (a.Info.ip = b.ip)
        ) AS source ON target.joyple_game_code = source.joyple_game_code AND target.device_id = source.device_id
        WHEN MATCHED AND (target.access_timestamp >= source.access_timestamp) THEN
        UPDATE SET target.country_code        =  source.country_code
                , target.ip                 =  source.ip
                , target.market_id           =  source.market_id
                , target.os_id               =  source.os_id
                , target.platform_device_type =  source.platform_device_type
                , target.access_timestamp    =  source.access_timestamp
                , target.tracker_account_id   =  IF(target.tracker_account_id IS NULL AND source.tracker_account_id IS NOT NULL, source.tracker_account_id, target.tracker_account_id)
                , target.tracker_type_id      =  IF(target.tracker_type_id IS NULL AND source.tracker_type_id IS NOT NULL, source.tracker_type_id, target.tracker_type_id)
        WHEN NOT MATCHED BY target THEN
        INSERT (game_id, joyple_game_code, device_id, tracker_account_id, tracker_type_id, ip, country_code, market_id, os_id, platform_device_type, datekey, access_timestamp)
        VALUES (
            source.game_id
            , source.joyple_game_code
            , source.device_id
            , source.tracker_account_id
            , source.tracker_type_id
            , source.ip
            , source.country_code
            , source.market_id
            , source.os_id
            , source.platform_device_type
            , DATE(source.access_timestamp, "Asia/Seoul")
            , source.access_timestamp
        );
        """

        # 1. 쿼리 실행
        query_job = client.query(query)

        try:
            # 2. 작업 완료 대기 (여기서 쿼리가 끝날 때까지 블로킹됨)
            # 쿼리에 에러가 있다면 이 라인에서 예외(Exception)가 발생합니다.
            query_job.result()

            # 3. 성공 시 출력
            print(f"✅ 쿼리 실행 성공! (Job ID: {query_job.job_id})")
            print(f"■ {td_str} f_funnel_access_first Batch 완료")

        except Exception as e:
            # 4. 실패 시 출력
            print(f"❌ 쿼리 실행 중 에러 발생: {e}")
            # Airflow에서 Task를 '실패(Failed)'로 처리하려면 에러를 다시 던져줘야 합니다.
            raise e
    
    print("✅ f_funnel_access_first ETL 완료")
    return True



def etl_f_funnel_access(target_date:list, client):

    kst = pytz.timezone('Asia/Seoul')

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
            MERGE `datahub-478802.datahub.f_funnel_access` AS target
            USING
            (
              SELECT a.game_id
                  , a.joyple_game_code
                  , a.tracker_account_id
                  , a.tracker_type_id
                  , a.device_id
                  , a.step_id
                  , a.step_name
                  , b.install_datekey 
                  , b.install_time 
                  , b.app_id
                  , b.campaign
                  , b.init_campaign
                  , b.media_source
                  , b.is_organic
                  , b.country_code as install_country_code
                  , b.is_retargeting
                  , timestamp_add(a.log_time, interval 9 hour) AS log_datetime
              FROM (
                    SELECT joyple_game_code
                        , game_id
                        , tracker_account_id
                        , IFNULL(mmp_type, 1) AS tracker_type_id
                        , ARRAY_AGG(device_id IGNORE NULLS ORDER BY log_time ASC LIMIT 1)[OFFSET(0)] AS device_id
                        , step_id
                        , step_name
                        , MIN(log_time) AS log_time
                    FROM `dataplatform-204306.CommonLog.Funnel`
                    WHERE log_time >= '{start_utc}'
                    AND log_time < '{end_utc}'
                    AND joyple_game_code   IS NOT NULL
                    AND tracker_account_id IS NOT NULL
                    AND mmp_type           IS NOT NULL
                    AND device_id          IS NOT NULL
                    AND step_id            IS NOT NULL
                    AND step_name          IS NOT NULL
                    GROUP BY joyple_game_code, game_id, tracker_account_id, mmp_type, step_id, step_name
                   )  AS a
            LEFT OUTER JOIN `datahub-478802.datahub.f_tracker_install` as b
            on a.tracker_account_id = b.tracker_account_id AND a.tracker_type_id = b.tracker_type_id
            )  AS source 
            ON target.game_id = source.game_id
            AND target.joyple_game_code = source.joyple_game_code 
            AND target.tracker_account_id = source.tracker_account_id 
            AND target.tracker_type_id = source.tracker_type_id 
            AND target.device_id = source.device_id
            AND target.step_id = source.step_id 
            AND target.step_name = source.step_name
            WHEN MATCHED AND target.install_datekey > CAST(source.install_datekey AS DATE) - 3 THEN
            UPDATE SET 
                    target.app_id = source.app_id
                    , target.campaign = source.campaign
                    , target.init_campaign = source.init_campaign
                    , target.media_source = source.media_source
                    , target.is_organic = source.is_organic
                    , target.install_country_code = source.install_country_code
                    , target.log_datetime = source.log_datetime
            WHEN NOT MATCHED BY target THEN
            INSERT (
                game_id
                , joyple_game_code
                , tracker_account_id
                , tracker_type_id
                , install_datekey
                , is_retargeting
                , device_id
                , step_id
                , step_name
                , app_id
                , campaign
                , init_campaign
                , media_source
                , is_organic
                , install_country_code
                , log_datetime
                )
            VALUES (
                source.game_id
                , source.joyple_game_code
                , source.tracker_account_id
                , source.tracker_type_id
                , source.install_datekey
                , source.is_retargeting
                , source.device_id
                , source.step_id
                , source.step_name
                , source.app_id
                , source.campaign
                , source.init_campaign
                , source.media_source
                , source.is_organic
                , source.install_country_code
                , source.log_datetime
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
            print(f"■ {td_str} f_funnel_access Batch 완료")

        except Exception as e:
            # 4. 실패 시 출력
            print(f"❌ 쿼리 실행 중 에러 발생: {e}")
            # Airflow에서 Task를 '실패(Failed)'로 처리하려면 에러를 다시 던져줘야 합니다.
            raise e
    
    print("✅ f_funnel_access ETL 완료")
    return True  