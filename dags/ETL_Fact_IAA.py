from google.cloud import bigquery
from datetime import datetime, timezone, timedelta
import time
import os
import pytz


def etl_f_IAA_game_sub_user_watch(target_date: list, client):

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

        query=f"""
        MERGE `datahub-478802.datahub.f_IAA_game_sub_user_watch` AS target
        USING
        (
            -- POTC
            SELECT 131                             AS joyple_game_code
                , DATE(LogDate_Svr, 'Asia/Seoul') AS watch_datekey
                , CAST(userid AS string)          AS game_sub_user_name
                , COUNT(userid)                   AS watch_cnt
            FROM `dataplatform-204306.POTC.COMMAND_GOLD_GET`
            WHERE WorldID <= 900
            AND Reason = 511
            AND LogDate_Svr >= '{start_utc}'
            AND LogDate_Svr < '{end_utc}'
            AND p0 != 0
            AND ((p5 = 0 and p6 = 2) OR (p5 = 0 and p6 = 0 and logdate_svr <= '2022-03-23 05:01:34'))
            GROUP BY joyple_game_code, watch_datekey, game_sub_user_name
            
            UNION ALL -- GW 
            
            SELECT 133                        AS joyple_game_code
                , DATE(logdate,'Asia/Seoul') AS watch_datekey
                , CAST(userid AS STRING)     AS game_sub_user_name
                , COUNT(userid)              AS watch_cnt
            FROM `dataplatform-204306.GW.T_DM_InappADS`
            WHERE reason = 4906 -- 광고시청 완료
            AND worldid >=1000
            AND p0 in (0,1,3,4,5,6,7,8,9,10,11,12,13) -- 정의서 상 2번은 골드로 되어있으나, 1번이 골드이며, 2번은 실제 남지 않는 로그 
            AND logdate >= '{start_utc}'
            AND logdate < '{end_utc}'
            GROUP BY joyple_game_code, watch_datekey, game_sub_user_name 
            
            UNION ALL -- WWM
            
            SELECT 30001                          AS joyple_game_code
                , WatchDateKST                   AS watch_datekey
                , CAST(UserID AS STRING)         AS game_sub_user_name
                , SUM(watch_cnt1)                AS watch_cnt
            FROM(
            SELECT DATE(LogDate_Svr,'Asia/Seoul') AS WatchDateKST
                , UserID
                , C1
                , MAX(CASE WHEN C1 IN ('D_kill', 'D_Star') THEN 1 ELSE p4 END) AS watch_cnt1  --퍼킬의 경우, p4에 퍼킬몬스터 남음 
            FROM `dataplatform-204306.WWM.T_Log_Game` 
            WHERE Reason = 3901 
            AND LogDate_Svr >= '{start_utc}'
            AND LogDate_Svr < '{end_utc}'
            GROUP BY WatchDateKST, UserID, C1 
            )
            GROUP BY joyple_game_code, watch_datekey, game_sub_user_name
            
            UNION ALL -- GBCC
            
            SELECT 153                            AS joyple_game_code
                , DATE(LogDate_Svr,'Asia/Seoul') AS watch_datekey
                , CAST(userid AS STRING)         AS game_sub_user_name
                , COUNT(userid)                  AS watch_cnt
            FROM `dataplatform-204306.GBCC.dblog`
            WHERE reason  = 4906 -- 광고시청 완료
            AND command = 49  -- 인앱광고
            AND worldid >= 1000 
            AND LogDate_Svr >= '{start_utc}'
            AND LogDate_Svr < '{end_utc}'
            GROUP BY joyple_game_code, watch_datekey, game_sub_user_name
            
            UNION ALL -- DS
            
            SELECT 30003                          AS joyple_game_code
                , DATE(LogDate_Svr,'Asia/Seoul') AS watch_datekey
                , CAST(UserID AS STRING)         AS game_sub_user_name
                , COUNT(p3)                      AS watch_cnt
            FROM `dataplatform-204306.DS.T_Log_Game`
            WHERE Reason = 63001 
            AND WorldID NOT IN (6108,5555)
            AND LogDate_Svr >= '{start_utc}'
            AND LogDate_Svr < '{end_utc}'
            GROUP BY joyple_game_code, watch_datekey, game_sub_user_name
            
            UNION ALL -- KOF
            
            SELECT 148                            AS joyple_game_code
                , DATE(LogDate_Svr,'Asia/Seoul') AS watch_datekey
                , CAST(UserID AS STRING)         AS game_sub_user_name
                , COUNT(UserID)                  AS watch_cnt
            FROM `dataplatform-204306.KOFS.T_Log_Game` 
            WHERE Reason = 63001 
            AND LogDate_Svr >= '{start_utc}'
            AND LogDate_Svr < '{end_utc}'
            GROUP BY joyple_game_code, watch_datekey, game_sub_user_name
            
            UNION ALL -- DRB
            
            SELECT 155                            AS joyple_game_code
                , DATE(LogDate_Svr,'Asia/Seoul') AS watch_datekey
                , CAST(UserID AS STRING)         AS game_sub_user_name
                , COUNT(UserID)                  AS watch_cnt
            FROM `dataplatform-204306.DRB.T_Log_Game` 
            WHERE Reason = 3901 
            AND LogDate_Svr >= '{start_utc}'
            AND LogDate_Svr < '{end_utc}'
            GROUP BY joyple_game_code, watch_datekey, game_sub_user_name

        ) AS source 
        ON target.joyple_game_code = source.joyple_game_code 
        AND target.game_sub_user_name = source.game_sub_user_name 
        AND target.watch_datekey = source.watch_datekey   
        
        WHEN MATCHED AND (target.watch_cnt <> source.watch_cnt) THEN
        UPDATE SET target.watch_cnt =  source.watch_cnt
        WHEN NOT MATCHED BY target THEN
        INSERT (joyple_game_code, watch_datekey, game_sub_user_name, watch_cnt)
        VALUES (
            source.joyple_game_code
            , source.watch_datekey
            , source.game_sub_user_name
            , source.watch_cnt
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
            print(f"■ {td_str} f_IAA_game_sub_user_watch Batch 완료")

        except Exception as e:
            # 4. 실패 시 출력
            print(f"❌ 쿼리 실행 중 에러 발생: {e}")
            # Airflow에서 Task를 '실패(Failed)'로 처리하려면 에러를 다시 던져줘야 합니다.
            raise e
    
    print("✅ f_IAA_game_sub_user_watch ETL 완료")
    return True


def etl_f_IAA_performance(client):
    ############### truncate insert 처리
    truncate_query=f"""
    TRUNCATE TABLE `datahub-478802.datahub.f_IAA_performance`
    """
 
    query=f""" 
    INSERT INTO `datahub-478802`.datahub.f_IAA_performance
    (app_name, mediation_name, ADNW_name, watch_datekey, country_code, platform, ad_unit, ad_format, requests, impressions, matched_requests, clicks, revenue)

    SELECT TRIM(LOWER(AppName))                                                                   AS app_name
        , MediationName                                                                          AS mediation_name
        , ADNWName                                                                               AS ADNW_name
        , Date                                                                                   AS watch_datekey                         
        , UPPER(CountryCode)                                                                     AS country_code
        , CASE WHEN Platform IN ('iOS','ios','IOS','Ios','iphone','iPhone') THEN 'iOS' 
                WHEN Platform IN ('Android','android','aos','AOS')           THEN 'Android'
                ELSE CONCAT('Unknown:',Platform) 
        END                                                                                    AS platform
        , ADUnit                                                                                 AS ad_unit
        , ADFormat                                                                               AS ad_format
        , Requests                                                                               AS requests
        , Impressions                                                                            AS impressions
        , MatchedRequests                                                                        AS matched_requests
        , Clicks                                                                                 AS clicks
        , Revenue                                                                                AS revenue 
    FROM 
    (
    SELECT 
            CASE WHEN APP.value IN ('ca-app-pub-9222823336006969~4823674397','ca-app-pub-9222823336006969~8860386047') THEN 'Heroball Z(Mojito)' 
                WHEN APP.displayLabel IN ('HeroBall Z', 'Heroball Z')                                                 THEN 'HeroBall Z'
                ELSE APP.displayLabel 
            END                            AS AppName   
        , 'AdMob'                        AS MediationName
        , AD_SOURCE.displayLabel         AS ADNWName
        , date                           AS Date
        , COUNTRY                        AS CountryCode
        , PLATFORM                       AS Platform
        , AD_UNIT.displayLabel           AS ADUnit
        , FORMAT                         AS ADFormat
        , AD_REQUESTS                    AS Requests
        , IMPRESSIONS                    AS Impressions
        , MATCHED_REQUESTS               AS MatchedRequests
        , CLICKS                         AS Clicks
        , ESTIMATED_EARNINGS             AS Revenue
    FROM `dataplatform-bdts.ads_admob.mediation_ads`  
    WHERE APP.displayLabel != 'BLESS MOBILE'
        AND AD_UNIT.displayLabel NOT IN ('DRB_MAX_AOS_RB', 'DRB_MAX_AOS_f50', 'DBR_MAX_AOS_f50') 
    UNION ALL
    SELECT 
            app                             AS AppName
        , 'ADx'                           AS MediationName
        , CASE WHEN adnetwork = 'admob'      THEN 'AdMob Network'
                WHEN adnetwork = 'vungle'     THEN 'Vungle'
                WHEN adnetwork = 'unityads'   THEN 'Unity Ads'
                WHEN adnetwork = 'facebook'   THEN 'Facebook Audience Network'
                WHEN adnetwork = 'ironsource' THEN 'Ironsource'
                ELSE adnetwork
            END                             AS ADNWName
        , DATE_ADD(date, INTERVAL +1 day) AS Date
        , country                         AS CountryCode
        , ostype                          AS Platform
        , adunitname                      AS ADUnit
        , adtype                          AS ADFormat
        , attempt                         AS Requests
        , impression                      AS Impressions
        , fill                            AS MatchedRequests
        , click                           AS Clicks
        , revenue                         AS Revenue
    FROM `dataplatform-bdts.ads_adx.adx_ads` 
    UNION ALL ### MAX 미디에이션 처리 부분
    SELECT
        max_ad_unit                                   AS AppName
        , 'MAX'                                          AS MediationName
        , CASE WHEN network = 'ADMOB_NETWORK'     THEN 'AdMob Network'
            WHEN network = 'ADMOB_BIDDING'     THEN 'AdMob Network(입찰)'
            WHEN network = 'APPLOVIN_EXCHANGE' THEN 'Applovin(입찰)'
            WHEN network = 'APPLOVIN_NETWORK'  THEN 'Applovin'
            WHEN network = 'FACEBOOK_NETWORK'  THEN 'Meta Audience Network'
            WHEN network = 'FACEBOOK_BIDDING'  THEN 'Meta Audience Network(입찰)'
            WHEN network = 'FYBER_NETWORK'     THEN 'Fyber'
            WHEN network = 'MINTEGRAL_BIDDING' THEN 'Mintegral(입찰)'
            WHEN network = 'FYBER_NETWORK'     THEN 'Fyber'
            WHEN network = 'VUNGLE_BIDDING'    THEN 'Liftoff Monetize(입찰)'
            WHEN network = 'VUNGLE_NETWORK'    THEN 'Liftoff Monetize'
            WHEN network = 'UNITY_NETWORK'     THEN 'Unity Ads'
            ELSE network
        END                                           AS ADNWName
        , day                                           AS Date
        , country                                       AS CountryCode
        , CASE WHEN platform = 'android' THEN 'Android' 
            WHEN platform = 'ios'     THEN 'iOS' END AS Platform
        , max_ad_unit                                   AS ADUnit
        , ad_format                                     AS ADFormat
        , attempts                                      AS Requests
        , impressions                                   AS Impressions
        , responses                                     AS MatchedRequests
        , 0                                             AS Clicks
        , estimated_revenue                             AS Revenue
    FROM `dataplatform-bdts.ads_applovin.max_revenue_responses`
    ) AS a
    WHERE AppName NOT LIKE 'Joyple Sample Test%'
    
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

    print("✅ f_IAA_performance ETL 완료")




def etl_f_IAA_auth_account_performance_joyple(target_date:list, client):

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


        delete_query = f"""
        DELETE FROM `datahub-478802`.datahub.f_IAA_auth_account_performance_joyple
        WHERE watch_datekey >= DATE('{start_utc}', "Asia/Seoul")
        AND watch_datekey <  DATE('{end_utc}', "Asia/Seoul")
        """

        query = f"""
        INSERT INTO `datahub-478802`.datahub.f_IAA_auth_account_performance_joyple
        (
            joyple_game_code, 
            watch_datekey, 
            watch_datetime, 
            mediation_name, 
            auth_account_name, 
            auth_method_id, 
            game_sub_user_name, 
            reg_datekey, 
            reg_country_code,
            os_iaa, 
            log_id, 
            ad_network_code, 
            ad_code, 
            ad_name, 
            placement, 
            media_source, 
            campaign, 
            init_campaign, 
            reg_datediff, 
            revenue_per_user_USD, 
            revenue_per_user_KRW
            )

            WITH Iaa_Data
            AS (
            SELECT log_id                                                          as LogID
                , CurrencyCode                                                    as CurrencyCode
                , Info.joyple_game_code                                           as JoypleGameID
                , Info.mediation_name                                             as MediationName
                , Info.userkey                                                    as AuthAccountName
                , b.auth_method_id                                                  as AuthMethodID
                , Info.char_id                                                    as GameSubUserName
                , Info.os_iaa                                                     as OsIAA
                , Info.ad_network_code                                            as AdNetworkCode
                , Info.ad_code                                                    as AdCode
                , Info.ad_name                                                    as AdName
                , Info.placement                                                  as Placement
                , Info.iaa_rev_usd                                                as RevenuePerUserUSD
                , Info.logtime_kst                                                as LogTimeKST
                , Info.logdate_kst                                                as LogDateKST
                , Info.logtime_utc                                                as LogTimeUTC
                , Info.logdate_utc                                                as LogDateUTC
                , b.reg_datekey                                         as AuthAccountRegisterDateKST
                , b.reg_country_code                                                   as CountryCode
                , b.tracker_account_id
                , b.tracker_type_id
                , DATE_DIFF(DATE(Info.logdate_kst), b.reg_datekey, DAY) as DaysFromRegisterDate
            FROM (
                SELECT log_id 
                    , 'USD' AS CurrencyCode
                    , ARRAY_AGG(
                        STRUCT(
                            a.joyple_game_code
                            , a.mediation_name  
                            , a.userkey
                            , a.char_id
                            , a.os_iaa
                            , a.ad_network_code
                            , a.ad_code
                            , a.ad_name
                            , a.placement
                            , a.iaa_rev_usd
                            , a.logtime_kst
                            , a.logdate_kst
                            , a.logtime_utc
                            , a.logdate_utc  
                        ) ORDER BY a.logtime_utc ASC LIMIT 1)[OFFSET(0)] AS Info
                FROM (
                    -- Admob 미디에이션에서 송출한 광고
                    SELECT DISTINCT 
                        game_code                                   as joyple_game_code
                        , "Admob"                                     as mediation_name  
                        , userkey                                     as userkey
                        , game_char_id                                as char_id
                        , os                                          as os_iaa
                        , log_id                                      as log_id
                        , ad_network_code                             as ad_network_code
                        , ad_code                                     as ad_code
                        , ad_name                                     as ad_name
                        , placement                                   as placement
                        , value_micros/1000000                        as iaa_rev_usd
                        , DATETIME(event_time, "Asia/Seoul")          as logtime_kst
                        , DATE(event_time, "Asia/Seoul")              as logdate_kst 
                        , event_time                                  as logtime_utc
                        , date(event_time)                            as logdate_utc
                    FROM `dataplatform-204306.JoypleLog.iaa_admob_log` 
                    WHERE event_time >= '{start_utc}'
                    AND event_time < '{end_utc}'
                    AND event_name = 'COMPLETED' -- 광고 시청 완료 Case만   
                    UNION ALL
                    -- Max 미디에이션에서 송출한 광고
                    SELECT DISTINCT 
                        game_code                                   as joyple_game_code
                        , "Max"                                       as mediation_name  
                        , userkey                                     as userkey
                        , game_char_id                                as char_id
                        , os                                          as os_iaa
                        , log_id                                      as log_id
                        , ad_network_code                             as ad_network_code
                        , ad_code                                     as ad_code
                        , ad_name                                     as ad_name
                        , placement                                   as placement
                        , value_micros                                as iaa_rev_usd
                        , DATETIME(event_time, "Asia/Seoul")          as logtime_kst
                        , DATE(event_time, "Asia/Seoul")              as logdate_kst 
                        , event_time                                  as logtime_utc
                        , date(event_time)                            as logdate_utc
                    FROM `dataplatform-204306.JoypleLog.iaa_max_log`
                    WHERE event_time >= '{start_utc}'
                    AND event_time < '{end_utc}'
                    AND event_name = 'COMPLETED' -- 광고 시청 완료 Case만   
                    UNION ALL
                    -- Inhouse 송출 광고 (매출 X)
                    SELECT DISTINCT 
                        game_code                                   as joyple_game_code
                        , "Inhouse"                                   as mediation_name  
                        , userkey                                     as userkey
                        , game_char_id                                as char_id
                        , os                                          as os_iaa
                        , log_id                                      as log_id
                        , ad_network_code                             as ad_network_code
                        , ad_code                                     as ad_code
                        , ad_name                                     as ad_name
                        , placement                                   as placement
                        , NULL                                        as iaa_rev_usd
                        , DATETIME(event_time, "Asia/Seoul")          as logtime_kst
                        , DATE(event_time, "Asia/Seoul")              as logdate_kst 
                        , event_time                                  as logtime_utc
                        , date(event_time)                            as logdate_utc
                    FROM `dataplatform-204306.JoypleLog.iaa_inhouse_log`
                    WHERE event_time >= '{start_utc}'
                    AND event_time < '{end_utc}'
                    AND event_name = 'COMPLETED' -- 광고 시청 완료 Case만       
                ) AS a     
                GROUP BY log_id
            ) AS a
            LEFT OUTER JOIN `datahub-478802`.datahub.f_common_register AS b
            ON (CAST(a.Info.joyple_game_code AS STRING) = CAST(b.joyple_game_code AS STRING) 
            AND CAST(a.Info.userkey AS STRING) = CAST(b.auth_account_name AS STRING))
            )
            
            SELECT a.JoypleGameID AS joyple_game_code
                , a.LogDateKST AS watch_datekey
                , a.LogTimeKST       AS watch_datetime
                , a.MediationName AS mediation_name
                , a.AuthAccountName AS auth_account_name
                , a.AuthMethodID AS auth_method_id
                , a.GameSubUserName AS game_sub_user_name
                , a.AuthAccountRegisterDateKST AS reg_datekey
                , a.CountryCode AS reg_country_code
                , CASE WHEN LOWER(a.OsIAA) = 'android'         THEN "android"
                        WHEN LOWER(a.OsIAA) in ('ios','iphone') THEN "ios"
                        ELSE LOWER(a.OsIAA)
                END AS os_iaa
                , a.LogID AS log_id
                , a.AdNetworkCode AS ad_network_code
                , a.AdCode AS ad_code 
                , a.AdName As AD_name
                , a.Placement AS placement
                , d.media_source 
                , d.campaign 
                , d.init_campaign 
                , a.DaysFromRegisterDate AS reg_datediff
                , IFNULL(a.RevenuePerUserUSD, 0) AS revenue_per_user_USD
                , IFNULL(a.RevenuePerUserUSD * exchange_rate,0) AS revenue_per_user_KRW
            FROM Iaa_Data AS a
            INNER JOIN `datahub-478802`.datahub.dim_exchange AS c 
            ON (a.CurrencyCode = c.currency AND a.LogDateKST = c.datekey)
            LEFT OUTER JOIN `datahub-478802`.datahub.f_tracker_install AS d
            ON (a.tracker_account_id = d.tracker_account_id AND a.tracker_type_id = d.tracker_type_id)
        """

        # 1. 쿼리 실행
        truncate_query_job = client.query(delete_query)
        truncate_query_job.result()  # 작업 완료 대기
        query_job = client.query(query)

        try:
            # 2. 작업 완료 대기 (여기서 쿼리가 끝날 때까지 블로킹됨)
            # 쿼리에 에러가 있다면 이 라인에서 예외(Exception)가 발생합니다.
            query_job.result()

            # 3. 성공 시 출력
            print(f"✅ 쿼리 실행 성공! (Job ID: {query_job.job_id})")
            print(f"■ {td_str} f_IAA_auth_account_performance_joyple Batch 완료")

        except Exception as e:
            # 4. 실패 시 출력
            print(f"❌ 쿼리 실행 중 에러 발생: {e}")
            # Airflow에서 Task를 '실패(Failed)'로 처리하려면 에러를 다시 던져줘야 합니다.
            raise e
    
    print("✅ f_IAA_auth_account_performance_joyple ETL 완료")
    return True



def etl_f_IAA_auth_account_performance(target_date:list, client):
    ################ 30일치 데이터 삭제 후 insert 처리

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

        delete_query=f"""
        DELETE FROM `datahub-478802.datahub.f_IAA_auth_account_performance`
        WHERE watch_datekey >= DATE('{start_utc}', "Asia/Seoul")
        AND watch_datekey < DATE('{end_utc}', "Asia/Seoul")
        """

        query=f"""            
        INSERT `datahub-478802`.datahub.f_IAA_auth_account_performance
        (
            joyple_game_code,
            watch_datekey,
            auth_account_name,  
            auth_method_id, 
            reg_datekey, 
            reg_country_code,
            os, 
            media_source, 
            campaign, 
            init_campaign, 
            reg_datediff, 
            watch_cnt, 
            revenue_per_user, 
            revenue_per_user_KRW, 
            from_source
        )

        WITH InAppADPerformance
            AS 
            (  
            SELECT b.joyple_game_code
                    , a.watch_datekey
                    , a.country_code
                    , CASE WHEN LOWER(a.Platform) = 'android'         THEN "android"
                        WHEN LOWER(a.Platform) in ('ios','iphone') THEN "ios"
                        ELSE LOWER(a.Platform)
                    END AS os
                    , SUM(Impressions) AS impressions
                    , SUM(Revenue)     AS revenue
            FROM `datahub-478802.datahub.f_IAA_performance` AS a
            INNER JOIN `datahub-478802.datahub.dim_IAA_app_name` AS b
            ON (a.app_name = b.app_name)  
            WHERE watch_datekey >= DATE('{start_utc}', "Asia/Seoul")
                AND watch_datekey <  DATE('{end_utc}', "Asia/Seoul")
                AND revenue != 0
            GROUP BY joyple_game_code, watch_datekey,  country_code, os
            ),
            GameSubUserInfo
            AS 
            (
            SELECT  a.watch_datekey
                    , a.joyple_game_code
                    , b.auth_account_name
                    , b.auth_method_id
                    , b.reg_datekey
                    , SUM(a.watch_cnt) AS watch_cnt
                    , b.country_code
                    , LOWER(c.os_name) AS os
                    , DATE_DIFF(a.watch_datekey, b.reg_datekey, Day) as reg_datediff
                    , d.media_source
                    , d.campaign
                    , d.init_campaign
            FROM `datahub-478802.datahub.f_IAA_game_sub_user_watch` AS a
            LEFT OUTER JOIN `datahub-478802.datahub.f_common_register_char` AS b
            ON (a.joyple_game_code = b.joyple_game_code AND a.game_sub_user_name = b.game_sub_user_name)
            LEFT OUTER JOIN `datahub-478802.datahub.dim_os_id` AS c
            on (b.os_id = c.os_id)
            LEFT OUTER JOIN `datahub-478802.datahub.f_tracker_install` AS d
            on (b.tracker_account_id = d.tracker_account_id AND CAST(b.tracker_type_id AS STRING) = CAST(d.tracker_type_id AS STRING))
            WHERE a.watch_datekey >= DATE('{start_utc}', "Asia/Seoul")
                AND a.watch_datekey <  DATE('{end_utc}', "Asia/Seoul")
            GROUP BY watch_datekey, joyple_game_code, auth_account_name, auth_method_id, reg_datekey, country_code, os, reg_datediff, media_source, campaign, init_campaign
            ),
            T_FULL_SUM # 3 > 일별X국가별XOS 별 매출, 시청 횟수 매칭 =>  GameSubUserInfo2 에서 rev_user, rev_user_krw 구하기 위해 필요
            AS
            (
            SELECT IFNULL(a.joyple_game_code, b.joyple_game_code ) AS joyple_game_code 
                , IFNULL(a.watch_datekey, b.watch_datekey) AS watch_datekey
                , IF((a.revenue IS NULL OR b.watch_cnt IS NULL), 'ETC', IFNULL(a.country_code, b.country_code)) AS country_code
                , IFNULL(a.os, b.os ) AS os
                , SUM(a.revenue) AS revenue
                , SUM(b.watch_cnt) AS watch_cnt
            FROM InAppADPerformance AS a
            FULL OUTER JOIN (
                SELECT u.watch_datekey, u.joyple_game_code, u.country_code, u.os, SUM(watch_cnt) AS watch_cnt -- 시청자수, 매출 누락방지를 위한 full join
                FROM GameSubUserInfo AS u
                WHERE OS in ('android','ios') -- 과거 PC에서는 인하우스 광고만 노출되어 PC 등의 기타 OS는 고려하지 않음
                GROUP BY u.watch_datekey, u.joyple_game_code, u.country_code, u.os
            ) AS b
            ON (a.joyple_game_code = b.joyple_game_code  AND a.watch_datekey = b.watch_datekey AND a.country_code = b.country_code AND a.os = b.os)
            GROUP BY 1,2,3,4
            ),
            RESULT
            AS (      
            SELECT  u.joyple_game_code
                    , u.auth_account_name
                    , u.auth_method_id
                    , u.watch_datekey
                    , u.country_code
                    , o.os_name
                    , u.media_source
                    , u.campaign
                    , u.init_campaign
                    , u.reg_datekey 
                    , u.reg_datediff
                    , u.watch_cnt
                    , (a.revenue /  a.watch_cnt) * u.watch_cnt AS revenue_per_user
                    , (a.revenue /  a.watch_cnt) * u.watch_cnt * e.exchange_rate AS revenue_per_user_KRW
            FROM T_FULL_SUM AS a
            Left JOIN GameSubUserInfo AS u
            ON (a.joyple_game_code = u.joyple_game_code AND a.watch_datekey = u.watch_datekey AND a.country_code = u.country_code AND a.os = u.os)
            INNER JOIN `datahub-478802.datahub.dim_exchange` AS e
            ON (u.watch_datekey = e.datekey AND e.currency = "USD")
            INNER JOIN `datahub-478802.datahub.dim_os_id` AS o
            ON (a.os = Lower(o.os_name))
            WHERE a.watch_cnt > 0
            )

            SELECT DISTINCT
                joyple_game_code
                , watch_datekey
                , a.auth_account_name 
                , auth_method_id
                , reg_datekey
                , country_code AS reg_country_code
                , os
                , media_source
                , campaign
                , init_campaign
                , reg_datediff
                , watch_cnt
                , revenue_per_user
                , a.revenue_per_user_KRW 
                , a.FromSource 

            FROM (
            SELECT joyple_game_code
                , watch_datekey
                , CAST(auth_account_name as STRING) as auth_account_name
                , auth_method_id
                , reg_datekey
                , country_code
                , os_name as os
                , media_source
                , campaign
                , init_campaign
                , reg_datediff
                , watch_cnt
                , revenue_per_user
                , revenue_per_user_KRW
                , "MediationAPI" AS FromSource
            FROM RESULT
            WHERE joyple_game_code  NOT IN (131, 133, 153, 155, 30003, 30001, 30006, 90001, 159)
                OR (joyple_game_code = 131   AND watch_datekey < '2023-11-16')
                OR (joyple_game_code = 133   AND watch_datekey < '2023-10-16')
                OR (joyple_game_code = 153   AND watch_datekey < '2024-02-01')
                OR (joyple_game_code = 30003 AND watch_datekey < '2024-09-28')
                OR (joyple_game_code = 30001 AND watch_datekey < '2024-10-23')  
                
            UNION ALL 

            SELECT joyple_game_code
                , watch_datekey
                , CAST(auth_account_name as STRING)
                , auth_method_id
                , reg_datekey
                , reg_country_code
                , os_iaa AS os
                , media_source
                , campaign
                , init_campaign
                , reg_datediff
                , COUNT(*)               AS watch_cnt
                , SUM(revenue_per_user_USD) AS revenue_per_user
                , SUM(revenue_per_user_KRW) AS revenue_per_user_KRW
                , "JoypleAPI"            AS FromSource
            FROM `datahub-478802.datahub.f_IAA_auth_account_performance_joyple`
            WHERE watch_datekey >= DATE('2025-11-15', "Asia/Seoul")
                AND watch_datekey <  DATE('2025-11-22', "Asia/Seoul")        
                AND ad_network_code != 33 
                AND (
                        (joyple_game_code = 155   AND revenue_per_user_USD > 0)
                    OR (joyple_game_code = 156   AND revenue_per_user_USD > 0)
                    OR (joyple_game_code = 131   AND watch_datekey >= '2023-11-16' AND revenue_per_user_USD > 0 )
                    OR (joyple_game_code = 133   AND watch_datekey >= '2023-10-16' AND revenue_per_user_USD > 0 )
                    OR (joyple_game_code = 153   AND watch_datekey >= '2024-02-01' AND revenue_per_user_USD > 0 )
                    OR (joyple_game_code = 30003 AND watch_datekey >= '2024-09-28' AND revenue_per_user_USD > 0 )
                    OR (joyple_game_code = 30001 AND watch_datekey >= '2024-10-23' AND revenue_per_user_USD > 0 )
                    OR (joyple_game_code = 30006 AND watch_datekey >= '2025-03-01' AND revenue_per_user_USD > 0 ) 
                    OR (joyple_game_code = 90001 AND watch_datekey >= '2025-03-01' AND revenue_per_user_USD > 0 )
                    OR (joyple_game_code = 159   AND revenue_per_user_USD > 0 ) 
                )
            GROUP BY joyple_game_code
                , watch_datekey
                , auth_account_name
                , auth_method_id
                , reg_datekey
                , reg_country_code
                , os_iaa
                , media_source
                , campaign
                , init_campaign
                , reg_datediff
            ) AS a 
            ;
            """

        # 1. 쿼리 실행
        truncate_query_job = client.query(delete_query)
        truncate_query_job.result()  # 작업 완료 대기
        query_job = client.query(query)

        try:
            # 2. 작업 완료 대기 (여기서 쿼리가 끝날 때까지 블로킹됨)
            # 쿼리에 에러가 있다면 이 라인에서 예외(Exception)가 발생합니다.
            query_job.result()

            # 3. 성공 시 출력
            print(f"✅ 쿼리 실행 성공! (Job ID: {query_job.job_id})")
            print(f"■ {td_str} f_IAA_auth_account_performance Batch 완료")

        except Exception as e:
            # 4. 실패 시 출력
            print(f"❌ 쿼리 실행 중 에러 발생: {e}")
            # Airflow에서 Task를 '실패(Failed)'로 처리하려면 에러를 다시 던져줘야 합니다.
            raise e
    
    print("✅ f_IAA_auth_account_performance ETL 완료")
    return True

        
