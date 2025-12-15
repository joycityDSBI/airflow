from google.cloud import bigquery
from datetime import datetime, timezone, timedelta
import time
import pytz


# 빅쿼리 클라이언트 연결
client = bigquery.Client()

def etl_f_user_map():

    for td in target_date:
        target_date = td

        # KST 00:00:00 ~ 23:59:59를 UTC로 변환
        kst = pytz.timezone('Asia/Seoul')
        start_utc = target_date.replace(tzinfo=kst).astimezone(pytz.UTC)
        end_utc = (target_date + timedelta(days=1)).replace(tzinfo=kst).astimezone(pytz.UTC)
        print(f"📝 시작시간 : ", start_utc, f" 📝 종료시간 : ", end_utc)

        query = f"""
        select A.datekey
        , A.joyple_game_code 
        , A.auth_method_id 
        , A.auth_account_name 
        , A.RU
        , B.reg_datekey 
        , B.reg_datetime
        , DATE_DIFF(A.datekey, B.reg_datekey, DAY) as datediff_reg
        , B.reg_country_code 
        , B.market_id as reg_market_id
        , B.os_id as reg_os_id
        , B.platform_device_type as reg_platform_device_type
        , B.install_datekey
        , DATE_DIFF(A.datekey, B.install_datekey, DAY) as datediff_install
        , B.tracker_account_id 
        , B.tracker_type_id 
        , B.app_id
        , B.bundle_id
        , B.is_organic 
        , B.agency
        , B.campaign
        , B.init_campaign
        , B.adset_name
        , B.ad_name
        , B.is_retargeting 
        , B.advertising_id 
        , B.idfa
        , B.site_id
        , B.channel
        , B.CB1_media_source 
        , B.CB1_campaign 
        , B.CB2_media_source 
        , B.CB2_campaign 
        , B.CB3_media_source 
        , B.CB3_campaign 
        , C.daily_total_rev
        , C.daily_buy_cnt
        , C.daily_pu
        , C.stacked_rev
        , C.stacked_buy_cnt
        , C.stacked_rev_percent_rank
        , C.monthly_rev
        , CASE WHEN C.monthly_rev > 10000000 then '1.R0'
                WHEN C.monthly_rev > 1000000 then '2.R1'
                WHEN C.monthly_rev > 100000 then '3.R2'
                WHEN C.monthly_rev > 10000 then '4.R3'
                WHEN C.monthly_rev > 0 then '5.R4'
                ELSE '6.Non_pu' END AS monthly_rgroup
        , C.before_monthly_rev
        , CASE WHEN C.before_monthly_rev > 10000000 then '1.R0'
                WHEN C.before_monthly_rev > 1000000 then '2.R1'
                WHEN C.before_monthly_rev > 100000 then '3.R2'
                WHEN C.before_monthly_rev > 10000 then '4.R3'
                WHEN C.before_monthly_rev > 0 then '5.R4'
                ELSE '6.Non_pu' END AS before_monthly_rgroup
        , C.yealy_rev
        , C.last_30days_rev
        , C.first_payment_datekey
        , C.last_payment_datekey
        , D.daily_IAP_rev
        , E.stacked_IAA_watch_cnt
        , E.stacked_IAA_rev
        , E.daily_IAA_watch_cnt
        , E.daily_IAA_rev
        , E.monthly_IAA_rev
        , F.play_seconds
        , F.access_cnt
        , F.daily_game_user_level
        , F.stacked_max_game_user_level
        , F.last_login_datekey
        , DATE_DIFF(A.datekey, F.last_login_datekey, DAY) as datediff_last_login
        , F.stickeness
        , IF(C.daily_pu = 1 AND A.RU = 1, 1, 0) as NRPU
        , H.first_game_sub_user_name
        , I.products_array
        from
        (
            SELECT datekey, joyple_game_code, auth_account_name, auth_method_id
            , IF(datekey = reg_datekey, 1, 0) AS RU
            FROM datahub-478802.datahub.f_common_access
            where datekey >= '2025-12-01' and datekey < '2025-12-02'
            and access_type_id = 1
            group by 1,2,3,4,5
        ) AS A
        LEFT OUTER JOIN
        (
            SELECT joyple_game_code, auth_account_name, auth_method_id,
            reg_datekey, reg_datetime,
            tracker_account_id, tracker_type_id,
            reg_country_code, market_id, os_id, platform_device_type,
            install_country_code, install_datekey, 
            app_id, bundle_id, is_organic, agency, campaign, init_campaign,
            adset_name, ad_name, is_retargeting, advertising_id, idfa, site_id,
            channel, CB1_media_source, CB1_campaign, CB2_media_source, CB2_campaign, CB3_media_source, CB3_campaign, 
            FROM datahub-478802.datahub.f_common_register
        ) AS B
        ON A.joyple_game_code = B.joyple_game_code AND A.auth_method_id = B.auth_method_id AND A.auth_account_name = B.auth_account_name
        -- 당일 구매액 (IAP + 웹상점)
        LEFT OUTER JOIN
        (
            SELECT joyple_game_code, auth_account_name, auth_method_id,
            SUM(IF(datekey >= '2025-12-01' AND datekey < '2025-12-02', revenue, 0)) as daily_total_rev,
            SUM(IF(datekey >= '2025-12-01' AND datekey < '2025-12-02', buy_cnt, 0)) as daily_buy_cnt,
            MAX(IF(datekey >= '2025-12-01' AND datekey < '2025-12-02', 1, 0)) as daily_pu,
            SUM(revenue) as stacked_rev,
            sum(buy_cnt) as stacked_buy_cnt,
            PERCENT_RANK() OVER (PARTITION BY joyple_game_code ORDER BY sum(revenue) DESC) AS stacked_rev_percent_rank,
            SUM(IF(datekey >= DATE_TRUNC(DATE('2025-12-01'), MONTH) AND datekey <= LAST_DAY(DATE_TRUNC(DATE('2025-12-01'), MONTH), MONTH), revenue, 0)) as monthly_rev,
            SUM(IF(datekey >= DATE_TRUNC(DATE_SUB(DATE('2025-12-01'), INTERVAL 1 MONTH), MONTH) AND datekey <= LAST_DAY(DATE_TRUNC(DATE_SUB(DATE('2025-12-01'), INTERVAL 1 MONTH), MONTH), MONTH), revenue, 0)) as before_monthly_rev,
            SUM(IF(datekey >= DATE_TRUNC(DATE('2025-12-01'), YEAR) AND datekey <= LAST_DAY(DATE_TRUNC(DATE('2025-12-01'), YEAR), YEAR), revenue, 0)) as yealy_rev,
            SUM(IF(datekey >= DATE_SUB('2025-12-01', INTERVAL 30 DAY) AND datekey < '2025-12-02', revenue, 0)) as last_30days_rev,
            min(datekey) as first_payment_datekey,
            MAX(IF(datekey < '2025-12-01', datekey, null)) as last_payment_datekey
            FROM datahub-478802.datahub.f_common_payment_total
            WHERE datekey < '2025-12-02'
            group by joyple_game_code, auth_account_name, auth_method_id
        ) AS C
        ON A.joyple_game_code = C.joyple_game_code AND A.auth_method_id = C.auth_method_id AND A.auth_account_name = C.auth_account_name
        -- 당일 구매액 및 당일 PU 여부 (IAP)
        LEFT OUTER JOIN
        (
            SELECT joyple_game_code, auth_account_name, auth_method_id,
            sum(revenue) as daily_IAP_rev
            FROM datahub-478802.datahub.f_common_payment
            where datekey >= '2025-12-01' and datekey < '2025-12-02'
            group by joyple_game_code, auth_account_name, auth_method_id
        ) AS D
        ON A.joyple_game_code = D.joyple_game_code AND A.auth_method_id = D.auth_method_id AND A.auth_account_name = D.auth_account_name
        -- 누적 IAA 매출액, 광고 시청 횟수
        LEFT OUTER JOIN
        (
            SELECT joyple_game_code, auth_account_name, auth_method_id,
            sum(watch_cnt) as stacked_IAA_watch_cnt,
            sum(revenue_per_user) as stacked_IAA_rev,
            sum(IF(watch_datekey >= '2025-12-01' AND watch_datekey < '2025-12-02', watch_cnt, 0)) as daily_IAA_watch_cnt,
            sum(IF(watch_datekey >= '2025-12-01' AND watch_datekey < '2025-12-02', revenue_per_user, 0)) as daily_IAA_rev,
            SUM(IF(watch_datekey >= DATE_TRUNC(DATE('2025-12-01'), MONTH) AND watch_datekey <= LAST_DAY(DATE_TRUNC(DATE('2025-12-01'), MONTH), MONTH), revenue_per_user, 0)) as monthly_IAA_rev
            FROM datahub-478802.datahub.f_IAA_auth_account_performance
            where watch_datekey < '2025-12-02'
            group by joyple_game_code, auth_account_name, auth_method_id
        ) AS E
        ON A.joyple_game_code = E.joyple_game_code AND A.auth_method_id = E.auth_method_id AND A.auth_account_name = E.auth_account_name
        -- 당일 플레이 타임 및 access count, 최대 달성 레벨 (access 로그 기준)
        LEFT OUTER JOIN
        (
            SELECT joyple_game_code, auth_account_name, auth_method_id
            , SUM(IF(datekey >= '2025-12-01' AND datekey < '2025-12-02', play_seconds, 0)) as play_seconds
            , SUM(IF(datekey >= '2025-12-01' AND datekey < '2025-12-02', access_cnt, 0)) as access_cnt
            , MAX(IF(datekey >= '2025-12-01' AND datekey < '2025-12-02', game_user_level, null)) as daily_game_user_level
            , MAX(game_user_level) as stacked_max_game_user_level
            , MAX(IF(datekey < '2025-12-01' AND access_type_id = 1, datekey, null)) as last_login_datekey
            , COUNT(DISTINCT(IF(datekey >= DATE_SUB('2025-12-01', INTERVAL 6 DAY) AND datekey < '2025-12-02' AND access_type_id = 1, datekey, null))) as stickeness
            FROM datahub-478802.datahub.f_common_access
            where datekey < '2025-12-02'
            group by joyple_game_code, auth_account_name, auth_method_id
        ) AS F
        ON A.joyple_game_code = F.joyple_game_code AND A.auth_method_id = F.auth_method_id AND A.auth_account_name = F.auth_account_name
        -- 가입 시 유저 캐릭터 ID (game sub user name)
        LEFT OUTER JOIN
        (
            SELECT joyple_game_code, auth_account_name, auth_method_id,
                ARRAY_AGG(game_sub_user_name ORDER BY game_sub_user_reg_datetime)[OFFSET(0)] AS first_game_sub_user_name
            FROM `datahub-478802.datahub.f_common_register_char`
            GROUP BY joyple_game_code, auth_account_name, auth_method_id
        ) AS H
        ON A.joyple_game_code = H.joyple_game_code AND A.auth_method_id = H.auth_method_id AND A.auth_account_name = H.auth_account_name
        -- PaymentDetailArrayStruct : 당일 구매한 아이템에 대해서 pg 포함한 아이템 정보 : PG사 별로 보기 위해 추가 필수
        LEFT OUTER JOIN
        (
            SELECT joyple_game_code, auth_method_id, auth_account_name, 
                ARRAY_AGG(STRUCT(pg_id, platform_device_type, product_code, product_name, revenue, buy_cnt) ORDER BY datekey ASC) as products_array
            FROM `datahub-478802.datahub.f_common_payment_total`
            WHERE datekey >= '2025-12-01' and datekey < '2025-12-02'
            GROUP BY joyple_game_code, auth_method_id, auth_account_name
        ) AS I
        ON A.joyple_game_code = I.joyple_game_code AND A.auth_method_id = I.auth_method_id AND A.auth_account_name  = I.auth_account_name
        """
        client.query(query)
        print(f"■ {target_date.strftime('%Y-%m-%d')} f_user_map Batch 완료")
    
    print("✅ f_user_map ETL 완료")
    return True