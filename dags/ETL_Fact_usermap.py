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
        start_date = datetime.strptime(td, "%Y-%m-%d")
        end_date = start_date + timedelta(days=1)
        start_date = start_date.strftime("%Y-%m-%d") # '2014-06-10'
        end_date = end_date.strftime("%Y-%m-%d")
        print(f"📝 시작시간 : ", start_date, f" 📝 종료시간 : ", end_date)
        print(f"■ {target_date.strftime('%Y-%m-%d')} f_user_map Batch 시작")

        query = f"""
        INSERT INTO datahub-478802.datahub.f_user_map
        (
        datekey
        , joyple_game_code 
        , auth_method_id 
        , auth_account_name 
        , RU
        , reg_datekey 
        , reg_datetime
        , datediff_reg
        , reg_country_code 
        , reg_market_id
        , reg_os_id
        , reg_platform_device_type
        , install_datekey
        , datediff_install
        , tracker_account_id 
        , tracker_type_id 
        , app_id
        , media_source
        , bundle_id
        , is_organic 
        , agency
        , campaign
        , init_campaign
        , adset_name
        , ad_name
        , is_retargeting 
        , advertising_id 
        , idfa
        , site_id
        , channel
        , CB1_media_source 
        , CB1_campaign 
        , CB2_media_source 
        , CB2_campaign 
        , CB3_media_source 
        , CB3_campaign 
        , daily_total_rev
        , daily_buy_cnt
        , daily_pu
        , stacked_rev
        , stacked_buy_cnt
        , stacked_rev_percent_rank
        , monthly_rev
        , monthly_rgroup
        , before_monthly_rev
        , before_monthly_rgroup
        , yearly_rev
        , last_30days_rev
        , first_payment_datekey
        , last_payment_datekey
        , stacked_IAA_watch_cnt
        , stacked_IAA_rev
        , daily_IAA_watch_cnt
        , daily_IAA_rev
        , monthly_IAA_rev
        , play_seconds
        , access_cnt
        , daily_game_user_level
        , last_login_datekey
        , max_game_user_level
        , datediff_last_login
        , stickiness
        , NRPU
        , game_sub_user_name_cnt
        , products_array
        )
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
        , B.media_source
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
        , C.yearly_rev
        , C.last_30days_rev
        , C.first_payment_datekey
        , C.last_payment_datekey
        , E.stacked_IAA_watch_cnt
        , E.stacked_IAA_rev
        , E.daily_IAA_watch_cnt
        , E.daily_IAA_rev
        , E.monthly_IAA_rev
        , F.play_seconds
        , F.access_cnt
        , F.daily_game_user_level
        , G.last_login_datekey
        , G.max_game_user_level
        , DATE_DIFF(A.datekey, G.last_login_datekey, DAY) as datediff_last_login
        , F.stickiness
        , IF(C.daily_pu = 1 AND A.RU = 1, 1, 0) as NRPU
        , H.game_sub_user_name_cnt
        , I.products_array
        from
        (
            SELECT datekey, joyple_game_code, auth_account_name, min(auth_method_id) as auth_method_id
            , IF(datekey = reg_datekey, 1, 0) AS RU
            FROM datahub-478802.datahub.f_common_access
            where datekey >= '{start_date}' and datekey < '{end_date}'
            and access_type_id = 1
            group by 1,2,3,5
        ) AS A
        LEFT OUTER JOIN
        (
            SELECT joyple_game_code, auth_account_name, 
            reg_datekey, reg_datetime,
            tracker_account_id, tracker_type_id,
            reg_country_code, market_id, os_id, platform_device_type,
            install_country_code, install_datekey, 
            app_id, media_source, bundle_id, is_organic, agency, campaign, init_campaign,
            adset_name, ad_name, is_retargeting, advertising_id, idfa, site_id,
            channel, CB1_media_source, CB1_campaign, CB2_media_source, CB2_campaign, CB3_media_source, CB3_campaign
            FROM datahub-478802.datahub.f_common_register
            WHERE reg_datekey < '{end_date}'
        ) AS B
        ON A.joyple_game_code = B.joyple_game_code AND A.auth_account_name = B.auth_account_name
        -- 당일 구매액 (IAP + 웹상점)
        LEFT OUTER JOIN
        (
            SELECT joyple_game_code, auth_account_name, 
            SUM(IF(datekey >= '{start_date}' AND datekey < '{end_date}', revenue, 0)) as daily_total_rev,
            SUM(IF(datekey >= '{start_date}' AND datekey < '{end_date}', buy_cnt, 0)) as daily_buy_cnt,
            MAX(IF(datekey >= '{start_date}' AND datekey < '{end_date}', 1, 0)) as daily_pu,
            SUM(revenue) as stacked_rev,
            sum(buy_cnt) as stacked_buy_cnt,
            PERCENT_RANK() OVER (PARTITION BY joyple_game_code ORDER BY sum(revenue) DESC) AS stacked_rev_percent_rank,
            SUM(IF(datekey >= DATE_TRUNC(DATE('{start_date}'), MONTH) AND datekey <= LAST_DAY(DATE_TRUNC(DATE('{start_date}'), MONTH), MONTH), revenue, 0)) as monthly_rev,
            SUM(IF(datekey >= DATE_TRUNC(DATE_SUB(DATE('{start_date}'), INTERVAL 1 MONTH), MONTH) AND datekey <= LAST_DAY(DATE_TRUNC(DATE_SUB(DATE('{start_date}'), INTERVAL 1 MONTH), MONTH), MONTH), revenue, 0)) as before_monthly_rev,
            SUM(IF(datekey >= DATE_TRUNC(DATE('{start_date}'), YEAR) AND datekey <= LAST_DAY(DATE_TRUNC(DATE('{start_date}'), YEAR), YEAR), revenue, 0)) as yearly_rev,
            SUM(IF(datekey >= DATE_SUB('{start_date}', INTERVAL 30 DAY) AND datekey < '{end_date}', revenue, 0)) as last_30days_rev,
            min(datekey) as first_payment_datekey,
            MAX(IF(datekey < '{start_date}', datekey, null)) as last_payment_datekey
            FROM datahub-478802.datahub.f_common_payment
            WHERE datekey < '{end_date}'
            group by joyple_game_code, auth_account_name
        ) AS C
        ON A.joyple_game_code = C.joyple_game_code AND A.auth_account_name = C.auth_account_name
        -- 누적 IAA 매출액, 광고 시청 횟수
        LEFT OUTER JOIN
        (
            SELECT joyple_game_code, auth_account_name,
            sum(watch_cnt) as stacked_IAA_watch_cnt,
            sum(revenue_per_user) as stacked_IAA_rev,
            sum(IF(watch_datekey >= '{start_date}' AND watch_datekey < '{end_date}', watch_cnt, 0)) as daily_IAA_watch_cnt,
            sum(IF(watch_datekey >= '{start_date}' AND watch_datekey < '{end_date}', revenue_per_user, 0)) as daily_IAA_rev,
            SUM(IF(watch_datekey >= DATE_TRUNC(DATE('{start_date}'), MONTH) AND watch_datekey <= LAST_DAY(DATE_TRUNC(DATE('{start_date}'), MONTH), MONTH), revenue_per_user, 0)) as monthly_IAA_rev
            FROM datahub-478802.datahub.f_IAA_auth_account_performance
            where watch_datekey < '{end_date}'
            group by joyple_game_code, auth_account_name
        ) AS E
        ON A.joyple_game_code = E.joyple_game_code AND A.auth_account_name = E.auth_account_name
        -- 당일 플레이 타임 및 access count
        LEFT OUTER JOIN
        (
            SELECT joyple_game_code, auth_account_name
            , SUM(IF(datekey >= '{start_date}' AND datekey < '{end_date}', play_seconds, 0)) as play_seconds
            , SUM(IF(datekey >= '{start_date}' AND datekey < '{end_date}', access_cnt, 0)) as access_cnt
            , MAX(IF(datekey >= '{start_date}' AND datekey < '{end_date}', game_user_level, null)) as daily_game_user_level
            , COUNT(DISTINCT(IF(datekey >= DATE_SUB('{start_date}', INTERVAL 6 DAY) AND datekey < '{end_date}' AND access_type_id = 1, datekey, null))) as stickiness
            FROM datahub-478802.datahub.f_common_access
            where datekey >= DATE_SUB('{start_date}', INTERVAL 6 DAY) AND datekey < '{end_date}'
            group by joyple_game_code, auth_account_name
        ) AS F
        ON A.joyple_game_code = F.joyple_game_code AND A.auth_account_name = F.auth_account_name
        -- 마지막 로그인 일자, 최대 달성 레벨 (access 로그 기준)
        LEFT OUTER JOIN
        (
            SELECT joyple_game_code, auth_account_name
                , MAX(last_login_datekey) as last_login_datekey
                , MAX(max_game_user_level) as max_game_user_level
            FROM datahub-478802.datahub.f_common_access_last_login
            group by joyple_game_code, auth_account_name
        ) AS G
        ON A.joyple_game_code = G.joyple_game_code AND A.auth_account_name = G.auth_account_name
        -- 가입 시 유저 캐릭터 ID (game sub user name), -- 보유 계정 수 (game_sub_user_name)
        LEFT OUTER JOIN
        (
            SELECT joyple_game_code, auth_account_name
                , count(DISTINCT game_sub_user_name) as game_sub_user_name_cnt
            FROM `datahub-478802.datahub.f_common_register_char`
            where reg_datekey < '{end_date}'
            GROUP BY joyple_game_code, auth_account_name
        ) AS H
        ON A.joyple_game_code = H.joyple_game_code AND A.auth_account_name = H.auth_account_name
        -- PaymentDetailArrayStruct : 당일 구매한 아이템에 대해서 pg 포함한 아이템 정보 : PG사 별로 보기 위해 추가 필수
        LEFT OUTER JOIN
        (
            SELECT joyple_game_code, auth_account_name, 
                ARRAY_AGG(STRUCT(pg_id, platform_device_type, product_code, product_name, revenue, buy_cnt) ORDER BY datekey ASC) as products_array
            FROM `datahub-478802.datahub.f_common_payment`
            WHERE datekey >= '{start_date}' and datekey < '{end_date}'
            GROUP BY joyple_game_code, auth_account_name
        ) AS I
        ON A.joyple_game_code = I.joyple_game_code AND A.auth_account_name  = I.auth_account_name
        ;
        """
        client.query(query)
        print(f"■ {target_date.strftime('%Y-%m-%d')} f_user_map Batch 완료")
    
    print("✅ f_user_map ETL 완료")
    return True



def etl_f_user_map_char():

    for td in target_date:
        target_date = td

        start_date = datetime.strptime(td, "%Y-%m-%d")
        end_date = start_date + timedelta(days=1)
        start_date = start_date.strftime("%Y-%m-%d") # '2014-06-10'
        end_date = end_date.strftime("%Y-%m-%d")  
        print(f"📝 시작시간 : ", start_date, f" 📝 종료시간 : ", end_date)

        query = f"""
        INSERT INTO `datahub-478802.datahub.f_user_map_char`
        (
            datekey, joyple_game_code, auth_account_name, auth_method_id, game_sub_user_name, RU, game_id, world_id, server_name,
            reg_country_code, reg_market_id, reg_os_id, platform_device_type, tracker_type_id, tracker_account_id,
            game_sub_user_reg_datekey, datediff_sub_user_reg, game_sub_user_reg_datetime, user_reg_datekey, datediff_reg,
            daily_total_rev, daily_buy_cnt, daily_pu, stacked_rev, stacked_buy_cnt, stacked_rev_percent_rank, monthly_rev,
            monthly_rgroup_sub_user, before_monthly_rev, before_monthly_rgroup_sub_user, yearly_rev, last_30days_rev,
            first_payment_datekey, last_payment_datekey, stacked_IAA_rev, daily_IAA_rev, play_seconds, access_cnt,
            max_game_user_level, last_login_datekey, datediff_last_login_sub_user, stickiness, products_array,
            install_country_code, install_datekey, app_id, media_source, bundle_id, is_organic, agency, campaign, init_campaign,
            adset_name, ad_name, is_retargeting, advertising_id, idfa, site_id, channel, CB1_media_source,
            CB1_campaign, CB2_media_source, CB2_campaign, CB3_media_source, CB3_campaign
        )
        SELECT A.datekey, A.joyple_game_code, A.auth_account_name, A.auth_method_id, A.game_sub_user_name, A.RU,
               B.game_id, B.world_id, B.server_name, B.country_code, B.market_id, B.os_id, B.platform_device_type,
               B.tracker_type_id, B.tracker_account_id, B.game_sub_user_reg_datekey, DATE_DIFF(A.datekey, B.game_sub_user_reg_datekey, DAY),
               B.game_sub_user_reg_datetime, B.reg_datekey, DATE_DIFF(A.datekey, B.reg_datekey, DAY),
               C.daily_total_rev, C.daily_buy_cnt, C.daily_pu, C.stacked_rev, C.stacked_buy_cnt, C.stacked_rev_percent_rank,
               C.monthly_rev,
               CASE WHEN C.monthly_rev > 10000000 then '1.R0' WHEN C.monthly_rev > 1000000 then '2.R1' 
                    WHEN C.monthly_rev > 100000 then '3.R2' WHEN C.monthly_rev > 10000 then '4.R3' 
                    WHEN C.monthly_rev > 0 then '5.R4' ELSE '6.Non_pu' END,
               C.before_monthly_rev,
               CASE WHEN C.before_monthly_rev > 10000000 then '1.R0' WHEN C.before_monthly_rev > 1000000 then '2.R1' 
                    WHEN C.before_monthly_rev > 100000 then '3.R2' WHEN C.before_monthly_rev > 10000 then '4.R3' 
                    WHEN C.before_monthly_rev > 0 then '5.R4' ELSE '6.Non_pu' END,
               C.yearly_rev, C.last_30days_rev, C.first_payment_datekey, C.last_payment_datekey,
               D.stacked_IAA_rev, D.daily_IAA_rev, E.play_seconds, E.access_cnt, H.max_game_user_level, H.last_login_datekey,
               DATE_DIFF(A.datekey, H.last_login_datekey, DAY), E.stickiness, F.products_array,
               G.install_country_code, G.install_datekey, G.app_id, G.media_source, G.bundle_id, G.is_organic, G.agency, G.campaign,
               G.init_campaign, G.adset_name, G.ad_name, G.is_retargeting, G.advertising_id, G.idfa, G.site_id, G.channel,
               G.CB1_media_source, G.CB1_campaign, G.CB2_media_source, G.CB2_campaign, G.CB3_media_source, G.CB3_campaign
        FROM (
            SELECT datekey, joyple_game_code, auth_account_name, game_sub_user_name, min(auth_method_id) as auth_method_id, 
                   MAX(IF(datekey = reg_datekey, 1, 0)) AS RU
            FROM `datahub-478802.datahub.f_common_access`
            WHERE datekey >= '{start_date}' and datekey < '{end_date}' AND access_type_id = 1
            GROUP BY 1,2,3,4
        ) AS A
        LEFT OUTER JOIN 
            (
                SELECT * FROM `datahub-478802.datahub.f_common_register_char` WHERE game_sub_user_reg_datekey < '{end_date}'
            ) AS B
        ON A.joyple_game_code = B.joyple_game_code AND A.auth_account_name = B.auth_account_name AND A.game_sub_user_name = B.game_sub_user_name
        LEFT OUTER JOIN (
            SELECT joyple_game_code, auth_account_name, game_sub_user_name,
                   SUM(IF(datekey >= '{start_date}' AND datekey < '{end_date}', revenue, 0)) as daily_total_rev,
                   SUM(IF(datekey >= '{start_date}' AND datekey < '{end_date}', buy_cnt, 0)) as daily_buy_cnt,
                   MAX(IF(datekey >= '{start_date}' AND datekey < '{end_date}', 1, 0)) as daily_pu,
                   SUM(revenue) as stacked_rev, sum(buy_cnt) as stacked_buy_cnt,
                   PERCENT_RANK() OVER (PARTITION BY joyple_game_code ORDER BY sum(revenue) DESC) AS stacked_rev_percent_rank,
                   SUM(IF(datekey >= DATE_TRUNC(DATE('{start_date}'), MONTH) AND datekey <= LAST_DAY(DATE_TRUNC(DATE('{start_date}'), MONTH), MONTH), revenue, 0)) as monthly_rev,
                   SUM(IF(datekey >= DATE_TRUNC(DATE_SUB(DATE('{start_date}'), INTERVAL 1 MONTH), MONTH) AND datekey <= LAST_DAY(DATE_TRUNC(DATE_SUB(DATE('{start_date}'), INTERVAL 1 MONTH), MONTH), MONTH), revenue, 0)) as before_monthly_rev,
                   SUM(IF(datekey >= DATE_TRUNC(DATE('{start_date}'), YEAR) AND datekey <= LAST_DAY(DATE_TRUNC(DATE('{start_date}'), YEAR), YEAR), revenue, 0)) as yearly_rev,
                   SUM(IF(datekey >= DATE_SUB('{start_date}', INTERVAL 30 DAY) AND datekey < '{end_date}', revenue, 0)) as last_30days_rev,
                   min(datekey) as first_payment_datekey, MAX(IF(datekey < '{start_date}', datekey, null)) as last_payment_datekey
            FROM `datahub-478802.datahub.f_common_payment` WHERE datekey < '{end_date}'
            GROUP BY 1,2,3
        ) AS C ON A.joyple_game_code = C.joyple_game_code AND A.auth_account_name = C.auth_account_name AND A.game_sub_user_name = C.game_sub_user_name
        LEFT OUTER JOIN (
            SELECT joyple_game_code, auth_account_name, game_sub_user_name, SUM(revenue_per_user_KRW) as stacked_IAA_rev,
                   SUM(IF(watch_datekey >= '{start_date}' AND watch_datekey < '{end_date}', revenue_per_user_KRW, 0)) as daily_IAA_rev
            FROM `datahub-478802.datahub.f_IAA_auth_account_performance_joyple` WHERE watch_datekey < '{end_date}'
            GROUP BY 1,2,3
        ) AS D ON A.joyple_game_code = D.joyple_game_code  AND A.auth_account_name = D.auth_account_name AND A.game_sub_user_name = D.game_sub_user_name
        LEFT OUTER JOIN (
            SELECT joyple_game_code, auth_account_name, game_sub_user_name,
                   SUM(IF(datekey >= '{start_date}' AND datekey < '{end_date}', play_seconds, 0)) as play_seconds,
                   SUM(IF(datekey >= '{start_date}' AND datekey < '{end_date}', access_cnt, 0)) as access_cnt,
                   MAX(game_user_level) as max_game_user_level,
                   MAX(IF(datekey < '{start_date}' AND access_type_id = 1, datekey, null)) as last_login_datekey,
                   COUNT(DISTINCT(IF(datekey >= DATE_SUB('{start_date}', INTERVAL 6 DAY) AND datekey < '{end_date}' AND access_type_id = 1, datekey, null))) as stickiness
            FROM `datahub-478802.datahub.f_common_access` WHERE datekey >= DATE_SUB('{start_date}', INTERVAL 6 DAY) AND datekey < '{end_date}'
            GROUP BY 1,2,3
        ) AS E ON A.joyple_game_code = E.joyple_game_code AND A.auth_account_name = E.auth_account_name AND A.game_sub_user_name = E.game_sub_user_name
        LEFT OUTER JOIN (
            SELECT joyple_game_code, auth_account_name, game_sub_user_name,
                   ARRAY_AGG(STRUCT(pg_id, platform_device_type, product_code, product_name, revenue, buy_cnt) ORDER BY datekey ASC) as products_array
            FROM `datahub-478802.datahub.f_common_payment` WHERE datekey >= '{start_date}' and datekey < '{end_date}'
            GROUP BY 1,2,3
        ) AS F ON A.joyple_game_code = F.joyple_game_code AND A.auth_account_name = F.auth_account_name AND A.game_sub_user_name = F.game_sub_user_name
        LEFT OUTER JOIN (SELECT * FROM `datahub-478802.datahub.f_common_register` WHERE reg_datekey < '{end_date}') AS G
            ON A.joyple_game_code = G.joyple_game_code AND A.auth_account_name = G.auth_account_name
        LEFT OUTER JOIN (
            SELECT joyple_game_code, auth_account_name, game_sub_user_name, MAX(last_login_datekey) as last_login_datekey, MAX(max_game_user_level) as max_game_user_level
            FROM `datahub-478802.datahub.f_common_access_last_login` GROUP BY 1,2,3
        ) AS H ON A.joyple_game_code = H.joyple_game_code AND A.auth_account_name = H.auth_account_name AND A.game_sub_user_name = H.game_sub_user_name;
        """
        client.query(query)
        print(f"■ {target_date.strftime('%Y-%m-%d')} f_user_map_char Batch 완료")
    
    print("✅ f_user_map_char ETL 완료")
    return True