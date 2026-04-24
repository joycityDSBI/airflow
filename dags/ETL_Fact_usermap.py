from google.cloud import bigquery
from datetime import datetime, timezone, timedelta
import time
import pytz



def etl_f_user_map(target_date: list, client):

    for td in target_date:
        start_date = datetime.strptime(td, "%Y-%m-%d")
        end_date = start_date + timedelta(days=1)
        start_date = start_date.strftime("%Y-%m-%d")
        end_date = end_date.strftime("%Y-%m-%d")
        print(f"📝 시작시간 : {start_date}  📝 종료시간 : {end_date}")
        print(f"■ {start_date} f_user_map Batch 시작")

        query = f"""
        MERGE datahub-478802.datahub.f_user_map as target
        USING
        ( 
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
                , B.first_tracking_datekey
                , DATE_DIFF(A.datekey, B.first_tracking_datekey, DAY) as datediff_first_tracking
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
                , CASE WHEN FORMAT_DATE('%Y-%m', B.reg_datekey) = FORMAT_DATE('%Y-%m', A.datekey) then '7.RU'
                        WHEN C.before_monthly_rev > 10000000 then '1.R0'
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
                    SELECT datekey, joyple_game_code, auth_account_name, MIN(auth_method_id) as auth_method_id
                    , MAX(IF(datekey = reg_datekey, 1, 0)) AS RU
                    FROM datahub-478802.datahub.f_common_access
                    where datekey >= '{start_date}' and datekey < '{end_date}'
                    and access_type_id = 1
                    group by 1,2,3
                ) AS A
                LEFT OUTER JOIN
                (
                    SELECT joyple_game_code, auth_account_name,
                    reg_datekey, reg_datetime,
                    tracker_account_id, tracker_type_id,
                    reg_country_code, market_id, os_id, platform_device_type,
                    first_tracking_country_code, first_tracking_datekey,
                    app_id, media_source, bundle_id, is_organic, agency, campaign, init_campaign,
                    adset_name, ad_name, is_retargeting, advertising_id, idfa, site_id,
                    channel, CB1_media_source, CB1_campaign, CB2_media_source, CB2_campaign, CB3_media_source, CB3_campaign
                    FROM datahub-478802.datahub.f_common_register
                    WHERE reg_datekey < '{end_date}'
                ) AS B
                ON A.joyple_game_code = B.joyple_game_code AND A.auth_account_name = B.auth_account_name
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
                LEFT OUTER JOIN
                (
                    SELECT joyple_game_code, auth_account_name,
                    sum(watch_cnt) as stacked_IAA_watch_cnt,
                    sum(revenue_per_user_krw) as stacked_IAA_rev,
                    sum(IF(watch_datekey >= '{start_date}' AND watch_datekey < '{end_date}', watch_cnt, 0)) as daily_IAA_watch_cnt,
                    sum(IF(watch_datekey >= '{start_date}' AND watch_datekey < '{end_date}', revenue_per_user_krw, 0)) as daily_IAA_rev,
                    SUM(IF(watch_datekey >= DATE_TRUNC(DATE('{start_date}'), MONTH) AND watch_datekey <= LAST_DAY(DATE_TRUNC(DATE('{start_date}'), MONTH), MONTH), revenue_per_user_krw, 0)) as monthly_IAA_rev
                    FROM datahub-478802.datahub.f_IAA_auth_account_performance
                    where watch_datekey < '{end_date}'
                    group by joyple_game_code, auth_account_name
                ) AS E
                ON A.joyple_game_code = E.joyple_game_code AND A.auth_account_name = E.auth_account_name
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
                LEFT OUTER JOIN
                (
                    SELECT joyple_game_code, auth_account_name
                        , MAX(last_login_datekey) as last_login_datekey
                        , MAX(max_game_user_level) as max_game_user_level
                    FROM datahub-478802.datahub.f_common_access_last_login
                    group by joyple_game_code, auth_account_name
                ) AS G
                ON A.joyple_game_code = G.joyple_game_code AND A.auth_account_name = G.auth_account_name
                LEFT OUTER JOIN
                (
                    SELECT joyple_game_code, auth_account_name
                        , count(DISTINCT game_sub_user_name) as game_sub_user_name_cnt
                    FROM `datahub-478802.datahub.f_common_register_char`
                    where reg_datekey < '{end_date}'
                    GROUP BY joyple_game_code, auth_account_name
                ) AS H
                ON A.joyple_game_code = H.joyple_game_code AND A.auth_account_name = H.auth_account_name
                LEFT OUTER JOIN
                (
                    SELECT joyple_game_code, auth_account_name,
                        ARRAY_AGG(STRUCT(pg_id, platform_device_type, product_code, product_name, revenue, buy_cnt) ORDER BY datekey ASC) as products_array
                    FROM `datahub-478802.datahub.f_common_payment`
                    WHERE datekey >= '{start_date}' and datekey < '{end_date}'
                    GROUP BY joyple_game_code, auth_account_name
                ) AS I
                ON A.joyple_game_code = I.joyple_game_code AND A.auth_account_name  = I.auth_account_name
        ) as source
        ON target.datekey = source.datekey
        AND target.joyple_game_code = source.joyple_game_code
        AND target.auth_account_name = source.auth_account_name
        WHEN NOT MATCHED BY target THEN
        INSERT
        (
        datekey, joyple_game_code, auth_method_id, auth_account_name, RU,
        reg_datekey, reg_datetime, datediff_reg, reg_country_code, reg_market_id,
        reg_os_id, reg_platform_device_type, first_tracking_datekey, datediff_first_tracking,
        tracker_account_id, tracker_type_id, app_id, media_source, bundle_id,
        is_organic, agency, campaign, init_campaign, adset_name, ad_name,
        is_retargeting, advertising_id, idfa, site_id, channel,
        CB1_media_source, CB1_campaign, CB2_media_source, CB2_campaign,
        CB3_media_source, CB3_campaign, daily_total_rev, daily_buy_cnt, daily_pu,
        stacked_rev, stacked_buy_cnt, stacked_rev_percent_rank,
        monthly_rev, monthly_rgroup, before_monthly_rev, before_monthly_rgroup,
        yearly_rev, last_30days_rev, first_payment_datekey, last_payment_datekey,
        stacked_IAA_watch_cnt, stacked_IAA_rev, daily_IAA_watch_cnt, daily_IAA_rev,
        monthly_IAA_rev, play_seconds, access_cnt, daily_game_user_level,
        last_login_datekey, max_game_user_level, datediff_last_login,
        stickiness, NRPU, game_sub_user_name_cnt, products_array
        )
        VALUES
        (
          source.datekey, source.joyple_game_code, source.auth_method_id,
          source.auth_account_name, source.RU, source.reg_datekey, source.reg_datetime,
          source.datediff_reg, source.reg_country_code, source.reg_market_id,
          source.reg_os_id, source.reg_platform_device_type, source.first_tracking_datekey,
          source.datediff_first_tracking, source.tracker_account_id, source.tracker_type_id,
          source.app_id, source.media_source, source.bundle_id, source.is_organic,
          source.agency, source.campaign, source.init_campaign, source.adset_name,
          source.ad_name, source.is_retargeting, source.advertising_id, source.idfa,
          source.site_id, source.channel, source.CB1_media_source, source.CB1_campaign,
          source.CB2_media_source, source.CB2_campaign, source.CB3_media_source,
          source.CB3_campaign, source.daily_total_rev, source.daily_buy_cnt,
          source.daily_pu, source.stacked_rev, source.stacked_buy_cnt,
          source.stacked_rev_percent_rank, source.monthly_rev, source.monthly_rgroup,
          source.before_monthly_rev, source.before_monthly_rgroup, source.yearly_rev,
          source.last_30days_rev, source.first_payment_datekey, source.last_payment_datekey,
          source.stacked_IAA_watch_cnt, source.stacked_IAA_rev, source.daily_IAA_watch_cnt,
          source.daily_IAA_rev, source.monthly_IAA_rev, source.play_seconds,
          source.access_cnt, source.daily_game_user_level, source.last_login_datekey,
          source.max_game_user_level, source.datediff_last_login, source.stickiness,
          source.NRPU, source.game_sub_user_name_cnt, source.products_array
        )
        ;
        """
        query_job = client.query(query)
        query_job.result()
        print(f"📊 처리된 행 개수(Insert/Update): {query_job.num_dml_affected_rows}")
        print(f"■ {start_date} f_user_map Batch 완료")

    print("✅ f_user_map ETL 완료")
    return True


def etl_f_user_map_char(target_date: list, client):

    for td in target_date:
        start_date = datetime.strptime(td, "%Y-%m-%d")
        end_date = start_date + timedelta(days=1)
        start_date = start_date.strftime("%Y-%m-%d")
        end_date = end_date.strftime("%Y-%m-%d")
        print(f"📝 시작시간 : {start_date}  📝 종료시간 : {end_date}")

        query = f"""
        MERGE datahub-478802.datahub.f_user_map_char as target
        USING
        (
            SELECT A.datekey, A.joyple_game_code, A.auth_account_name, A.auth_method_id,
            A.game_sub_user_name, A.RU, B.game_id, B.world_id, B.server_name,
            B.country_code, B.market_id, B.os_id, B.platform_device_type,
            B.tracker_type_id, B.tracker_account_id, B.game_sub_user_reg_datekey,
            DATE_DIFF(A.datekey, B.game_sub_user_reg_datekey, DAY) as datediff_sub_user_reg,
            B.game_sub_user_reg_datetime, B.reg_datekey,
            DATE_DIFF(A.datekey, B.reg_datekey, DAY) as datediff_reg,
            C.daily_total_rev, C.daily_buy_cnt, C.daily_pu, C.stacked_rev,
            C.stacked_buy_cnt, C.stacked_rev_percent_rank, C.monthly_rev,
            CASE WHEN C.monthly_rev > 10000000 then '1.R0' WHEN C.monthly_rev > 1000000 then '2.R1'
                    WHEN C.monthly_rev > 100000 then '3.R2' WHEN C.monthly_rev > 10000 then '4.R3'
                    WHEN C.monthly_rev > 0 then '5.R4' ELSE '6.Non_pu' END as monthly_rgroup_sub_user,
            C.before_monthly_rev,
            CASE WHEN FORMAT_DATE('%Y-%m', B.game_sub_user_reg_datekey) = FORMAT_DATE('%Y-%m', A.datekey) then '7.RU'
                    WHEN C.before_monthly_rev > 10000000 then '1.R0' WHEN C.before_monthly_rev > 1000000 then '2.R1'
                    WHEN C.before_monthly_rev > 100000 then '3.R2' WHEN C.before_monthly_rev > 10000 then '4.R3'
                    WHEN C.before_monthly_rev > 0 then '5.R4' ELSE '6.Non_pu' END as before_monthly_rgroup_sub_user,
            C.yearly_rev, C.last_30days_rev, C.first_payment_datekey, C.last_payment_datekey,
            D.stacked_IAA_rev, D.daily_IAA_rev, E.play_seconds, E.access_cnt,
            H.max_game_user_level, H.last_login_datekey,
            DATE_DIFF(A.datekey, H.last_login_datekey, DAY) as datediff_last_login_sub_user,
            E.stickiness, F.products_array, G.first_tracking_country_code, G.first_tracking_datekey,
            G.app_id, G.media_source, G.bundle_id, G.is_organic, G.agency, G.campaign,
            G.init_campaign, G.adset_name, G.ad_name, G.is_retargeting, G.advertising_id,
            G.idfa, G.site_id, G.channel, G.CB1_media_source, G.CB1_campaign,
            G.CB2_media_source, G.CB2_campaign, G.CB3_media_source, G.CB3_campaign
                FROM (
                    SELECT datekey, joyple_game_code, auth_account_name, game_sub_user_name, MIN(auth_method_id) as auth_method_id,
                        MAX(IF(datekey = reg_datekey, 1, 0)) AS RU
                    FROM `datahub-478802.datahub.f_common_access`
                    WHERE datekey >= '{start_date}' and datekey < '{end_date}' AND access_type_id = 1
                    GROUP BY 1,2,3,4
                ) AS A
                LEFT OUTER JOIN
                    (SELECT * FROM `datahub-478802.datahub.f_common_register_char` WHERE game_sub_user_reg_datekey < '{end_date}') AS B
                ON A.joyple_game_code = B.joyple_game_code AND A.auth_account_name = B.auth_account_name AND COALESCE(A.game_sub_user_name, 'dafault') = COALESCE(B.game_sub_user_name, 'dafault')
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
                        min(datekey) as first_payment_datekey,
                        MAX(IF(datekey < '{start_date}', datekey, null)) as last_payment_datekey
                    FROM `datahub-478802.datahub.f_common_payment` WHERE datekey < '{end_date}'
                    GROUP BY 1,2,3
                ) AS C ON A.joyple_game_code = C.joyple_game_code AND A.auth_account_name = C.auth_account_name AND COALESCE(A.game_sub_user_name, 'dafault') = COALESCE(C.game_sub_user_name, 'dafault')
                LEFT OUTER JOIN (
                    SELECT joyple_game_code, auth_account_name, game_sub_user_name,
                            SUM(revenue_per_user_KRW) as stacked_IAA_rev,
                            SUM(IF(watch_datekey >= '{start_date}' AND watch_datekey < '{end_date}', revenue_per_user_KRW, 0)) as daily_IAA_rev
                    FROM `datahub-478802.datahub.f_IAA_auth_account_performance_joyple` WHERE watch_datekey < '{end_date}'
                    GROUP BY 1,2,3
                ) AS D ON A.joyple_game_code = D.joyple_game_code AND A.auth_account_name = D.auth_account_name AND COALESCE(A.game_sub_user_name, 'dafault') = COALESCE(D.game_sub_user_name, 'dafault')
                LEFT OUTER JOIN (
                    SELECT joyple_game_code, auth_account_name, game_sub_user_name,
                        SUM(IF(datekey >= '{start_date}' AND datekey < '{end_date}', play_seconds, 0)) as play_seconds,
                        SUM(IF(datekey >= '{start_date}' AND datekey < '{end_date}', access_cnt, 0)) as access_cnt,
                        MAX(game_user_level) as max_game_user_level,
                        MAX(IF(datekey < '{start_date}' AND access_type_id = 1, datekey, null)) as last_login_datekey,
                        COUNT(DISTINCT(IF(datekey >= DATE_SUB('{start_date}', INTERVAL 6 DAY) AND datekey < '{end_date}' AND access_type_id = 1, datekey, null))) as stickiness
                    FROM `datahub-478802.datahub.f_common_access` WHERE datekey >= DATE_SUB('{start_date}', INTERVAL 6 DAY) AND datekey < '{end_date}'
                    GROUP BY 1,2,3
                ) AS E ON A.joyple_game_code = E.joyple_game_code AND A.auth_account_name = E.auth_account_name AND COALESCE(A.game_sub_user_name, 'dafault') = COALESCE(E.game_sub_user_name, 'dafault')
                LEFT OUTER JOIN (
                    SELECT joyple_game_code, auth_account_name, game_sub_user_name,
                        ARRAY_AGG(STRUCT(pg_id, platform_device_type, product_code, product_name, revenue, buy_cnt) ORDER BY datekey ASC) as products_array
                    FROM `datahub-478802.datahub.f_common_payment` WHERE datekey >= '{start_date}' and datekey < '{end_date}'
                    GROUP BY 1,2,3
                ) AS F ON A.joyple_game_code = F.joyple_game_code AND A.auth_account_name = F.auth_account_name AND COALESCE(A.game_sub_user_name, 'dafault') = COALESCE(F.game_sub_user_name, 'dafault')
                LEFT OUTER JOIN (SELECT * FROM `datahub-478802.datahub.f_common_register` WHERE reg_datekey < '{end_date}') AS G
                    ON A.joyple_game_code = G.joyple_game_code AND A.auth_account_name = G.auth_account_name
                LEFT OUTER JOIN (
                    SELECT joyple_game_code, auth_account_name, game_sub_user_name,
                        MAX(last_login_datekey) as last_login_datekey, MAX(max_game_user_level) as max_game_user_level
                    FROM `datahub-478802.datahub.f_common_access_last_login` GROUP BY 1,2,3
                ) AS H ON A.joyple_game_code = H.joyple_game_code AND A.auth_account_name = H.auth_account_name AND COALESCE(A.game_sub_user_name, 'dafault') = COALESCE(H.game_sub_user_name, 'dafault')
        ) as source
        ON target.datekey = source.datekey
        AND target.joyple_game_code = source.joyple_game_code
        AND target.auth_account_name = source.auth_account_name
        AND target.game_sub_user_name = source.game_sub_user_name
        WHEN NOT MATCHED BY target THEN
        INSERT
        (
                    datekey, joyple_game_code, auth_account_name, auth_method_id,
                    game_sub_user_name, RU, game_id, world_id, server_name,
                    reg_country_code, reg_market_id, reg_os_id, platform_device_type,
                    tracker_type_id, tracker_account_id, game_sub_user_reg_datekey,
                    datediff_sub_user_reg, game_sub_user_reg_datetime, user_reg_datekey,
                    datediff_reg, daily_total_rev, daily_buy_cnt, daily_pu, stacked_rev,
                    stacked_buy_cnt, stacked_rev_percent_rank, monthly_rev,
                    monthly_rgroup_sub_user, before_monthly_rev, before_monthly_rgroup_sub_user,
                    yearly_rev, last_30days_rev, first_payment_datekey, last_payment_datekey,
                    stacked_IAA_rev, daily_IAA_rev, play_seconds, access_cnt,
                    max_game_user_level, last_login_datekey, datediff_last_login_sub_user,
                    stickiness, products_array, first_tracking_country_code, first_tracking_datekey,
                    app_id, media_source, bundle_id, is_organic, agency, campaign, init_campaign,
                    adset_name, ad_name, is_retargeting, advertising_id, idfa, site_id, channel,
                    CB1_media_source, CB1_campaign, CB2_media_source, CB2_campaign,
                    CB3_media_source, CB3_campaign
        )
        VALUES
        (
                    source.datekey, source.joyple_game_code, source.auth_account_name,
                    source.auth_method_id, source.game_sub_user_name, source.RU,
                    source.game_id, source.world_id, source.server_name,
                    source.country_code, source.market_id, source.os_id,
                    source.platform_device_type, source.tracker_type_id, source.tracker_account_id,
                    source.game_sub_user_reg_datekey, source.datediff_sub_user_reg,
                    source.game_sub_user_reg_datetime, source.reg_datekey, source.datediff_reg,
                    source.daily_total_rev, source.daily_buy_cnt, source.daily_pu,
                    source.stacked_rev, source.stacked_buy_cnt, source.stacked_rev_percent_rank,
                    source.monthly_rev, source.monthly_rgroup_sub_user, source.before_monthly_rev,
                    source.before_monthly_rgroup_sub_user, source.yearly_rev, source.last_30days_rev,
                    source.first_payment_datekey, source.last_payment_datekey,
                    source.stacked_IAA_rev, source.daily_IAA_rev, source.play_seconds,
                    source.access_cnt, source.max_game_user_level, source.last_login_datekey,
                    source.datediff_last_login_sub_user, source.stickiness, source.products_array,
                    source.first_tracking_country_code, source.first_tracking_datekey, source.app_id,
                    source.media_source, source.bundle_id, source.is_organic, source.agency,
                    source.campaign, source.init_campaign, source.adset_name, source.ad_name,
                    source.is_retargeting, source.advertising_id, source.idfa, source.site_id,
                    source.channel, source.CB1_media_source, source.CB1_campaign,
                    source.CB2_media_source, source.CB2_campaign, source.CB3_media_source,
                    source.CB3_campaign
        )
        """
        query_job = client.query(query)
        query_job.result()
        print(f"📊 처리된 행 개수(Insert/Update): {query_job.num_dml_affected_rows}")
        print(f"■ {start_date} f_user_map_char Batch 완료")

    print("✅ f_user_map_char ETL 완료")
    return True


def etl_pre_f_user_map_cohort(execution_date: str, client):
    cutoff_date = (datetime.strptime(execution_date, "%Y-%m-%d") - timedelta(days=515)).strftime("%Y-%m-%d")

    print(f"■ pre_f_user_map_cohort Batch 시작 (기준일: {execution_date}, 데이터 시작일: {cutoff_date})")

    delete_query = f"""
    DELETE FROM `datahub-478802.datahub.pre_f_user_map_cohort`
    WHERE reg_datekey >= '{cutoff_date}'
    """
    delete_job = client.query(delete_query)
    delete_job.result()
    print(f"🗑️ DELETE 완료 (reg_datekey >= {cutoff_date}), 삭제 행: {delete_job.num_dml_affected_rows}")

    insert_query = f"""
    INSERT INTO `datahub-478802.datahub.pre_f_user_map_cohort`
    WITH perforaw AS (
        SELECT joyple_game_code, reg_datekey, app_id,
               gcat, media_category, media, media_source, media_detail, product_category, etc_category, optim,
               reg_country_code, geo_cam, reg_market_id, reg_os_id, os_cam,
               adset_name, ad_name, site_id, agency, target_group,
               device, setting_title, landing_title, ad_unit, mediation, init_campaign, uptdt_campaign, campaign, class,
               count(distinct if(datediff_reg = 0, auth_account_name, null)) as ru,
               sum(if(datediff_reg between 0 and 0,   daily_total_rev, 0)) as rev_d0,
               sum(if(datediff_reg between 0 and 1,   daily_total_rev, 0)) as rev_d1,
               sum(if(datediff_reg between 0 and 3,   daily_total_rev, 0)) as rev_d3,
               sum(if(datediff_reg between 0 and 7,   daily_total_rev, 0)) as rev_d7,
               sum(if(datediff_reg between 0 and 14,  daily_total_rev, 0)) as rev_d14,
               sum(if(datediff_reg between 0 and 30,  daily_total_rev, 0)) as rev_d30,
               sum(if(datediff_reg between 0 and 60,  daily_total_rev, 0)) as rev_d60,
               sum(if(datediff_reg between 0 and 90,  daily_total_rev, 0)) as rev_d90,
               sum(if(datediff_reg between 0 and 120, daily_total_rev, 0)) as rev_d120,
               sum(if(datediff_reg between 0 and 150, daily_total_rev, 0)) as rev_d150,
               sum(if(datediff_reg between 0 and 180, daily_total_rev, 0)) as rev_d180,
               sum(if(datediff_reg between 0 and 210, daily_total_rev, 0)) as rev_d210,
               sum(if(datediff_reg between 0 and 240, daily_total_rev, 0)) as rev_d240,
               sum(if(datediff_reg between 0 and 270, daily_total_rev, 0)) as rev_d270,
               sum(if(datediff_reg between 0 and 300, daily_total_rev, 0)) as rev_d300,
               sum(if(datediff_reg between 0 and 330, daily_total_rev, 0)) as rev_d330,
               sum(if(datediff_reg between 0 and 360, daily_total_rev, 0)) as rev_d360,
               sum(if(datediff_reg between 0 and 390, daily_total_rev, 0)) as rev_d390,
               sum(if(datediff_reg between 0 and 420, daily_total_rev, 0)) as rev_d420,
               sum(if(datediff_reg between 0 and 450, daily_total_rev, 0)) as rev_d450,
               sum(if(datediff_reg between 0 and 480, daily_total_rev, 0)) as rev_d480,
               sum(if(datediff_reg between 0 and 510, daily_total_rev, 0)) as rev_d510,
               sum(if(datediff_reg between 0 and 0,   daily_IAA_rev, 0)) as rev_iaa_d0,
               sum(if(datediff_reg between 0 and 1,   daily_IAA_rev, 0)) as rev_iaa_d1,
               sum(if(datediff_reg between 0 and 3,   daily_IAA_rev, 0)) as rev_iaa_d3,
               sum(if(datediff_reg between 0 and 7,   daily_IAA_rev, 0)) as rev_iaa_d7,
               sum(if(datediff_reg between 0 and 14,  daily_IAA_rev, 0)) as rev_iaa_d14,
               sum(if(datediff_reg between 0 and 30,  daily_IAA_rev, 0)) as rev_iaa_d30,
               sum(if(datediff_reg between 0 and 60,  daily_IAA_rev, 0)) as rev_iaa_d60,
               sum(if(datediff_reg between 0 and 90,  daily_IAA_rev, 0)) as rev_iaa_d90,
               sum(if(datediff_reg between 0 and 120, daily_IAA_rev, 0)) as rev_iaa_d120,
               sum(if(datediff_reg between 0 and 150, daily_IAA_rev, 0)) as rev_iaa_d150,
               sum(if(datediff_reg between 0 and 180, daily_IAA_rev, 0)) as rev_iaa_d180,
               sum(if(datediff_reg between 0 and 210, daily_IAA_rev, 0)) as rev_iaa_d210,
               sum(if(datediff_reg between 0 and 240, daily_IAA_rev, 0)) as rev_iaa_d240,
               sum(if(datediff_reg between 0 and 270, daily_IAA_rev, 0)) as rev_iaa_d270,
               sum(if(datediff_reg between 0 and 300, daily_IAA_rev, 0)) as rev_iaa_d300,
               sum(if(datediff_reg between 0 and 330, daily_IAA_rev, 0)) as rev_iaa_d330,
               sum(if(datediff_reg between 0 and 360, daily_IAA_rev, 0)) as rev_iaa_d360,
               sum(if(datediff_reg between 0 and 390, daily_IAA_rev, 0)) as rev_iaa_d390,
               sum(if(datediff_reg between 0 and 420, daily_IAA_rev, 0)) as rev_iaa_d420,
               sum(if(datediff_reg between 0 and 450, daily_IAA_rev, 0)) as rev_iaa_d450,
               sum(if(datediff_reg between 0 and 480, daily_IAA_rev, 0)) as rev_iaa_d480,
               sum(if(datediff_reg between 0 and 510, daily_IAA_rev, 0)) as rev_iaa_d510,
               count(distinct if(datediff_reg = 1,   auth_account_name, null)) as ru_d1,
               count(distinct if(datediff_reg = 3,   auth_account_name, null)) as ru_d3,
               count(distinct if(datediff_reg = 7,   auth_account_name, null)) as ru_d7,
               count(distinct if(datediff_reg = 14,  auth_account_name, null)) as ru_d14,
               count(distinct if(datediff_reg = 30,  auth_account_name, null)) as ru_d30,
               count(distinct if(datediff_reg = 60,  auth_account_name, null)) as ru_d60,
               count(distinct if(datediff_reg = 90,  auth_account_name, null)) as ru_d90,
               count(distinct if(datediff_reg = 120, auth_account_name, null)) as ru_d120,
               count(distinct if(datediff_reg = 150, auth_account_name, null)) as ru_d150,
               count(distinct if(datediff_reg = 180, auth_account_name, null)) as ru_d180,
               count(distinct if(datediff_reg = 210, auth_account_name, null)) as ru_d210,
               count(distinct if(datediff_reg = 240, auth_account_name, null)) as ru_d240,
               count(distinct if(datediff_reg = 270, auth_account_name, null)) as ru_d270,
               count(distinct if(datediff_reg = 300, auth_account_name, null)) as ru_d300,
               count(distinct if(datediff_reg = 330, auth_account_name, null)) as ru_d330,
               count(distinct if(datediff_reg = 360, auth_account_name, null)) as ru_d360,
               count(distinct if(datediff_reg = 390, auth_account_name, null)) as ru_d390,
               count(distinct if(datediff_reg = 420, auth_account_name, null)) as ru_d420,
               count(distinct if(datediff_reg = 450, auth_account_name, null)) as ru_d450,
               count(distinct if(datediff_reg = 480, auth_account_name, null)) as ru_d480,
               count(distinct if(datediff_reg = 510, auth_account_name, null)) as ru_d510,
               count(distinct if(datediff_reg = 0   and daily_total_rev > 0, auth_account_name, null)) as pu_d0,
               count(distinct if(datediff_reg = 1   and daily_total_rev > 0, auth_account_name, null)) as pu_d1,
               count(distinct if(datediff_reg = 3   and daily_total_rev > 0, auth_account_name, null)) as pu_d3,
               count(distinct if(datediff_reg = 7   and daily_total_rev > 0, auth_account_name, null)) as pu_d7,
               count(distinct if(datediff_reg = 14  and daily_total_rev > 0, auth_account_name, null)) as pu_d14,
               count(distinct if(datediff_reg = 30  and daily_total_rev > 0, auth_account_name, null)) as pu_d30,
               count(distinct if(datediff_reg = 60  and daily_total_rev > 0, auth_account_name, null)) as pu_d60,
               count(distinct if(datediff_reg = 90  and daily_total_rev > 0, auth_account_name, null)) as pu_d90,
               count(distinct if(datediff_reg = 120 and daily_total_rev > 0, auth_account_name, null)) as pu_d120,
               count(distinct if(datediff_reg = 150 and daily_total_rev > 0, auth_account_name, null)) as pu_d150,
               count(distinct if(datediff_reg = 180 and daily_total_rev > 0, auth_account_name, null)) as pu_d180,
               count(distinct if(datediff_reg = 210 and daily_total_rev > 0, auth_account_name, null)) as pu_d210,
               count(distinct if(datediff_reg = 240 and daily_total_rev > 0, auth_account_name, null)) as pu_d240,
               count(distinct if(datediff_reg = 270 and daily_total_rev > 0, auth_account_name, null)) as pu_d270,
               count(distinct if(datediff_reg = 300 and daily_total_rev > 0, auth_account_name, null)) as pu_d300,
               count(distinct if(datediff_reg = 330 and daily_total_rev > 0, auth_account_name, null)) as pu_d330,
               count(distinct if(datediff_reg = 360 and daily_total_rev > 0, auth_account_name, null)) as pu_d360,
               count(distinct if(datediff_reg = 390 and daily_total_rev > 0, auth_account_name, null)) as pu_d390,
               count(distinct if(datediff_reg = 420 and daily_total_rev > 0, auth_account_name, null)) as pu_d420,
               count(distinct if(datediff_reg = 450 and daily_total_rev > 0, auth_account_name, null)) as pu_d450,
               count(distinct if(datediff_reg = 480 and daily_total_rev > 0, auth_account_name, null)) as pu_d480,
               count(distinct if(datediff_reg = 510 and daily_total_rev > 0, auth_account_name, null)) as pu_d510
        FROM `datahub-478802.datahub.f_user_map_mas_view` AS a
        WHERE datekey >= '{cutoff_date}'
          AND reg_datekey >= '{cutoff_date}'
          AND datediff_reg >= 0
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30
    ),
    UA_perfo AS (
        SELECT a.joyple_game_code, a.reg_datekey
               , a.app_id, a.media_source, a.uptdt_campaign
               , CASE WHEN a.media_source IN ('Unknown', 'NULL') THEN 'Unknown'
                      WHEN a.campaign LIKE '%Pirates of the Caribbean Android AU%' THEN 'UA'
                      WHEN a.campaign LIKE '%Pirates of the Caribbean Android KR%' THEN 'UA'
                      WHEN a.campaign LIKE '%Pirates of the Caribbean Android US%' THEN 'UA'
                      WHEN a.campaign LIKE '%Pirates of the Caribbean Android GB%' THEN 'UA'
                      WHEN a.campaign = 'POTC_検索' THEN 'UA'
                      WHEN a.gcat IS NULL AND a.joyple_game_code = 131 THEN d.gcat
                      ELSE a.gcat
                 END AS gcat
               , CASE WHEN a.campaign LIKE '%Pirates of the Caribbean Android AU%' THEN 'ADNW'
                      WHEN a.campaign LIKE '%Pirates of the Caribbean Android KR%' THEN 'ADNW'
                      WHEN a.campaign LIKE '%Pirates of the Caribbean Android US%' THEN 'ADNW'
                      WHEN a.campaign LIKE '%Pirates of the Caribbean Android GB%' THEN 'ADNW'
                      WHEN a.campaign = 'POTC_検索' THEN 'ADNW'
                      WHEN a.gcat IS NULL AND a.joyple_game_code = 131 THEN d.media_category
                      ELSE a.media_category
                 END AS media_category
               , CASE WHEN a.optim = 'NONE' AND a.adset_name LIKE '%MAIA%' THEN 'MAIA'
                      WHEN a.optim = 'NONE' AND a.adset_name LIKE '%AEO%'  THEN 'AEO'
                      WHEN a.optim = 'NONE' AND a.adset_name LIKE '%VO%'   THEN 'VO'
                      ELSE a.optim
                 END AS optim
               , a.* EXCEPT(joyple_game_code, reg_datekey, app_id, media_source, uptdt_campaign, gcat, media_category, optim)
        FROM (
            SELECT a.*, b.market_name_KR AS reg_market_name, o.os_name_lower AS reg_os_name
            FROM perforaw AS a
            LEFT JOIN `datahub-478802.datahub.dim_market_id` AS b ON a.reg_market_id = b.market_id
            LEFT JOIN `datahub-478802.datahub.dim_os_id`    AS o ON a.reg_os_id    = o.os_id
        ) AS a
        LEFT JOIN `data-science-division-216308.POTC.before_mas_campaign` AS d
            ON a.uptdt_campaign = d.campaign
    )
    SELECT * FROM UA_perfo
    """
    insert_job = client.query(insert_query)
    insert_job.result()
    print(f"📊 INSERT 완료, 적재 행: {insert_job.num_dml_affected_rows}")
    print(f"■ pre_f_user_map_cohort Batch 완료")
    print("✅ pre_f_user_map_cohort ETL 완료")
    return True