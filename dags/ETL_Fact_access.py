from google.cloud import bigquery
from datetime import datetime, timezone, timedelta
import time
import os
import pytz


# 빅쿼리 클라이언트 연결
client = bigquery.Client()

def etl_f_common_register(target_date:list):

    for td in target_date:
        target_date = td

        # KST 00:00:00 ~ 23:59:59를 UTC로 변환
        kst = pytz.timezone('Asia/Seoul')
        start_utc = target_date.replace(tzinfo=kst).astimezone(pytz.UTC)
        end_utc = (target_date + timedelta(days=1)).replace(tzinfo=kst).astimezone(pytz.UTC)
        print(f"📝 시작시간 : ", start_utc, f" 📝 종료시간 : ", end_utc)

        # KST 00:00:00 ~ 23:59:59를 UTC로 변환
        query = f"""
        MERGE `datahub-478802.datahub.f_common_register` AS target
        USING
        (
            with TA as (
                SELECT a.GameID
                    , a.WorldID
                    , a.ServerName
                    , a.JoypleGameID
                    , a.AuthMethodID
                    , a.AuthAccountName
                    , a.GameAccountName
                    , a.GameSubUserName
                    , a.DeviceAccountName
                    , a.TrackerAccountName
                    , a.TrackerTypeID
                    , a.MarketID
                    , a.OSID
                    , a.PlatformDeviceType
                    , a.AccountLevel
                    , a.IP
                    , a.AccessTypeID
                    , a.PlaySeconds
                    , a.LogTime
                FROM `dataplatform-reporting.DataService.V_0160_0000_AccessLog_V` AS a
                WHERE a.LogTime >= '{start_utc}'
                AND a.LogTime < '{end_utc}'
                AND a.AccessTypeID = 1
            )
            , TB as (
                SELECT
                    DATE(Info.LogTime, "Asia/Seoul")      AS reg_datekey
                    , DATETIME(Info.LogTime, "Asia/Seoul")  AS reg_datetime
                    , Info.GameID               AS game_id
                    , Info.WorldID              AS world_id
                    , JoypleGameID              AS joyple_game_code
                    , Info.AuthMethodID         AS auth_method_id
                    , AuthAccountName           AS auth_account_name
                    , Info.TrackerAccountName   AS tracker_account_id
                    , Info.TrackerTypeID        AS tracker_type_id
                    , Info.DeviceAccountName    AS device_id
                    , b.country_code            AS country_code
                    , Info.MarketID             AS market_id
                    , Info.OSID                 AS os_id
                    , Info.PlatformDeviceType   AS platform_device_type
                FROM (
                    SELECT a.JoypleGameID
                        , a.AuthAccountName
                        -- 여기서 로그 중복 제거 (계정당 1개 로그만 선택)
                        , ARRAY_AGG(STRUCT(a.GameID, a.AuthMethodID, a.WorldID, a.TrackerTypeID, a.MarketID, a.OSID, a.IP, a.PlatformDeviceType, a.TrackerAccountName, a.DeviceAccountName, a.LogTime) ORDER BY a.LogTime ASC LIMIT 1)[OFFSET(0)] AS Info
                    FROM (
                        SELECT
                            a.GameID , a.WorldID , a.JoypleGameID , a.AuthMethodID , a.AuthAccountName
                            , a.TrackerAccountName , a.TrackerTypeID , a.DeviceAccountName , a.PlatformDeviceType
                            , a.MarketID , a.OSID , a.IP , a.LogTime
                        FROM TA AS a
                        WHERE a.AccessTypeID = 1
                        AND a.GameID IS NOT NULL
                        -- (NULL 체크 조건들 생략 가능하지만 유지)
                        AND a.AuthAccountName IS NOT NULL
                    ) AS a
                    GROUP BY a.JoypleGameID, a.AuthAccountName
                ) AS a
                LEFT OUTER JOIN datahub-478802.datahub.dim_ip4_country_code AS b
                ON (a.Info.IP = b.ip)
            )
            , TC as (
                SELECT
                TB.reg_datekey,
                TB.reg_datetime,
                TB.game_id,
                TB.world_id,
                TB.joyple_game_code,
                TB.auth_method_id,
                TB.auth_account_name,
                TB.tracker_account_id,
                TB.tracker_type_id,
                TB.device_id,
                TB.country_code as reg_country_code,
                TB.market_id,
                TB.os_id,
                TB.platform_device_type,
                aa.app_id,
                aa.bundle_id,
                aa.country_code as install_country_code,
                aa.media_source,
                aa.media_source_cat,
                if(J.auth_account_name is not null, 'Non-Organic',aa.is_organic) as is_organic,
                aa.agency,
                coalesce(J.Campaign_name,aa.campaign)                        as campaign,
                coalesce(J.Campaign_name,aa.init_campaign)                   as init_campaign,
                aa.adset_name,
                coalesce(cast(J.ad_name as string),aa.ad_name)               as ad_name,
                aa.is_retargeting,
                aa.advertising_id,
                aa.idfa,
                aa.site_id,
                aa.channel,
                aa.CB1_media_source,
                aa.CB1_campaign,
                aa.CB2_media_source,
                aa.CB2_campaign,
                aa.CB3_media_source,
                aa.CB3_campaign,
                aa.install_time,
                aa.event_time,
                aa.event_type,
                aa.install_datekey
                FROM TB
                -- [위험 구간 1] 트래커 데이터가 중복될 경우 여기서 행이 늘어남
                LEFT JOIN datahub-478802.datahub.f_tracker_install as aa
                ON TB.tracker_account_id = aa.tracker_account_id AND TB.tracker_type_id = aa.tracker_type_id
                -- [위험 구간 2] 조이 트래킹 데이터가 중복될 경우 여기서 행이 늘어남
                LEFT JOIN (
                    SELECT a.*, b.* except(campaign_name, joyple_game_code)
                    FROM `datahub-478802.datahub.pre_joytracking_tracker` as a
                    LEFT JOIN `datahub-478802.datahub.dim_pccampaign_list_joytracking` as b
                    ON a.campaign_name = b.campaign_name AND a.joyple_game_code = b.joyple_game_code) as J
                ON TB.joyple_game_code = j.joyple_game_code AND TB.auth_account_name = j.auth_account_name
            )

            -- [최종 수정] 여기서 중복 제거 수행
            SELECT * FROM TC
            QUALIFY ROW_NUMBER() OVER(
                PARTITION BY joyple_game_code, auth_account_name 
                ORDER BY install_time DESC, reg_datetime DESC
            ) = 1

        ) AS source ON target.joyple_game_code = source.joyple_game_code AND CAST(target.auth_account_name AS STRING) = CAST(source.auth_account_name AS STRING)

        WHEN MATCHED AND (target.campaign <> source.campaign OR target.media_source <> source.media_source)
                    AND source.app_id IS NOT NULL THEN
        UPDATE SET
            target.app_id = source.app_id
            , target.bundle_id = source.bundle_id
            , target.install_country_code = source.install_country_code
            , target.media_source = source.media_source
            , target.media_source_cat = source.media_source_cat
            , target.is_organic = source.is_organic
            , target.agency = source.agency
            , target.campaign = source.campaign
            , target.init_campaign = source.init_campaign
            , target.adset_name = source.adset_name
            , target.ad_name = source.ad_name
            , target.is_retargeting = source.is_retargeting
            , target.advertising_id = source.advertising_id
            , target.idfa = source.idfa
            , target.site_id = source.site_id
            , target.channel = source.channel
            , target.CB1_media_source = source.CB1_media_source
            , target.CB1_campaign = source.CB1_campaign
            , target.CB2_media_source = source.CB2_media_source
            , target.CB2_campaign = source.CB2_campaign
            , target.CB3_media_source = source.CB3_media_source
            , target.CB3_campaign = source.CB3_campaign
            , target.install_time = source.install_time
            , target.event_time = source.event_time
            , target.event_type = source.event_type
            , target.install_datekey = source.install_datekey

        WHEN NOT MATCHED BY target THEN
        INSERT(
            reg_datekey,
            reg_datetime,
            game_id,
            world_id,
            joyple_game_code,
            auth_method_id,
            auth_account_name,
            tracker_account_id,
            tracker_type_id,
            device_id,
            reg_country_code,
            market_id,
            os_id,
            platform_device_type,
            app_id,
            bundle_id,
            install_country_code,
            media_source,
            media_source_cat,
            is_organic,
            agency,
            campaign,
            init_campaign,
            adset_name,
            ad_name,
            is_retargeting,
            advertising_id,
            idfa,
            site_id,
            channel,
            CB1_media_source,
            CB1_campaign,
            CB2_media_source,
            CB2_campaign,
            CB3_media_source,
            CB3_campaign,
            install_time,
            event_time,
            event_type,
            install_datekey
            )
        VALUES(
            source.reg_datekey,
            source.reg_datetime,
            source.game_id,
            source.world_id,
            source.joyple_game_code,
            source.auth_method_id,
            source.auth_account_name,
            source.tracker_account_id,
            source.tracker_type_id,
            source.device_id,
            source.reg_country_code,
            source.market_id,
            source.os_id,
            source.platform_device_type,
            source.app_id,
            source.bundle_id,
            source.install_country_code,
            source.media_source,
            source.media_source_cat,
            source.is_organic,
            source.agency,
            source.campaign,
            source.init_campaign,
            source.adset_name,
            source.ad_name,
            source.is_retargeting,
            source.advertising_id,
            source.idfa,
            source.site_id,
            source.channel,
            source.CB1_media_source,
            source.CB1_campaign,
            source.CB2_media_source,
            source.CB2_campaign,
            source.CB3_media_source,
            source.CB3_campaign,
            source.install_time,
            source.event_time,
            source.event_type,
            source.install_datekey
            );
            """
        
        client.query(query)
        print(f"■ {target_date.strftime('%Y-%m-%d')} f_common_register Batch 완료")
    
    print("✅ f_common_register ETL 완료")
    return True


def adjust_f_common_register(target_date:list):

    for td in target_date:
        target_date = td

        # KST 00:00:00 ~ 23:59:59를 UTC로 변환
        kst = pytz.timezone('Asia/Seoul')
        start_utc = target_date.replace(tzinfo=kst).astimezone(pytz.UTC)
        end_utc = (target_date + timedelta(days=1)).replace(tzinfo=kst).astimezone(pytz.UTC)
        print(f"📝 시작시간 : ", start_utc, f" 📝 종료시간 : ", end_utc)

        query = f"""
        MERGE datahub-478802.datahub.f_common_register AS target
        USING 
        (
            select DATE(TA.INFO.log_time, "Asia/Seoul") as reg_datekey
            , DATETIME(TA.INFO.log_time, "Asia/Seoul") as reg_datetime
            , TA.INFO.game_id
            , TA.INFO.world_id
            , TA.joyple_game_code
            , TA.INFO.auth_method_id
            , TA.auth_account_name
            , TA.INFO.tracker_account_id
            , TA.INFO.mmp_type as tracker_type_id
            , TA.INFO.device_id
            , TC.country_code as reg_country_code
            , TA.INFO.market_id
            , TA.INFO.os_id
            , TA.INFO.platform_device_type
            , TA.INFO.app_id
            , TB.bundle_id
            , TC.country_code as install_country_code
            , TB.media_source
            , TB.media_source_cat
            , if(J.auth_account_name is not null, 'Non-Organic',TB.is_organic) as is_organic
            , TB.agency
            , coalesce(J.Campaign_name,TB.campaign) as campaign
            , coalesce(J.Campaign_name,TB.init_campaign) as init_campaign
            , TB.adset_name
            , coalesce(cast(J.ad_name as string),TB.ad_name) as ad_name 
            , TB.is_retargeting
            , TB.advertising_id
            , TB.idfa
            , TB.site_id
            , TB.channel
            , TB.CB1_media_source
            , TB.CB1_campaign
            , TB.CB2_media_source
            , TB.CB2_campaign
            , TB.CB3_media_source
            , TB.CB3_campaign
            , TB.install_time
            , TB.event_time
            , TB.event_type
            , TB.install_datekey
            from
            (
                select joyple_game_code, auth_account_name, 
                array_agg(STRUCT(game_id, world_id, auth_method_id, auth_account_name, tracker_account_id, mmp_type, device_id, ip, market_id, os_id, platform_device_type, 
                app_id, log_time) ORDER BY log_time asc)[OFFSET(0)] AS INFO
                from dataplatform-204306.CommonLog.Payment
                where log_time >= '{start_utc}'
                and log_time < '{end_utc}'
                group by joyple_game_code, auth_account_name
            ) TA
            left join 
            datahub-478802.datahub.f_tracker_install as TB
            on TA.INFO.tracker_account_id = TB.tracker_account_id AND TA.INFO.mmp_type = TB.tracker_type_id
            left join 
            datahub-478802.datahub.dim_ip4_country_code as TC
            on TA.INFO.ip = TC.ip
            LEFT JOIN ( -- 조이트래킹 서비스 종료할때까지만 반영하면됨.
                       SELECT a.*, b.* except(campaign_name, joyple_game_code)
                       FROM `datahub-478802.datahub.pre_joytracking_tracker` as a
                       LEFT JOIN `datahub-478802.datahub.dim_pccampaign_list_joytracking` as b
                       ON a.campaign_name = b.campaign_name AND a.joyple_game_code = b.joyple_game_code) as J
            ON TA.joyple_game_code = j.joyple_game_code AND TA.auth_account_name = j.auth_account_name
        ) AS source ON target.joyple_game_code = source.joyple_game_code AND target.auth_account_name = source.auth_account_name
        WHEN NOT MATCHED BY target THEN 
        INSERT
        (
            reg_datekey
            , reg_datetime
            , game_id
            , world_id
            , joyple_game_code
            , auth_method_id
            , auth_account_name
            , tracker_account_id
            , tracker_type_id
            , device_id
            , reg_country_code
            , market_id
            , os_id
            , platform_device_type
            , app_id
            , bundle_id
            , install_country_code
            , media_source
            , media_source_cat
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
            , install_time
            , event_time
            , event_type
            , install_datekey
        )
        VALUES
        (
            source.reg_datekey
            , source.reg_datetime
            , source.game_id
            , source.world_id
            , source.joyple_game_code
            , source.auth_method_id
            , source.auth_account_name
            , source.tracker_account_id
            , source.tracker_type_id
            , source.device_id
            , source.reg_country_code
            , source.market_id
            , source.os_id
            , source.platform_device_type
            , source.app_id
            , source.bundle_id
            , source.install_country_code
            , source.media_source
            , source.media_source_cat
            , source.is_organic
            , source.agency
            , source.campaign
            , source.init_campaign
            , source.adset_name
            , source.ad_name
            , source.is_retargeting
            , source.advertising_id
            , source.idfa
            , source.site_id
            , source.channel
            , source.CB1_media_source
            , source.CB1_campaign
            , source.CB2_media_source
            , source.CB2_campaign
            , source.CB3_media_source
            , source.CB3_campaign
            , source.install_time
            , source.event_time
            , source.event_type
            , source.install_datekey
        )
        WHEN MATCHED AND (target.reg_datekey > source.reg_datekey)
        THEN
        UPDATE SET 
        target.reg_datekey = source.reg_datekey;
        """
        client.query(query)
        print(f"■ {target_date.strftime('%Y-%m-%d')} f_common_register_adjust Batch 완료")
    
    print("✅ f_common_register_adjust ETL 완료")
    return True



def etl_f_common_register_char(target_date:list):

    for td in target_date:
        target_date = td

        # KST 00:00:00 ~ 23:59:59를 UTC로 변환
        kst = pytz.timezone('Asia/Seoul')
        start_utc = target_date.replace(tzinfo=kst).astimezone(pytz.UTC)
        end_utc = (target_date + timedelta(days=1)).replace(tzinfo=kst).astimezone(pytz.UTC)
        print(f"📝 시작시간 : ", start_utc, f" 📝 종료시간 : ", end_utc)

        query = f"""
        MERGE `datahub-478802.datahub.f_common_register_char` AS target
        USING
        (
        with TA as (
        SELECT a.GameID
            , a.WorldID
            , a.ServerName
            , a.JoypleGameID
            , a.AuthMethodID
            , a.AuthAccountName
            , a.GameAccountName
            , a.GameSubUserName
            , a.DeviceAccountName
            , a.TrackerAccountName
            , a.TrackerTypeID
            , a.MarketID
            , a.OSID
            , a.PlatformDeviceType
            , a.AccountLevel
            , a.IP
            , a.AccessTypeID
            , a.PlaySeconds
            , a.LogTime
        FROM `dataplatform-reporting.DataService.V_0160_0000_AccessLog_V` AS a
        WHERE a.LogTime >= '{start_utc}'
        AND a.LogTime < '{end_utc}'
        AND a.AccessTypeID = 1
        )
            SELECT 
                Info.GameID AS game_id
                , Info.WorldID AS world_id
                , JoypleGameID AS joyple_game_code
                , Info.AuthMethodID AS auth_method_id
                , AuthAccountName AS auth_account_name
                , GameAccountName AS game_account_name
                , GameSubUserName AS game_sub_user_name
                , Info.ServerName AS server_name
                , FirstTrackerAccountID AS tracker_account_id
                , Info.TrackerTypeID AS tracker_type_id
                , FirstDeviceID AS device_id
                , b.country_code AS country_code
                , Info.MarketID AS market_id
                , Info.OSID AS os_id
                , Info.PlatformDeviceType AS platform_device_type
                , DATE(Info.LogTime, "Asia/Seoul") AS game_sub_user_reg_datekey
                , DATETIME(Info.LogTime, "Asia/Seoul") AS game_sub_user_reg_datetime
                , c.reg_datekey
                , c.reg_datetime
            FROM (
                SELECT a.JoypleGameID
                    , a.AuthAccountName
                    , a.GameAccountName
                    , a.GameSubUserName
                    , ARRAY_AGG(STRUCT(a.GameID, a.AuthMethodID, a.WorldID, a.ServerName, a.TrackerTypeID, a.MarketID, a.OSID, a.IP, a.PlatformDeviceType, a.LogTime) ORDER BY a.LogTime ASC LIMIT 1)[OFFSET(0)] AS Info
                    , ARRAY_AGG(a.TrackerAccountName IGNORE NULLS ORDER BY a.LogTime ASC LIMIT 1)[OFFSET(0)] AS FirstTrackerAccountID
                    , ARRAY_AGG(a.DeviceAccountName IGNORE NULLS ORDER BY a.LogTime ASC LIMIT 1)[OFFSET(0)] AS FirstDeviceID
                FROM (
                    SELECT a.GameID, a.WorldID, a.JoypleGameID, a.AuthMethodID, a.AuthAccountName,
                        a.TrackerAccountName, a.TrackerTypeID, a.DeviceAccountName, a.GameAccountName,
                        a.GameSubUserName, a.ServerName, a.PlatformDeviceType, a.MarketID, a.OSID, a.IP, a.LogTime
                    FROM TA AS a
                    WHERE a.LogTime >= '{start_utc}'
                    AND a.LogTime < '{end_utc}'
                    AND a.AccessTypeID = 1
                    AND a.GameID IS NOT NULL
                    AND a.WorldID           IS NOT NULL
                    AND a.ServerName        IS NOT NULL
                    AND a.AuthMethodID      IS NOT NULL
                    AND a.AuthAccountName   IS NOT NULL
                    AND a.GameSubUserName   IS NOT NULL
                    AND a.GameSubUserName   NOT IN ("", "0")
                    AND a.GameAccountName   IS NOT NULL
                    AND a.MarketID          IS NOT NULL
                    AND a.OSID              IS NOT NULL
                    AND a.AccountLevel      IS NOT NULL
                    AND a.IP                IS NOT NULL
                    AND a.AccessTypeID      IS NOT NULL
                    AND a.LogTime           IS NOT NULL  
                ) AS a
                GROUP BY a.JoypleGameID
                    , a.AuthAccountName
                    , a.GameAccountName
                    , a.GameSubUserName
            ) AS a
            LEFT OUTER JOIN `datahub-478802.datahub.dim_ip4_country_code` AS b ON a.Info.IP = b.ip
            LEFT OUTER JOIN `datahub-478802.datahub.f_common_register` AS c 
            ON a.JoypleGameID = c.joyple_game_code AND CAST(a.AuthAccountName AS STRING) = CAST(c.auth_account_name AS STRING)) AS source
        ON target.joyple_game_code = source.joyple_game_code 
        AND target.auth_account_name = source.auth_account_name
        AND target.game_sub_user_name = source.game_sub_user_name
        WHEN NOT MATCHED THEN
            INSERT 
            (
            reg_datekey
            , reg_datetime
            , game_id
            , world_id
            , joyple_game_code
            , auth_method_id
            , auth_account_name
            , game_sub_user_name
            , server_name
            , tracker_account_id
            , tracker_type_id
            , device_id
            , country_code
            , market_id
            , os_id
            , platform_device_type
            , game_sub_user_reg_datekey
            , game_sub_user_reg_datetime
            )
            VALUES
            (
            source.reg_datekey
            , source.reg_datetime
            , source.game_id
            , source.world_id
            , source.joyple_game_code
            , source.auth_method_id
            , source.auth_account_name
            , source.game_sub_user_name
            , source.server_name
            , source.tracker_account_id
            , source.tracker_type_id
            , source.device_id
            , source.country_code
            , source.market_id
            , source.os_id
            , source.platform_device_type
            , source.game_sub_user_reg_datekey
            , source.game_sub_user_reg_datetime
            )
        """

        client.query(query)
        print(f"■ {target_date.strftime('%Y-%m-%d')} f_common_register_char Batch 완료")
    
    print("✅ f_common_register_char ETL 완료")
    return True


def adjust_f_common_register_char(target_date:list):

    for td in target_date:
        target_date = td

        # KST 00:00:00 ~ 23:59:59를 UTC로 변환
        kst = pytz.timezone('Asia/Seoul')
        start_utc = target_date.replace(tzinfo=kst).astimezone(pytz.UTC)
        end_utc = (target_date + timedelta(days=1)).replace(tzinfo=kst).astimezone(pytz.UTC)
        print(f"📝 시작시간 : ", start_utc, f" 📝 종료시간 : ", end_utc)

        query = f"""
        MERGE datahub-478802.datahub.f_common_register_char AS target
        USING 
        (
            select IFNULL(TB.reg_datekey, DATE(TA.INFO.log_time, "Asia/Seoul")) as reg_datekey
            , IFNULL(TB.reg_datetime, DATETIME(TA.INFO.log_time, "Asia/Seoul")) as reg_datetime
            , TA.INFO.game_id
            , TA.INFO.world_id
            , TA.joyple_game_code
            , TA.INFO.auth_method_id
            , TA.auth_account_name
            , TA.game_sub_user_name
            , TA.INFO.server_name
            , TA.INFO.tracker_account_id
            , TA.INFO.mmp_type as tracker_type_id
            , TA.INFO.device_id
            , TC.country_code as country_code
            , TA.INFO.market_id
            , TA.INFO.os_id
            , TA.INFO.platform_device_type
            , DATE(TA.INFO.log_time, "Asia/Seoul") as game_sub_user_reg_datekey
            , DATETIME(TA.INFO.log_time, "Asia/Seoul") as game_sub_user_reg_datetime            
            from
            (
                select joyple_game_code, auth_account_name, game_sub_user_name,
                array_agg(STRUCT(game_id, world_id, auth_method_id, auth_account_name, tracker_account_id, mmp_type, device_id, ip, market_id, os_id, platform_device_type, app_id, log_time, server_name) ORDER BY log_time asc)[OFFSET(0)] AS INFO
                from dataplatform-204306.CommonLog.Payment
                where log_time >= '{start_utc}'
                and log_time < '{end_utc}'
                group by joyple_game_code, auth_account_name, game_sub_user_name
            ) TA
            left join
            datahub-478802.datahub.f_common_register as TB
            on TA.joyple_game_code = TB.joyple_game_code AND CAST(TA.auth_account_name AS STRING) = CAST(TB.auth_account_name AS STRING)
            left join 
            datahub-478802.datahub.dim_ip4_country_code as TC
            on TA.INFO.ip = TC.ip
        ) AS source 
        ON target.joyple_game_code = source.joyple_game_code AND target.auth_method_id = source.auth_method_id 
        AND target.auth_account_name = source.auth_account_name AND target.game_sub_user_name = source.game_sub_user_name
        WHEN NOT MATCHED BY target THEN 
        INSERT
        (
            reg_datekey
            , reg_datetime
            , game_id
            , world_id
            , joyple_game_code
            , auth_method_id
            , auth_account_name
            , tracker_account_id
            , tracker_type_id
            , device_id
            , country_code
            , market_id
            , os_id
            , platform_device_type
            , game_sub_user_reg_datekey
            , game_sub_user_reg_datetime
        )
        VALUES
        (
            source.reg_datekey
            , source.reg_datetime
            , source.game_id
            , source.world_id
            , source.joyple_game_code
            , source.auth_method_id
            , source.auth_account_name
            , source.tracker_account_id
            , source.tracker_type_id
            , source.device_id
            , source.country_code
            , source.market_id
            , source.os_id
            , source.platform_device_type
            , source.game_sub_user_reg_datekey
            , source.game_sub_user_reg_datetime
        )
        WHEN MATCHED AND (target.reg_datekey > source.reg_datekey)
        THEN
        UPDATE SET 
        target.reg_datekey = source.reg_datekey;

        """
        client.query(query)
        print(f"■ {target_date.strftime('%Y-%m-%d')} adjust_common_register_char Batch 완료")
    
    print("✅ adjust_common_register_char ETL 완료")
    return True


def etl_f_common_access(target_date: list):

    for td in target_date:
        target_date = td

        # KST 00:00:00 ~ 23:59:59를 UTC로 변환
        kst = pytz.timezone('Asia/Seoul')
        start_utc = target_date.replace(tzinfo=kst).astimezone(pytz.UTC)
        end_utc = (target_date + timedelta(days=1)).replace(tzinfo=kst).astimezone(pytz.UTC)
        print(f"📝 시작시간 : ", start_utc, f" 📝 종료시간 : ", end_utc)

        query = f"""
        MERGE `datahub-478802.datahub.f_common_access` AS target
        USING (
        with TA as (
        SELECT a.GameID
            , a.WorldID
            , a.ServerName
            , a.JoypleGameID
            , a.AuthMethodID
            , a.AuthAccountName
            , a.GameAccountName
            , a.GameSubUserName
            , a.DeviceAccountName
            , a.TrackerAccountName
            , a.TrackerTypeID
            , a.MarketID
            , a.OSID
            , a.PlatformDeviceType
            , a.AccountLevel
            , a.IP
            , a.AccessTypeID
            , a.PlaySeconds
            , a.LogTime
        FROM `dataplatform-reporting.DataService.V_0160_0000_AccessLog_V` AS a
        WHERE a.LogTime >= '{start_utc}'
        AND a.LogTime < '{end_utc}'

        UNION ALL

        SELECT GameID
        , null as WorldID
        , ServerName
        , JoypleGameID
        , AuthMethodID
        , AuthAccountName
        , GameAccountName
        , GameSubUserName
        , null as DeviceAccountName
        , null as TrackerAccountName
        , null as TrackerTypeID
        , MarketID
        , OSID
        , PlatformDeviceType
        , GameSubUserLevel as AccountLevel
        , ip as IP
        , 1 as AccessTypeID
        , 0 as PlaySeconds
        , LogTime
        FROM `dataplatform-reporting.DataService.V_0156_0000_CommonLogPaymentFix_V`
        WHERE LogTime >= '{start_utc}'
        AND LogTime < '{end_utc}'
        )
            SELECT 
                DATE(TA.LogTime, "Asia/Seoul") as datekey
                , c.reg_datekey
                , DATE_DIFF(DATE(TA.LogTime, "Asia/Seoul"), c.reg_datekey, DAY) reg_datediff	      
                , TA.GameID AS game_id
                , TA.WorldID AS world_id
                , TA.JoypleGameID AS joyple_game_code
                , TA.GameAccountName AS game_account_name
                , TA.GameSubUserName AS game_sub_user_name
                , TA.AuthMethodID AS auth_method_id
                , TA.AuthAccountName AS auth_account_name
                , TA.ServerName AS server_name
                , TA.DeviceAccountName as device_id
                , b.country_code AS country_code
                , TA.MarketID AS market_id
                , TA.OSID AS os_id
                , TA.PlatformDeviceType AS platform_device_type
                , TA.AccountLevel AS game_user_level
                , TA.AccessTypeID AS access_type_id
                , SUM(CASE WHEN TA.AccessTypeID = 1 THEN 1 ELSE 0 END) AS access_cnt
                , IF(SUM(TA.PlaySeconds) > 86400, 86400, SUM(TA.PlaySeconds)) AS play_seconds
                FROM TA
                LEFT OUTER JOIN `datahub-478802.datahub.dim_ip4_country_code` AS b ON TA.IP = b.ip
                LEFT OUTER JOIN `datahub-478802.datahub.f_common_register` AS c 
                ON TA.JoypleGameID = c.joyple_game_code AND CAST(TA.AuthAccountName AS STRING) = CAST(c.auth_account_name AS STRING)
                group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
        ) AS source
            ON target.datekey = source.datekey
            AND target.game_id = source.game_id
            AND target.world_id = source.world_id
            AND target.joyple_game_code = source.joyple_game_code
            AND target.game_account_name = source.game_account_name
            AND target.game_sub_user_name = source.game_sub_user_name
            AND target.auth_method_id = source.auth_method_id
            AND target.auth_account_name = source.auth_account_name
            AND target.server_name = source.server_name
            AND target.device_id = source.device_id
            AND target.country_code = source.country_code
            AND target.market_id = source.market_id
            AND target.os_id = source.os_id
            AND target.platform_device_type = source.platform_device_type
            AND target.game_user_level = source.game_user_level
            AND target.access_type_id = source.access_type_id
            WHEN NOT MATCHED THEN
            INSERT
            (datekey, 
            reg_datekey, 
            reg_datediff, 
            game_id, 
            world_id, 
            joyple_game_code, 
            game_account_name, 
            game_sub_user_name, 
            auth_method_id, 
            auth_account_name,
            server_name, 
            device_id, 
            country_code, 
            market_id, 
            os_id, 
            platform_device_type, 
            game_user_level,
            access_type_id, 
            access_cnt, 
            play_seconds
            )
            VALUES
            (
            source.datekey, 
            source.reg_datekey, 
            source.reg_datediff, 
            source.game_id, 
            source.world_id, 
            source.joyple_game_code, 
            source.game_account_name, 
            source.game_sub_user_name,
            source.auth_method_id, 
            source.auth_account_name, 
            source.server_name, 
            source.device_id, 
            source.country_code, 
            source.market_id, 
            source.os_id, 
            source.platform_device_type,
            source.game_user_level,
            source.access_type_id, 
            source.access_cnt, 
            source.play_seconds
            );
        """

        client.query(query)
        print(f"■ {target_date.strftime('%Y-%m-%d')} f_common_access Batch 완료")
    
    print("✅ f_common_access ETL 완료")
    return True
    

def etl_f_common_access_last_login(target_date: list):

    for td in target_date:
        target_date = td


        start_date = datetime.strptime(td, "%Y-%m-%d")
        end_date = start_date + timedelta(days=1)
        start_date = start_date.strftime("%Y-%m-%d") # '2014-06-10'
        end_date = end_date.strftime("%Y-%m-%d")     # '2014-06-11'

        print(f"📝 시작시간 : ", start_date, f" 📝 종료시간 : ", end_date)

        query = f"""
        MERGE datahub-478802.datahub.f_common_access_last_login AS target
        USING
        (
            SELECT joyple_game_code, auth_account_name, game_sub_user_name
                , min(auth_method_id) as auth_method_id
                , max(datekey) as datekey
                , max(game_user_level) as max_game_user_level
            FROM datahub-478802.datahub.f_common_access
            WHERE datekey >= DATE_SUB(DATE({target_date}), INTERVAL 1 DAY) AND datekey < {target_date}
            GROUP BY joyple_game_code, auth_account_name, game_sub_user_name
        ) AS source
        ON target.joyple_game_code = source.joyple_game_code AND target.auth_account_name = source.auth_account_name AND target.game_sub_user_name = source.game_sub_user_name
        WHEN MATCHED AND (target.max_game_user_level <> source.max_game_user_level OR target.last_login_datekey < source.datekey)
        THEN
        UPDATE SET 
        target.last_login_datekey = source.datekey
        , target.max_game_user_level = source.max_game_user_level
        , target.update_timestamp = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED BY target THEN
        INSERT (joyple_game_code, game_sub_user_name, auth_method_id, auth_account_name, last_login_datekey, max_game_user_level, update_timestamp)
        VALUES
        (
            source.joyple_game_code
            , source.game_sub_user_name
            , source.auth_method_id
            , source.auth_account_name
            , source.datekey
            , source.max_game_user_level
            , CURRENT_TIMESTAMP()
        );
        """
        client.query(query)
        print(f"■ {target_date.strftime('%Y-%m-%d')} f_common_access_last_login Batch 완료")
    
    print("✅ f_common_access_last_login ETL 완료")
    return True