from google.cloud import bigquery
from datetime import datetime, timezone, timedelta
import time
import pytz


    
# 빅쿼리 클라이언트 연결
client = bigquery.Client()

def etl_pre_payment_deduct_user(target_date: list):

    for td in target_date:
        target_date = td

        # KST 00:00:00 ~ 23:59:59를 UTC로 변환
        kst = pytz.timezone('Asia/Seoul')
        start_utc = target_date.replace(tzinfo=kst).astimezone(pytz.UTC)
        end_utc = (target_date + timedelta(days=1)).replace(tzinfo=kst).astimezone(pytz.UTC)
        print(f"📝 시작시간 : ", start_utc, f" 📝 종료시간 : ", end_utc)

        query = f"""
        MERGE `datahub-478802.datahub.pre_payment_deduct_user` AS target
        USING
        (
        SELECT JoypleGameID
                , AuthMethodID
                , AuthAccountName
                , RegistredTime AS UpdatedTimestamp
        FROM `dataplatform-services.ServiceData.PaymentDeductUser`
        WHERE RegistredTime >= {start_utc}
        AND RegistredTime < {end_utc}')
        AND JoypleGameID IS NOT NULL
        AND AuthMethodID IS NOT NULL
        AND AuthAccountName IS NOT NULL
        ) AS source 
            ON target.joyple_game_code = source.JoypleGameID 
            AND target.auth_method_id = source.AuthMethodID 
            AND target.auth_account_name = source.AuthAccountName
        WHEN MATCHED AND target.update_timestamp <> source.UpdatedTimestamp THEN
        UPDATE SET target.update_timestamp = source.UpdatedTimestamp
        WHEN NOT MATCHED BY target THEN
        INSERT(joyple_game_code, auth_method_id, auth_account_name, update_timestamp)
        VALUES(
                source.JoypleGameID
            , source.AuthMethodID
            , source.AuthAccountName
            , source.UpdatedTimestamp
        )
        WHEN NOT MATCHED BY source THEN
        DELETE 
        """

        client.query(query)
        print(f"■ {target_date.strftime('%Y-%m-%d')} pre_payment_deduct_user Batch 완료")
    
    print("✅ pre_payment_deduct_user ETL 완료")
    return True



def etl_pre_payment_deduct_order(target_date: list):

    for td in target_date:
        target_date = td

        # KST 00:00:00 ~ 23:59:59를 UTC로 변환
        kst = pytz.timezone('Asia/Seoul')
        start_utc = target_date.replace(tzinfo=kst).astimezone(pytz.UTC)
        end_utc = (target_date + timedelta(days=1)).replace(tzinfo=kst).astimezone(pytz.UTC)
        print(f"📝 시작시간 : ", start_utc, f" 📝 종료시간 : ", end_utc)

        query = f"""
        MERGE `datahub-478802.datahub.pre_payment_deduct_order` AS target
        USING
        (
        SELECT JoypleGameID
                , OrderID
                , ProductCode
                , LogTime       AS LogTimestamp
                , RegistredTime AS UpdatedTimestamp
        FROM `dataplatform-services.ServiceData.PaymentDeductOrder` --
            WHERE RegistredTime >= {start_utc}
            AND RegistredTime < {end_utc}
            AND JoypleGameID IS NOT NULL
            AND OrderID IS NOT NULL
            AND ProductCode IS NOT NULL
        ) AS source ON target.joyple_game_code = source.JoypleGameID AND target.order_id = source.OrderID
        WHEN MATCHED AND target.product_code <> source.ProductCode OR target.update_timestamp <> source.UpdatedTimestamp THEN
        UPDATE SET target.product_code = source.ProductCode
                , target.update_timestamp = source.UpdatedTimestamp
        WHEN NOT MATCHED BY target THEN
        INSERT(joyple_game_code, order_id, product_code, update_timestamp)
        VALUES(
            source.JoypleGameID
            , source.OrderID
            , source.ProductCode
            , source.UpdatedTimestamp
        )
        WHEN NOT MATCHED BY source 
            AND (
                target.update_timestamp >= {start_utc}
                AND target.update_timestamp < {end_utc}
                ) THEN
        DELETE  
        ;
        """

        client.query(query)
        print(f"■ {target_date.strftime('%Y-%m-%d')} pre_payment_deduct_order Batch 완료")
    
    print("✅ pre_payment_deduct_order ETL 완료")
    return True


def etl_pre_payment_info_fix():
    truncate_query = f"""
    TRUNCATE TABLE `datahub-478802.datahub.pre_payment_info_fix`
    """

    query = f"""
    INSERT INTO `datahub-478802.datahub.pre_payment_info_fix`
    (
    log_type, 
    joyple_game_code, 
    order_id, 
    market_id, 
    pg_id, 
    platform_device_type, 
    product_code, 
    product_name, 
    currency_code, 
    price, 
    log_timestamp, 
    update_timestamp
    )
    SELECT 
    LogType, 
    JoypleGameID, 
    OrderID, 
    MarketID, 
    PGID, 
    PlatformDeviceType, 
    ProductCode, 
    ProductName,
    CurrencyCode, 
    Price, 
    LogTimestamp, 
    UpdatedTimestamp
    FROM dataplatform-reporting.DataService.T_0141_0000_PaymentInfoFix_V ---- 얘가 어떤 데이터인지 좀 명확히 확인하고 지금처럼 처리하는게 맞을지?
    """

    client.query(truncate_query)
    time.sleep(5)
    client.query(query)
    print("✅ pre_payment_info_fix ETL 완료")


def etl_f_common_payment_total(target_date: list):

    for td in target_date:
        target_date = td

        # KST 00:00:00 ~ 23:59:59를 UTC로 변환
        kst = pytz.timezone('Asia/Seoul')
        start_utc = target_date.replace(tzinfo=kst).astimezone(pytz.UTC)
        end_utc = (target_date + timedelta(days=1)).replace(tzinfo=kst).astimezone(pytz.UTC)
        print(f"📝 시작시간 : ", start_utc, f" 📝 종료시간 : ", end_utc)

        query = f"""
            MERGE datahub-478802.datahub.f_common_payment_total AS  target
            USING
            (
            with TA as (
            SELECT a.game_id
                , a.joyple_game_code 
                , a.auth_method_id
                , a.auth_account_name
                , IFNULL(b.market_id, a.market_id) AS market_id
                , IFNULL(b.pg_id, a.pg_id)         AS pg_id
                , IFNULL(b.platform_device_type, a.platform_device_type) AS platform_device_type
                , a.ip
                , a.order_id
                , IFNULL(b.product_code, a.product_code) AS product_code 
                , IFNULL(b.product_name, a.product_name) AS product_name
                , IFNULL(b.currency_code, a.currency_code) AS currency_code
                , IFNULL(b.price, a.price) AS price
                , a.log_time
                , a.MultiQuantity
            FROM (
            SELECT b.game_id
                , a.GCode                              AS joyple_game_code
                , 1                                    AS auth_method_id
                , a.userkey                            AS auth_account_name
                , a.market_code                        AS market_id
                , a.pg_code                            AS pg_id
                , IFNULL(a.platform_device_type, 1)    AS platform_device_type
                , a.ip                                 AS ip
                , a.payment_key                        AS order_id
                , a.product_id                         AS product_code
                , a.product_name                       AS product_name
                , d.currency                       AS currency_code
                , a.product_price * d.Ratio            AS price
                , a.timestamp                          AS log_time
                , 1                                    AS MultiQuantity
            FROM `dataplatform-204306.JoypleLog.payment_log` AS a
            INNER JOIN `datahub-478802.datahub.dim_joyple_game_code` AS b ON a.GCode = b.joyple_game_code
            LEFT OUTER JOIN `datahub-478802.datahub.dim_currency_joyple` AS d ON a.money_type = d.money_type
            WHERE a.timestamp >= '2025-12-01 15:00:00 UTC'
                AND a.timestamp < '2025-12-02 15:00:00 UTC'
                AND a.product_price > 0
                AND a.gcode < 30000
                AND IF((a.timestamp >= '2025-02-19 15:00:00 UTC' AND a.gcode in (131, 133, 155, 156, 159, 1590)), FALSE, IF(gcode < 30000, TRUE, FALSE))
                AND IF((a.timestamp >= '2025-04-07 15:00:00 UTC' AND a.gcode in (67)), FALSE, IF(gcode < 30000, TRUE, FALSE))
                AND a.market_code NOT IN (40)  -- 조이포인트 구매 제외

            UNION ALL

            SELECT a.game_id                                                                            
                , a.joyple_game_code                                                                   
                , a.auth_method_id                                                                     
                , a.auth_account_name                                                                  
                , a.market_id                                                                          
                , a.pg_id
                , IFNULL(IF(a.joyple_game_code BETWEEN 60000 AND 60008,  2, a.platform_device_type),1) AS platform_device_type
                , a.ip                                                                                 
                , a.order_id                                                                                
                , a.product_code                                                                       
                , IFNULL(a.product_name, a.product_code) AS product_name
                , a.currency_code                                       
                , a.price                                               
                , a.log_time                                            
                , IFNULL(a.multiQuantity, 1) AS multiQuantity
            FROM `dataplatform-204306.CommonLog.Payment` AS a
            WHERE a.log_time >= '2025-12-01 15:00:00 UTC'
                AND a.log_time < '2025-12-02 15:00:00 UTC'
                AND a.game_id           IS NOT NULL
                AND a.world_id          IS NOT NULL
                AND a.server_name       IS NOT NULL
                AND a.auth_method_id    IS NOT NULL
                AND a.auth_account_name IS NOT NULL
                AND a.game_account_name IS NOT NULL
                AND a.market_id         IS NOT NULL
                AND a.os_id             IS NOT NULL
                AND a.pg_id             IS NOT NULL
                AND a.ip                IS NOT NULL
                AND a.product_code      IS NOT NULL
                AND a.currency_code     IS NOT NULL
                AND a.price             IS NOT NULL
                AND a.order_id          IS NOT NULL
                AND a.log_time          IS NOT NULL
                AND a.price > 0
                AND (a.joyple_game_code >= 30000 OR joyple_game_code in (131, 133, 155, 156, 67, 159, 1590))
                AND IF(a.joyple_game_code >= 30000, TRUE, IF(a.log_time < '2025-02-19 15:00:00 UTC' AND a.joyple_game_code in (131, 133, 155, 156), FALSE, TRUE))
                AND IF(a.joyple_game_code >= 30000, TRUE, IF(a.log_time < '2025-04-07 15:00:00 UTC' AND a.joyple_game_code in (67), FALSE, TRUE))
                AND a.market_id NOT IN (40)  -- 조이포인트 구매 제외     
            ) AS a 
            LEFT OUTER JOIN datahub-478802.datahub.pre_payment_info_fix AS b 
            ON (a.joyple_game_code = b.joyple_game_code AND a.order_id = b.order_id)
            ), 
            TB as (
            SELECT TA.game_id,
            TA.joyple_game_code,
            TA.auth_method_id,
            TA.auth_account_name,
            TA.market_id,
            TA.pg_id,
            TA.platform_device_type,
            TA.ip,
            TA.order_id,
            TA.product_code,
            TA.product_name,
            TA.currency_code,
            TA.price,
            TA.log_time,
            TA.MultiQuantity
            FROM TA
            WHERE CONCAT(TA.joyple_game_code, TA.order_id) not in (
                SELECT CONCAT(joyple_game_code, order_id) FROM datahub-478802.datahub.pre_payment_deduct_order
            )
            AND CONCAT(TA.joyple_game_code, TA.auth_method_id, TA.auth_account_name) not in (
                SELECT CONCAT(joyple_game_code, auth_method_id, auth_account_name) FROM datahub-478802.datahub.pre_payment_deduct_user
            )
            )
            , TC as (
            SELECT DATE(a.log_time, "Asia/Seoul") as datekey
            , d.reg_datekey
            , DATE_DIFF(d.reg_datekey, DATE(a.log_time, "Asia/Seoul"), DAY) as reg_datediff
            , d.reg_country_code
            , a.game_id
            , a.joyple_game_code
            , a.auth_method_id
            , a.auth_account_name
            , a.ip
            , a.market_id
            , a.platform_device_type
            , a.pg_id
            , a.product_code
            , a.product_name
            , a.Price * IFNULL(c.exchange_rate, 1) AS Price_KRW
            , IFNULL(a.MultiQuantity, 1) AS MultiQuantity
            FROM TB as a
            LEFT JOIN (
                SELECT currency, datekey, exchange_rate
                FROM `datahub-478802.datahub.dim_exchange`
                GROUP BY currency, datekey, exchange_rate
            ) AS c  
            ON a.currency_code = c.currency AND Date(a.log_time, "Asia/Seoul") = c.datekey
            LEFT JOIN `datahub-478802.datahub.f_common_register` as d
            on (CAST(a.joyple_game_code AS STRING) = CAST(d.joyple_game_code AS STRING) 
            AND a.auth_method_id = d.auth_method_id
            AND CAST(a.auth_account_name AS STRING) = CAST(d.auth_account_name AS STRING))
            )

            SELECT datekey, 
                reg_datekey, 
                reg_datediff,
                reg_country_code,
                game_id, 
                joyple_game_code,
                auth_method_id,
                auth_account_name,
                b.country_code as country_code,
                market_id,
                platform_device_type,
                pg_id,
                product_code,
                product_name,
                sum(price_KRW) as revenue,
                sum(MultiQuantity) as buy_cnt
                FROM TC as a
                LEFT OUTER JOIN datahub-478802.datahub.dim_ip4_country_code AS b
                ON (a.ip = b.ip)
                GROUP BY 
                datekey, 
                reg_datekey, 
                reg_datediff,
                reg_country_code,
                game_id, 
                joyple_game_code,
                auth_method_id,
                auth_account_name,
                b.country_code as country_code,
                market_id,
                platform_device_type,
                pg_id,
                product_code,
                product_name

            ) as source
            ON target.datekey = source.datekey
            AND target.reg_datekey = source.reg_datekey
            AND target.reg_datediff = source.reg_datediff
            AND target.reg_country_code = source.reg_country_code
            AND target.game_id = source.game_id
            AND target.joyple_game_code = source.joyple_game_code
            AND target.auth_method_id = source.auth_method_id
            AND target.auth_account_name = source.auth_account_name
            AND target.country_code = source.country_code
            AND target.platform_device_type = source.platform_device_type
            AND target.pg_id = source.pg_id
            AND target.product_code = source.product_code
            AND target.product_name = source.product_name
            
            WHEN NOT MATCHED BY target THEN 
            INSERT (
            datekey,
            reg_datekey,
            reg_datediff,
            reg_country_code,
            game_id,
            joyple_game_code,
            auth_method_id,
            auth_account_name,
            country_code,
            market_id,
            platform_device_type,
            pg_id,
            product_code,
            product_name,
            revenue,
            buy_cnt
            )
            VALUES (
            source.datekey,
            source.reg_datekey,
            source.reg_datediff,
            source.reg_country_code,
            source.game_id,
            source.joyple_game_code,
            source.auth_method_id,
            source.auth_account_name,
            source.country_code,
            source.market_id,
            source.platform_device_type,
            source.pg_id,
            source.product_code,
            source.product_name,
            source.revenue,
            source.buy_cnt
            )
        """
        client.query(query)
        print(f"■ {target_date.strftime('%Y-%m-%d')} f_common_payment_total Batch 완료")
    
    print("✅ f_common_payment_total ETL 완료")
    return True




def etl_f_common_payment(target_date: list):

    for td in target_date:
        target_date = td

        # KST 00:00:00 ~ 23:59:59를 UTC로 변환
        kst = pytz.timezone('Asia/Seoul')
        start_utc = target_date.replace(tzinfo=kst).astimezone(pytz.UTC)
        end_utc = (target_date + timedelta(days=1)).replace(tzinfo=kst).astimezone(pytz.UTC)
        print(f"📝 시작시간 : ", start_utc, f" 📝 종료시간 : ", end_utc)

        query = f"""
            -- TA는 V_0150_0000_Payment_V 를 처리한 내용
            -- TB는 V_0153_0000_PaymentDeduct_V 랑 V_0155_0000_PaymentFix_V를 합쳐서 처리한 내용
            -- 이후 insert 처리를 진행해야 함
            
            MERGE datahub-478802.datahub.f_common_payment AS target 
            USING (
            with TA AS (
            SELECT a.game_id                                                                            
                , a.joyple_game_code                                                                          
                , a.game_account_name
                , a.game_sub_user_name
                , a.auth_method_id
                , a.auth_account_name
                , a.server_name
                , a.device_id
                , a.market_id
                , a.os_id
                , a.pg_id
                , a.game_user_level
                , IFNULL(IF(a.joyple_game_code BETWEEN 60000 AND 60008,  2, a.platform_device_type),1) AS platform_device_type
                , a.ip                                                                                 
                , a.order_id                                                                                
                , a.product_code                                                                       
                , IFNULL(a.product_name, a.product_code) AS product_name
                , a.currency_code                                       
                , a.price                                               
                , a.log_time                                            
                , IFNULL(a.multiQuantity, 1) AS multiQuantity
            FROM `dataplatform-204306.CommonLog.Payment` AS a
            LEFT OUTER JOIN datahub-478802.datahub.pre_payment_info_fix AS b 
            ON (a.joyple_game_code = b.joyple_game_code AND a.order_id = b.order_id)
            WHERE a.log_time >= {start_utc}
                AND a.log_time < {end_utc}
                AND a.game_id           IS NOT NULL
                AND a.world_id          IS NOT NULL
                AND a.server_name       IS NOT NULL
                AND a.auth_method_id    IS NOT NULL
                AND a.auth_account_name IS NOT NULL
                AND a.game_account_name IS NOT NULL
                AND a.market_id         IS NOT NULL
                AND a.os_id             IS NOT NULL
                AND a.pg_id             IS NOT NULL
                AND a.ip                IS NOT NULL
                AND a.product_code      IS NOT NULL
                AND a.currency_code     IS NOT NULL
                AND a.price             IS NOT NULL
                AND a.order_id          IS NOT NULL
                AND a.log_time          IS NOT NULL
                AND a.price > 0
                AND (a.joyple_game_code >= 30000 OR a.joyple_game_code in (131, 133, 155, 156, 67, 159, 1590))
                AND IF(a.joyple_game_code >= 30000, TRUE, IF(a.log_time < '2025-02-19 15:00:00 UTC' AND a.joyple_game_code in (131, 133, 155, 156), FALSE, TRUE))
                AND IF(a.joyple_game_code >= 30000, TRUE, IF(a.log_time < '2025-04-07 15:00:00 UTC' AND a.joyple_game_code in (67), FALSE, TRUE))
                AND a.market_id NOT IN (40)  -- 조이포인트 구매 제외  
            ), 
            -- deduction 처리
            TB as (
            SELECT TA.game_id,
            TA.joyple_game_code,
            TA.game_account_name,
            TA.game_sub_user_name,
            TA.auth_method_id,
            TA.auth_account_name,
            TA.server_name,
            TA.device_id,
            TA.ip,
            TA.market_id,
            TA.os_id,
            TA.pg_id,
            TA.platform_device_type,
            TA.game_user_level,
            TA.order_id,
            TA.product_code,
            TA.product_name,
            TA.currency_code,
            TA.price,
            TA.log_time,
            TA.MultiQuantity
            FROM TA
            WHERE CONCAT(TA.joyple_game_code, TA.order_id) not in (
                SELECT CONCAT(joyple_game_code, order_id) FROM datahub-478802.datahub.pre_payment_deduct_order
            )
            AND CONCAT(TA.joyple_game_code, TA.auth_method_id, TA.auth_account_name) not in (
                SELECT CONCAT(joyple_game_code, auth_method_id, auth_account_name) FROM datahub-478802.datahub.pre_payment_deduct_user
            )
            )
            , TC as (
            SELECT DATE(a.log_time, "Asia/Seoul") as datekey
            , d.reg_datekey
            , DATE_DIFF(d.reg_datekey, DATE(a.log_time, "Asia/Seoul"), DAY) as reg_datediff
            , d.reg_country_code
            , a.game_id
            , a.joyple_game_code
            , a.game_account_name
            , a.game_sub_user_name
            , a.auth_method_id
            , a.auth_account_name
            , a.server_name
            , a.device_id
            , a.ip
            , a.market_id
            , a.os_id
            , a.pg_id
            , a.platform_device_type
            , a.game_user_level
            , a.product_code
            , a.product_name
            , a.price * IFNULL(c.exchange_rate, 1) AS Price_KRW
            , IFNULL(a.MultiQuantity, 1) AS MultiQuantity
            FROM TB as a
            LEFT JOIN (
                SELECT currency, datekey, exchange_rate
                FROM `datahub-478802.datahub.dim_exchange`
                GROUP BY currency, datekey, exchange_rate
            ) AS c  
            ON a.currency_code = c.currency AND Date(a.log_time, "Asia/Seoul") = c.datekey
            LEFT JOIN `datahub-478802.datahub.f_common_register` as d
            on CAST(a.joyple_game_code AS STRING) = CAST(d.joyple_game_code AS STRING) 
               AND CAST(a.auth_method_id AS STRING) = CAST(a.auth_method_id AS STRING)
               AND CAST(a.auth_account_name AS STRING) = CAST(d.auth_account_name AS STRING)
            )

                SELECT a.datekey, 
                a.reg_datekey, 
                a.reg_datediff,
                a.reg_country_code,
                a.game_id,
                a.joyple_game_code,
                a.game_account_name,
                a.game_sub_user_name,
                a.auth_method_id,
                a.auth_account_name,
                a.server_name,
                a.device_id,
                b.country_code,
                a.market_id,
                a.os_id,
                a.pg_id,
                a.platform_device_type,
                a.game_user_level,
                product_code,
                product_name,
                sum(price_KRW) as revenue,
                sum(MultiQuantity) as buy_cnt
                FROM TC as a
                LEFT OUTER JOIN datahub-478802.datahub.dim_ip4_country_code AS b
                ON (a.ip = b.ip)
                group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
            ) as source
            ON target.datekey = source.datekey
            AND target.reg_datekey = source.reg_datekey
            AND target.reg_datediff = source.reg_datediff
            AND target.reg_country_code = source.reg_country_code
            AND target.game_id = source.game_id
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
            AND target.pg_id = source.pg_id
            AND target.platform_device_type = source.platform_device_type
            AND target.game_user_level = source.game_user_level
            AND target.product_code = source.product_code
            AND target.product_name = source.product_name
            WHEN NOT MATCHED BY target THEN 
            INSERT (
            datekey,
            reg_datekey,
            reg_datediff,
            reg_country_code,
            game_id,
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
            pg_id,
            platform_device_type,
            game_user_level,
            product_code,
            product_name,
            revenue,
            buy_cnt
            )
            VALUES (
            source.datekey,
            source.reg_datekey,
            source.reg_datediff,
            source.reg_country_code,
            source.game_id,
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
            source.pg_id,
            source.platform_device_type,
            source.game_user_level,
            source.product_code,
            source.product_name,
            source.revenue,
            source.buy_cnt
            )
        """
        client.query(query)
        print(f"■ {target_date.strftime('%Y-%m-%d')} f_common_payment Batch 완료")
    
    print("✅ f_common_payment ETL 완료")
    return True