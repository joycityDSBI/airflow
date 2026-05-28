from google.cloud import bigquery
from datetime import datetime, timezone, timedelta
import pytz


def etl_f_game_exchange(target_date: list, client):

    kst = pytz.timezone('Asia/Seoul')

    for td_str in target_date:
        try:
            current_date_obj = datetime.strptime(td_str, "%Y-%m-%d")
        except ValueError:
            print(f"⚠️ 날짜 형식이 잘못되었습니다: {td_str}")
            continue

        start_kst = kst.localize(current_date_obj)
        start_utc = start_kst.astimezone(timezone.utc)
        end_kst   = start_kst + timedelta(days=1)
        end_utc   = end_kst.astimezone(timezone.utc)

        print(f"📝 대상날짜: {td_str}")
        print(f"   ㄴ 시작시간(UTC): {start_utc}")
        print(f"   ㄴ 종료시간(UTC): {end_utc}")

        query = f"""
        MERGE `datahub-478802.IMGN.f_game_exchange` AS T
        USING (
            WITH base AS (
                SELECT
                    DATE(LogDate_Svr, 'Asia/Seoul') AS datekey,
                    LogDate_Svr,
                    UserID,
                    UserLevel,
                    WorldID AS server_name,
                    Reason,
                    p0, p1, p2, p3, p5, p6, p7, p8, p12
                FROM `dataplatform-204306.IMGN.T_Log_Game`
                WHERE Reason IN (70001, 70002, 70003, 70004, 70006)
                  AND LogDate_Svr >= TIMESTAMP('{start_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
                  AND LogDate_Svr <  TIMESTAMP('{end_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
            ),
            normalized AS (
                -- 70001 등록 (p7=1 성공건만)
                SELECT
                    datekey, LogDate_Svr,
                    CAST(UserID AS STRING)   AS game_sub_user_name,
                    CAST(UserLevel AS INT64) AS game_user_level,
                    server_name,
                    CAST(p0 AS STRING)       AS item_kind,
                    '등록'                   AS action_type,
                    CAST(NULL AS INT64)      AS is_auto_exchange,
                    LogDate_Svr              AS registered_at,
                    CAST(p2 AS INT64)        AS exchange_count,
                    CAST(p5 AS INT64)        AS draconic_amount,
                    CAST(NULL AS INT64)      AS draconic_fee
                FROM base WHERE Reason = 70001 AND CAST(p7 AS INT64) = 1

                UNION ALL

                -- 70003 구매 (수동, is_auto_exchange=0)
                SELECT
                    datekey, LogDate_Svr,
                    CAST(UserID AS STRING)   AS game_sub_user_name,
                    CAST(UserLevel AS INT64) AS game_user_level,
                    server_name,
                    CAST(p0 AS STRING)       AS item_kind,
                    '구매'                   AS action_type,
                    0                        AS is_auto_exchange,
                    TIMESTAMP_SECONDS(CAST(p6 AS INT64)) AS registered_at,
                    CAST(p2 AS INT64)        AS exchange_count,
                    CAST(p7 AS INT64)        AS draconic_amount,
                    CAST(NULL AS INT64)      AS draconic_fee
                FROM base WHERE Reason = 70003

                UNION ALL

                -- 70006 구매 (자동, is_auto_exchange=1)
                SELECT
                    datekey, LogDate_Svr,
                    CAST(UserID AS STRING)   AS game_sub_user_name,
                    CAST(UserLevel AS INT64) AS game_user_level,
                    server_name,
                    CAST(p1 AS STRING)       AS item_kind,
                    '구매'                   AS action_type,
                    1                        AS is_auto_exchange,
                    TIMESTAMP_SECONDS(CAST(p6 AS INT64)) AS registered_at,
                    CAST(p3 AS INT64)        AS exchange_count,
                    CAST(p7 AS INT64)        AS draconic_amount,
                    CAST(NULL AS INT64)      AS draconic_fee
                FROM base WHERE Reason = 70006

                UNION ALL

                -- 70002 판매
                SELECT
                    datekey, LogDate_Svr,
                    CAST(p12 AS STRING)      AS game_sub_user_name,
                    CAST(NULL AS INT64)      AS game_user_level,
                    server_name,
                    CAST(p0 AS STRING)       AS item_kind,
                    '판매'                   AS action_type,
                    CAST(NULL AS INT64)      AS is_auto_exchange,
                    TIMESTAMP_SECONDS(CAST(p6 AS INT64)) AS registered_at,
                    CAST(p2 AS INT64)        AS exchange_count,
                    CAST(p5 AS INT64)        AS draconic_amount,
                    CAST(NULL AS INT64)      AS draconic_fee
                FROM base WHERE Reason = 70002

                UNION ALL

                -- 70004 정산 (draconic_fee = p5 - (p8 - p7))
                SELECT
                    datekey, LogDate_Svr,
                    CAST(UserID AS STRING)   AS game_sub_user_name,
                    CAST(UserLevel AS INT64) AS game_user_level,
                    server_name,
                    CAST(p0 AS STRING)       AS item_kind,
                    '정산'                   AS action_type,
                    CAST(NULL AS INT64)      AS is_auto_exchange,
                    TIMESTAMP_SECONDS(CAST(p6 AS INT64)) AS registered_at,
                    CAST(p2 AS INT64)        AS exchange_count,
                    CAST(p5 AS INT64)        AS draconic_amount,
                    CAST(p5 AS INT64) - (CAST(p8 AS INT64) - CAST(p7 AS INT64)) AS draconic_fee
                FROM base WHERE Reason = 70004
            ),
            final AS (
                SELECT
                    a.datekey,
                    a.game_sub_user_name,
                    b.reg_datekey,
                    DATE_DIFF(a.datekey, b.reg_datekey, DAY)             AS reg_datediff,
                    b.reg_country_code,
                    b.game_sub_user_reg_datekey,
                    a.game_user_level,
                    a.server_name,
                    a.item_kind,
                    a.action_type,
                    a.is_auto_exchange,
                    a.registered_at,
                    SUM(a.exchange_count)                                AS exchange_count,
                    SUM(a.draconic_amount)                               AS draconic_amount,
                    SUM(a.draconic_fee)                                  AS draconic_fee,
                    SAFE_DIVIDE(SUM(a.draconic_amount),
                                SUM(a.exchange_count))                   AS unit_price_draconic,
                    CAST(AVG(IF(a.action_type = '판매',
                                TIMESTAMP_DIFF(a.LogDate_Svr, a.registered_at, MINUTE),
                                NULL)) AS INT64)                         AS avg_holding_minutes,
                    COUNT(*)                                             AS action_cnt
                FROM normalized AS a
                LEFT JOIN `datahub-478802.datahub.f_common_register_char` AS b
                    ON  b.joyple_game_code   = 30007
                    AND a.game_sub_user_name = b.game_sub_user_name
                GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
            )
            SELECT * FROM final
        ) AS S
        ON  T.datekey            = S.datekey
        AND T.game_sub_user_name = S.game_sub_user_name
        AND T.item_kind          = S.item_kind
        AND T.action_type        = S.action_type
        AND T.is_auto_exchange   IS NOT DISTINCT FROM S.is_auto_exchange
        AND T.registered_at      = S.registered_at
        WHEN MATCHED THEN UPDATE SET
            T.reg_datekey               = S.reg_datekey,
            T.reg_datediff              = S.reg_datediff,
            T.reg_country_code          = S.reg_country_code,
            T.game_sub_user_reg_datekey = S.game_sub_user_reg_datekey,
            T.game_user_level           = S.game_user_level,
            T.server_name               = S.server_name,
            T.exchange_count            = S.exchange_count,
            T.draconic_amount           = S.draconic_amount,
            T.draconic_fee              = S.draconic_fee,
            T.unit_price_draconic       = S.unit_price_draconic,
            T.avg_holding_minutes       = S.avg_holding_minutes,
            T.action_cnt                = S.action_cnt
        WHEN NOT MATCHED THEN INSERT (
            datekey, game_sub_user_name, reg_datekey, reg_datediff, reg_country_code,
            game_sub_user_reg_datekey, game_user_level, server_name,
            item_kind, action_type, is_auto_exchange, registered_at,
            exchange_count, draconic_amount, draconic_fee,
            unit_price_draconic, avg_holding_minutes, action_cnt
        ) VALUES (
            S.datekey, S.game_sub_user_name, S.reg_datekey, S.reg_datediff, S.reg_country_code,
            S.game_sub_user_reg_datekey, S.game_user_level, S.server_name,
            S.item_kind, S.action_type, S.is_auto_exchange, S.registered_at,
            S.exchange_count, S.draconic_amount, S.draconic_fee,
            S.unit_price_draconic, S.avg_holding_minutes, S.action_cnt
        )
        """

        query_job = client.query(query)

        try:
            query_job.result()
            print(f"✅ 쿼리 실행 성공! (Job ID: {query_job.job_id})")
            print(f"■ {td_str} f_game_exchange Batch 완료")
        except Exception as e:
            print(f"❌ 쿼리 실행 중 에러 발생: {e}")
            raise e

    print("✅ f_game_exchange ETL 완료")
    return True
