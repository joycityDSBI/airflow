from datetime import datetime, timezone, timedelta
import pytz


def etl_f_d2d_item_flow(target_date: list, client):
    """
    RESU 아이템 획득/소비 Fact (`datahub-478802.RESU.f_d2d_item_flow`) ETL.

    소스:
      - dataplatform-204306.RESU.T_DM_Common              (UTC 기준 로그)
      - datahub-478802.datahub.f_common_register_char     (joyple_game_code, auth_account_name 매핑)

    적재 방식:
      datekey 파티션 DELETE -> INSERT (BEGIN/COMMIT 트랜잭션, idempotent)
    """

    kst = pytz.timezone('Asia/Seoul')

    for td_str in target_date:
        try:
            current_date_obj = datetime.strptime(td_str, "%Y-%m-%d")
        except ValueError:
            print(f"⚠️ 날짜 형식이 잘못되었습니다: {td_str}")
            continue

        # KST 하루를 UTC 윈도우로 환산 (T_DM_Common.LogDate는 UTC TIMESTAMP)
        start_kst = kst.localize(current_date_obj)
        start_utc = start_kst.astimezone(timezone.utc)
        end_kst   = start_kst + timedelta(days=1)
        end_utc   = end_kst.astimezone(timezone.utc)

        print(f"📝 대상날짜(KST): {td_str}")
        print(f"   ㄴ 시작시간(UTC): {start_utc}")
        print(f"   ㄴ 종료시간(UTC): {end_utc}")

        query = f"""
        BEGIN TRANSACTION;

        DELETE FROM `datahub-478802.RESU.f_d2d_item_flow`
        WHERE datekey = DATE('{td_str}');

        INSERT INTO `datahub-478802.RESU.f_d2d_item_flow` (
            datekey, joyple_game_code, server_name, game_sub_user_name, auth_account_name,
            game_user_level, reason, act_detail_id, item_kind, item_amount,
            used_item_kind, used_item_amount
        )
        WITH base AS (
            SELECT
                DATE('{td_str}')            AS datekey,
                CAST(t.WorldID AS STRING)   AS server_name,
                CAST(t.UserID AS STRING)    AS game_sub_user_name,
                CAST(t.UserLevel AS INT64)  AS game_user_level,
                t.Reason                    AS reason,
                t.p0                        AS act_detail_id,
                t.p1                        AS item_kind,
                t.p2                        AS item_amount,
                t.p5                        AS used_item_kind,
                t.p6                        AS used_item_amount
            FROM `dataplatform-204306.RESU.T_DM_Common` AS t
            WHERE t.LogDate >= TIMESTAMP('{start_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
              AND t.LogDate <  TIMESTAMP('{end_utc.strftime("%Y-%m-%d %H:%M:%S %Z")}')
        )
        SELECT
            b.datekey,
            reg.joyple_game_code,
            b.server_name,
            b.game_sub_user_name,
            reg.auth_account_name,
            b.game_user_level,
            b.reason,
            b.act_detail_id,
            b.item_kind,
            SUM(b.item_amount)      AS item_amount,
            b.used_item_kind,
            SUM(b.used_item_amount) AS used_item_amount
        FROM base AS b
        LEFT JOIN `datahub-478802.datahub.f_common_register_char` AS reg
            ON  reg.joyple_game_code IN (159, 1590)
            AND reg.game_sub_user_name = b.game_sub_user_name
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 11;

        COMMIT TRANSACTION;
        """

        query_job = client.query(query)

        try:
            query_job.result()
            print(f"✅ 쿼리 실행 성공! (Job ID: {query_job.job_id})")
            print(f"■ {td_str} f_d2d_item_flow Batch 완료")
        except Exception as e:
            print(f"❌ 쿼리 실행 중 에러 발생: {e}")
            raise e

    print("✅ f_d2d_item_flow ETL 완료")
    return True