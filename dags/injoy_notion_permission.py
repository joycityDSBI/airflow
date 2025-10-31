import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
import json
import pytz
from databricks import sql
import os
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

with DAG(
    dag_id='injoy_monitoringdata_consumer_2',
    default_args=default_args,
    description='Databricks 데이터를 Notion DB에 동기화하는 DAG',
    schedule= '10 23 * * *', # 매일 새벽 2시 10분 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['notion', 'sync', 'monitoring'],
) as dag:


    def get_var(key: str, default: str = None, required: bool = False) -> str:
        """환경 변수 → Airflow Variable 순서로 조회"""
        env_value = os.environ.get(key)
        if env_value:
            print(f"✓ 환경 변수에서 {key} 로드됨")
            return env_value
        
        try:
            try:
                var_value = Variable.get(key, default=None)
            except TypeError:
                var_value = Variable.get(key, default_var=None)
            
            if var_value:
                print(f"✓ Airflow Variable에서 {key} 로드됨")
                return var_value
        except Exception as e:
            print(f"⚠️  Variable.get({key}) 오류: {str(e)}")
        
        if default is not None:
            print(f"ℹ️  기본값으로 {key} 설정됨: {default}")
            return default
        
        if required:
            raise ValueError(f"필수 설정 {key}을(를) 찾을 수 없습니다.")
        
        print(f"ℹ️  {key} 값을 찾을 수 없습니다 (선택사항)")
        return None

    def get_notion_headers():
        """Notion API 헤더 생성"""
        return {
            "Authorization": f"Bearer {get_var('NOTION_TOKEN', required=True)}",
            "Notion-Version": get_var("NOTION_API_VERSION", "2022-06-28"),
            "Content-Type": "application/json"
        }

    NOTION_HEADERS = get_notion_headers()
    DATABRICKS_INSTANCE = get_var("databricks_instance", required=True)
    DATABRICKS_TOKEN = get_var("databricks_token", required=True)
    url = f"{DATABRICKS_INSTANCE}/api/2.0/token/list"

    DATABRICKS_HEADERS = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }

    def tokenize_databricks(url, headers):
        
        resp = requests.get(url, headers=headers)

        if resp.status_code != 200:
            print(f"❌ 토큰 조회 실패: {resp.status_code}")
            print(resp.text)
        else:
            tokens = resp.json().get("token_infos", [])
            
            def convert_ms(ms):
                if ms == -1:
                    return None
                return datetime.fromtimestamp(ms / 1000)

            for t in tokens:
                creation = convert_ms(t.get("creation_time"))
                expiry = convert_ms(t.get("expiry_time"))
                if creation:
                    print(f"   생성시간: {creation.strftime('%Y-%m-%d %H:%M:%S')}")
                else:
                    print("   생성시간: 알 수 없음")
                if expiry:
                    print(f"   만료시간: {expiry.strftime('%Y-%m-%d %H:%M:%S')}")
                else:
                    print("   만료시간: 무기한 또는 없음")

    def search_databricks_users(**context):

        try:
            user_url = f"{DATABRICKS_INSTANCE}/api/2.0/preview/scim/v2/Users"
            user_resp = requests.get(user_url, headers=DATABRICKS_HEADERS)
            user_resp.raise_for_status()
            user_list = user_resp.json().get("Resources", [])
            user_data = []

            for user in user_list:
                user_data.append({
                    "user_id": user.get("id"),
                    "user_name": user.get("userName"),
                    "display_name": user.get("displayName"),
                    "email": next((e.get("value") for e in user.get("emails", []) if e.get("primary")), None),
                    "groups": ", ".join([g.get("display") for g in user.get("groups", []) if g.get("display")])
                })
            df_users = pd.DataFrame(user_data)
            print("✅ 사용자 정보 수집 완료")
        except Exception as e:
            print(f"❌ 사용자 정보 조회 실패: {e}")
            df_users = pd.DataFrame()
        
        return df_users


    def search_databricks_groups(**context):
        try:
            group_url = f"{DATABRICKS_INSTANCE}/api/2.0/preview/scim/v2/Groups"
            group_resp = requests.get(group_url, headers=DATABRICKS_HEADERS)
            group_resp.raise_for_status()
            groups = group_resp.json().get("Resources", [])
            group_data = []

            for g in groups:
                group_data.append({
                    "group_name": g.get("displayName"),
                    "group_id": g.get("id"),
                    "entitlements": ", ".join([e.get("value") for e in g.get("entitlements", []) if e.get("value")])
                })

            df_groups = pd.DataFrame(group_data)
            print("✅ 그룹 정보 수집 완료")
        except Exception as e:
            print(f"❌ 그룹 정보 조회 실패: {e}")
            df_groups = pd.DataFrame()

        return df_groups


    # 3️⃣ Unity Catalog 오브젝트 정보 수집 (datahub만 수집, datahub//information_schema 제외)
    def search_unity_catalog_object(**context):

        catalog_objects = []

        try:
            catalog_name = "datahub"  # ✅ datahub만 수집
            print(f"📌 Catalog 대상: {catalog_name}")
            
            # catalog 추가
            catalog_objects.append({"type": "catalog", "full_name": catalog_name})

            # schema 수집 (단, information_schema 제외)
            schemas_url = f"{DATABRICKS_INSTANCE}/api/2.1/unity-catalog/schemas?catalog_name={catalog_name}"
            sch_resp = requests.get(schemas_url, headers=DATABRICKS_HEADERS)
            sch_resp.raise_for_status()
            schemas = sch_resp.json().get("schemas", [])

            for sch in schemas:
                if sch['name'] == 'information_schema':
                    continue  # ❌ 제외
                
                sch_full = f"{catalog_name}.{sch['name']}"
                catalog_objects.append({"type": "schema", "full_name": sch_full})

                # table 수집
                tables_url = f"{DATABRICKS_INSTANCE}/api/2.1/unity-catalog/tables?catalog_name={catalog_name}&schema_name={sch['name']}"
                tbl_resp = requests.get(tables_url, headers=DATABRICKS_HEADERS)
                tbl_resp.raise_for_status()
                tables = tbl_resp.json().get("tables", [])

                for tbl in tables:
                    tbl_full = f"{catalog_name}.{sch['name']}.{tbl['name']}"
                    catalog_objects.append({"type": "table", "full_name": tbl_full})

            df_catalog_objects = pd.DataFrame(catalog_objects)
            print("✅ Unity Catalog 오브젝트 목록 수집 완료")

        except Exception as e:
            print(f"❌ Unity Catalog 오브젝트 조회 실패: {e}")
            df_catalog_objects = pd.DataFrame()

        return df_catalog_objects


    def search_object_permissions(**context):

        # search_unity_catalog_object 함수 호출
        df_catalog_objects = search_unity_catalog_object(**context)
        permissions_data = []
        
        for _, row in df_catalog_objects.iterrows():
            try:
                url = f"{DATABRICKS_INSTANCE}/api/2.1/unity-catalog/permissions/{row['type']}/{row['full_name']}"
                perm_resp = requests.get(url, headers=DATABRICKS_HEADERS)
                perm_resp.raise_for_status()
                acl_list = perm_resp.json().get("privilege_assignments", [])

                for acl in acl_list:
                    permissions_data.append({
                        "object_type": row["type"],
                        "object_name": row["full_name"],
                        "principal": acl.get("principal"),
                        "privileges": ", ".join(acl.get("privileges", []))
                    })
            except Exception as e:
                print(f"⚠️ {row['full_name']} 권한 조회 실패: {e}")

        df_permissions = pd.DataFrame(permissions_data)
        return df_permissions

        print("✅ 오브젝트 권한 수집 완료")
        # df_permissions.head()

    def search_warehouses_dashboards_permissions(**context):
        workspace_objects = []

        # 5-1️⃣ Warehouses 수집
        try:
            warehouse_url = f"{DATABRICKS_INSTANCE}/api/2.0/sql/warehouses"
            resp = requests.get(warehouse_url, headers=DATABRICKS_HEADERS)
            resp.raise_for_status()

            for wh in resp.json().get("warehouses", []):
                workspace_objects.append({
                    "type": "warehouses",
                    "object_id": wh["id"],
                    "object_name": wh["name"]
                })
            print("✅ Warehouses 수집 완료")

        except Exception as e:
            print(f"❌ Warehouses 수집 실패: {e}")

        # 5-2️⃣ Dashboards 수집
        try:
            dashboard_url = f"{DATABRICKS_INSTANCE}/api/2.0/lakeview/dashboards"
            resp = requests.get(dashboard_url, headers=DATABRICKS_HEADERS)
            resp.raise_for_status()

            for dash in resp.json().get("dashboards", []):
                workspace_objects.append({
                    "type": "dashboards",
                    "object_id": dash["dashboard_id"],
                    "object_name": dash["display_name"]
                })
            print("✅ Dashboards 수집 완료")

        except Exception as e:
            print(f"❌ Dashboards 수집 실패: {e}")

        # 5-3️⃣ 각 오브젝트별 permission 수집
        workspace_perms = []

        for obj in workspace_objects:
            try:
                url = f"{DATABRICKS_INSTANCE}/api/2.0/permissions/{obj['type']}/{obj['object_id']}"
                resp = requests.get(url, headers=DATABRICKS_HEADERS)
                resp.raise_for_status()

                acl_list = resp.json().get("access_control_list", [])

                for entry in acl_list:
                    workspace_perms.append({
                        "object_type": obj["type"],
                        "object_name": obj["object_name"],
                        "principal": entry.get("user_name") or entry.get("group_name"),
                        "privileges": ", ".join([
                            perm.get("permission_level") for perm in entry.get("all_permissions", [])
                        ])
                    })

            except Exception as e:
                print(f"⚠️ {obj['type']} - {obj['object_name']} 권한 조회 실패: {e}")

        # DataFrame으로 정리
        df_workspace_perms = pd.DataFrame(workspace_perms)
        return df_workspace_perms
        
        print("✅ Warehouses & Dashboards 권한 수집 완료")
        

    def complex_all_permissions(**context):
        """모든 권한 정보 통합"""

        # 🔁 6️⃣ 권한 정보 통합

        df_workspace_perms = search_warehouses_dashboards_permissions(**context)
        df_permissions = search_object_permissions(**context)
        df_users = search_databricks_users(**context)
        df_groups = search_databricks_groups(**context)

        df_all_permissions = pd.concat([df_permissions, df_workspace_perms], ignore_index=True)

        # 6-1️⃣ 직접 권한 (사용자에게 직접 부여된 권한)
        direct_perms = df_all_permissions[df_all_permissions["principal"].isin(df_users["user_name"])].copy()
        direct_perms = direct_perms[direct_perms["object_name"].notnull()]  # user에게 직접 부여된 경우 object_name 없으면 제거

        df_direct = pd.merge(
            direct_perms,
            df_users,
            left_on="principal",
            right_on="user_name",
            how="left"
        )

        # 직접 권한이므로 그룹 기반 inherited 정보는 비워두기
        df_direct["inherited_group_name"] = ""
        df_direct["inherited_group_id"] = ""
        df_direct["inherited_entitlements"] = ""

        # 칼럼 순서 재정렬
        df_direct = df_direct[[
            "user_id", "display_name", "email", "groups",  # 사용자 기본 정보
            "inherited_group_name", "inherited_group_id", "inherited_entitlements",  # 상속 정보
            "object_name", "object_type", "privileges"  # 권한 정보
        ]]

        print("✅ df_direct (직접 권한) 생성 완료")

        # ✅ 6-2️⃣ 그룹 기반 권한 (그룹에 부여된 권한 → 사용자에게 상속)
        group_perms = df_all_permissions[df_all_permissions["principal"].isin(df_groups["group_name"])].copy()

        # 그룹명 → 그룹 정보 연결
        group_perms = pd.merge(
            group_perms,
            df_groups,
            left_on="principal",
            right_on="group_name",
            how="left"
        )

        # 사용자 정보와 그룹 연결: 그룹명 → 사용자들 추출
        user_group_pairs = []
        for _, row in df_users.iterrows():
            user_groups = str(row["groups"]).split(", ")
            for g in user_groups:
                user_group_pairs.append({
                    "user_id": row["user_id"],
                    "display_name": row["display_name"],
                    "email": row["email"],
                    "groups": row["groups"],
                    "user_group_name": g
                })
        df_user_groups = pd.DataFrame(user_group_pairs)

        # 그룹 권한 → 사용자 상속 매핑
        df_group_inherited = pd.merge(
            group_perms,
            df_user_groups,
            left_on="principal",
            right_on="user_group_name",
            how="inner"
        )

        # 칼럼명 정리
        df_group_inherited["inherited_group_name"] = df_group_inherited["principal"]
        df_group_inherited["inherited_group_id"] = df_group_inherited["group_id"]
        df_group_inherited["inherited_entitlements"] = df_group_inherited["entitlements"]

        # 칼럼 순서 재정렬
        df_group_inherited = df_group_inherited[[
            "user_id", "display_name", "email", "groups",
            "inherited_group_name", "inherited_group_id", "inherited_entitlements",
            "object_name", "object_type", "privileges"
        ]]

        # ✅ 6-3️⃣ df_direct + df_group_inherited 통합
        df_final = pd.concat([df_direct, df_group_inherited], ignore_index=True)

        # ✅ 중복 제거 (같은 유저가 같은 object에 같은 권한을 여러 경로로 받을 수 있음)
        df_final.drop_duplicates(subset=[
            "user_id", "object_name", "privileges"
        ], inplace=True)

        # 현재 시각 (UTC 또는 local time, 필요에 따라 조정 가능)
        kst = pytz.timezone("Asia/Seoul")
        now_kst = datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S")

        # loaded_at 컬럼 추가
        df_final["loaded_at"] = now_kst

        df_final = df_final.rename(columns={
        "display_name": "user_name",
        "groups": "all_groups"  # 해당 유저가 속한 모든 그룹
        })

        return df_final



    def detect_and_store_changes(databricks_config=None):
        """변경 사항 감지 및 저장"""
        
        # 모든 권한 정보 통합
        df_final = complex_all_permissions()

        # Databricks 연결 정보
        config = databricks_config or {
            'server_hostname': get_var('DATABRICKS_SERVER_HOSTNAME'),
            'http_path': get_var('databricks_http_path'),
            'access_token': get_var('databricks_token')
        }
        
        target_table = "datahub.injoy_ops_schema.user_permission_snapshot"
        changes_detected = False

        # 기존 데이터 로드 시도
        try:
            with sql.connect(**config) as conn:
                df_existing = pd.read_sql(f"SELECT * FROM {target_table}", conn)
            
            print(f"✅ 기존 테이블 로드 완료")
            
            # 'loaded_at' 제외하고 비교
            compare_cols = [col for col in df_final.columns if col != 'loaded_at']
            df_new = df_final[compare_cols].sort_values(by=compare_cols).reset_index(drop=True)
            df_old = df_existing[compare_cols].sort_values(by=compare_cols).reset_index(drop=True)
            
            changes_detected = not df_new.equals(df_old)
            print(f"ℹ️ 변경 사항: {'있음' if changes_detected else '없음'}")
            
        except Exception as e:
            if "NOT_FOUND" in str(e).upper() or "NOT EXIST" in str(e).upper():
                print(f"ℹ️ 기존 테이블 없음. 첫 실행으로 간주")
                changes_detected = True
            else:
                raise e

        # 변경 사항이 있으면 저장
        if changes_detected:
            print("🚀 테이블 저장 중...")
            with sql.connect(**config) as conn:
                cursor = conn.cursor()
                
                # 테이블 재생성
                cursor.execute(f"DROP TABLE IF EXISTS {target_table}")
                
                # 컬럼 타입 매핑
                type_map = {'object': 'STRING', 'int64': 'BIGINT', 'float64': 'DOUBLE', 'bool': 'BOOLEAN'}
                cols = ', '.join([f"`{col}` {type_map.get(str(df_final[col].dtype), 'STRING')}" 
                                for col in df_final.columns])
                cursor.execute(f"CREATE TABLE {target_table} ({cols})")
                
                # SQL 값 이스케이프 처리
                def escape_sql_value(v):
                    if pd.isna(v):
                        return 'NULL'
                    s = str(v).replace("\\", "\\\\").replace("'", "''")
                    return f"'{s}'"
                
                for i in range(0, len(df_final), 1000):
                    batch = df_final.iloc[i:i+1000]
                    values = []
                    for _, row in batch.iterrows():
                        row_values = [escape_sql_value(v) for v in row]  # ✅ 수정
                        values.append(f"({', '.join(row_values)})")
                    cursor.execute(f"INSERT INTO {target_table} VALUES {', '.join(values)}")
                
                cursor.close()
            
            print(f"✅ 저장 완료: {target_table}")
            return {"has_changes": True}
        
        print("⏭️ 변경 없음. 업데이트 건너뜀")
        return {"has_changes": False}
    




    task_tokenize_databricks = PythonOperator(
        task_id='tokenize_databricks',
        python_callable=tokenize_databricks,
        op_kwargs={
            'url': url,
            'headers': {'Authorization': f"Bearer {DATABRICKS_TOKEN}"}
        },
        dag=dag,
    )

    task_detect_and_store_changes = PythonOperator(
        task_id='detect_and_store_changes',
        python_callable=detect_and_store_changes,
        op_kwargs={'databricks_config': {
            'server_hostname': get_var('DATABRICKS_SERVER_HOSTNAME'),
            'http_path': get_var('databricks_http_path'),
            'access_token': get_var('databricks_token')
        }},
        dag=dag,
    )

    task_tokenize_databricks >> task_detect_and_store_changes