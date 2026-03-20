import requests
import pandas as pd
import time
import os
from datetime import datetime, timedelta
from databricks import sql
from airflow import DAG, Dataset
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models import Variable
from typing import Any, cast

injoy_notion_permission_producer = Dataset('injoy_notion_permission_producer')

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
}

with DAG(
    dag_id='injoy_notion_permission_consumer',
    default_args=default_args,
    description='Databricks 데이터를 Notion DB에 동기화하는 DAG',
    schedule= [injoy_notion_permission_producer],
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['notion', 'sync', 'databricks'],
) as dag:

    # ==================== 유틸리티 함수 ====================
    
    def get_var(key: str, default: str | None = None, required: bool = False) -> str:
        """환경 변수 → Airflow Variable 순서로 조회"""
        env_value = os.environ.get(key)
        if env_value:
            print(f"✓ 환경 변수에서 {key} 로드됨")
            return env_value
        
        try:
            try:
                var_value = Variable.get(key, None)
            except TypeError:
                var_value = Variable.get(key, None)
            
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
        return ''

    def get_notion_headers():
        """Notion API 헤더 생성"""
        return {
            "Authorization": f"Bearer {get_var('NOTION_TOKEN', required=True)}",
            "Notion-Version": get_var("NOTION_API_VERSION", "2022-06-28"),
            "Content-Type": "application/json"
        }

    def get_databricks_connection():
        """Databricks 연결 설정 반환"""
        return {
            'server_hostname': get_var('DATABRICKS_SERVER_HOSTNAME', required=True),
            'http_path': get_var('databricks_http_path', required=True),
            'access_token': get_var('databricks_token', required=True)
        }

    # ==================== Notion 함수 ====================

    MULTI_SELECT_KEYS = {
        "user": ["그룹 권한", "권한(에셋타입|에셋명|에셋권한)"],
        "group": ["그룹 권한", "권한(에셋타입|에셋명|에셋권한)"],
        "asset": ["에셋권한"]
    }
    
    RELATION_KEYS = {
        "user": ["소속 그룹"],
        "group": [],
        "asset": ["권한 획득 그룹", "권한 획득 개인"]
    }

    def generate_row_key(row: dict, db_type: str) -> str:
        """복합 키 생성"""
        if db_type == "user":
            return f"{row.get('유저 이름','').strip()}_{row.get('email','').strip()}"
        elif db_type == "group":
            return f"{row.get('그룹이름','').strip()}"
        elif db_type == "asset":
            asset_type = row.get('에셋 타입','').strip()
            asset_name = row.get('에셋명','').strip()
            
            permissions = str(row.get('에셋권한','')).strip()
            sorted_permissions = ",".join(sorted(permissions.split(',')))

            groups = str(row.get('권한 획득 그룹','')).strip()
            sorted_groups = ",".join(sorted(groups.split(',')))

            users = str(row.get('권한 획득 개인','')).strip()
            sorted_users = ",".join(sorted(users.split(',')))

            return f"{asset_type}_{asset_name}_{sorted_permissions}_{sorted_groups}_{sorted_users}"
        return ""

    def get_all_notion_pages(database_id: str, headers: dict):
        """Notion DB 전체 조회"""
        url = f"https://api.notion.com/v1/databases/{database_id}/query"
        results = []
        has_more = True
        next_cursor = None
        
        while has_more:
            payload = {"page_size": 100}
            if next_cursor:
                payload["start_cursor"] = next_cursor
            try:
                res = requests.post(url, headers=headers, json=payload)
                res.raise_for_status()
                data = res.json()
                results.extend(data.get("results", []))
                next_cursor = data.get("next_cursor")
                has_more = data.get("has_more", False)
            except requests.exceptions.RequestException as e:
                print(f"❌ Notion API Error: {e}")
                break
            time.sleep(0.3)
        return results

    def extract_notion_data_map(pages, db_type, id_to_name_maps=None):
        """Notion 페이지 → 복합 키 기반 dict 생성"""
        page_dict = {}
        if id_to_name_maps is None:
            id_to_name_maps = {}
        
        group_id_map = id_to_name_maps.get("group", {})
        user_id_map = id_to_name_maps.get("user", {})

        for page in pages:
            props = page.get("properties", {})
            row = {}
            
            for key, val in props.items():
                if "title" in val:
                    row[key] = val["title"][0]["plain_text"] if val["title"] else ""
                elif "rich_text" in val:
                    row[key] = val["rich_text"][0]["plain_text"] if val["rich_text"] else ""
                elif "multi_select" in val:
                    row[key] = ",".join(sorted([opt["name"] for opt in val.get("multi_select", [])]))
                elif "relation" in val:
                    ids = [rel.get("id") for rel in val.get("relation", [])]
                    names = []
                    if (key == '소속 그룹' or key == '권한 획득 그룹') and group_id_map:
                        names = [group_id_map.get(id, '') for id in ids]
                    elif key == '권한 획득 개인' and user_id_map:
                        names = [user_id_map.get(id, '') for id in ids]
                    row[key] = ",".join(sorted([name for name in names if name]))
            
            row_key = generate_row_key(row, db_type)
            if row_key:
                page_dict[row_key] = {"page_id": page["id"], "values": row}
        
        return page_dict

    def build_properties_payload(row_data, db_type, group_name_to_id_map, user_name_to_id_map):
        """Notion 속성 페이로드 생성"""
        properties = {}
        title_key = {"user": "유저 이름", "group": "그룹이름", "asset": "에셋 타입"}.get(db_type)

        for key, value in row_data.items():
            if key == title_key:
                properties[key] = {"title": [{"text": {"content": str(value or "")}}]}
            elif key in RELATION_KEYS.get(db_type, []):
                related_ids = []
                names = [s.strip() for s in str(value).split(',') if s.strip()]
                if key == '권한 획득 개인':
                    for name in names:
                        page_id = user_name_to_id_map.get(name)
                        if page_id:
                            related_ids.append(page_id)
                elif key == '소속 그룹' or key == '권한 획득 그룹':
                    for name in names:
                        page_id = group_name_to_id_map.get(name)
                        if page_id:
                            related_ids.append(page_id)
                properties[key] = {"relation": [{"id": pid} for pid in related_ids]}
            elif key in MULTI_SELECT_KEYS.get(db_type, []):
                delimiter = ',' if db_type == 'asset' and key == '에셋권한' else '//'
                items = [s.strip() for s in str(value or "").split(delimiter) if s.strip()]
                properties[key] = {"multi_select": [{"name": item} for item in items]}
            else:
                properties[key] = {"rich_text": [{"text": {"content": str(value or "")}}]}
        
        return properties

    def has_data_changed(source_row, notion_row, db_type):
        """데이터 변경 여부 비교"""
        for key, new_value in source_row.items():
            existing_value = notion_row.get(key, "")
            
            if key in MULTI_SELECT_KEYS.get(db_type, []):
                delimiter_for_source = '//'
                if db_type == 'asset' and key == '에셋권한':
                    delimiter_for_source = ','

                new_value_sorted = ",".join(sorted(str(new_value or "").split(delimiter_for_source)))
                existing_value_sorted = ",".join(sorted(str(existing_value or "").split(',')))

                if new_value_sorted != existing_value_sorted:
                    return True
            elif str(new_value or "") != str(existing_value or ""):
                return True
        
        return False

    def deduplicate_notion_rows(database_id: str, db_type: str, headers: dict, id_to_name_maps=None):
        """중복 제거"""
        print(f"\n📦 [{db_type.upper()}] Starting final deduplication process...")
        pages = get_all_notion_pages(database_id, headers)
        notion_data_map = extract_notion_data_map(pages, db_type, id_to_name_maps)
        
        key_to_page_ids = {}
        for page in pages:
            temp_row = {}
            for key, val in page.get("properties", {}).items():
                if "title" in val:
                    temp_row[key] = val["title"][0]["plain_text"] if val["title"] else ""
                elif "rich_text" in val:
                    temp_row[key] = val["rich_text"][0]["plain_text"] if val["rich_text"] else ""
                elif "multi_select" in val:
                    temp_row[key] = ",".join(sorted([opt["name"] for opt in val.get("multi_select", [])]))
                elif "relation" in val:
                    ids = [rel.get("id") for rel in val.get("relation", [])]
                    names = []
                    if id_to_name_maps:
                        group_id_map = id_to_name_maps.get("group", {})
                        user_id_map = id_to_name_maps.get("user", {})
                        if (key == '소속 그룹' or key == '권한 획득 그룹') and group_id_map:
                            names = [group_id_map.get(id, '') for id in ids]
                        elif key == '권한 획득 개인' and user_id_map:
                            names = [user_id_map.get(id, '') for id in ids]
                    temp_row[key] = ",".join(sorted([name for name in names if name]))

            row_key = generate_row_key(temp_row, db_type)
            if row_key:
                key_to_page_ids.setdefault(row_key, []).append(page["id"])

        for row_key, page_ids in key_to_page_ids.items():
            if len(page_ids) > 1:
                for dup_page_id in page_ids[1:]:
                    res = requests.patch(
                        f"https://api.notion.com/v1/pages/{dup_page_id}",
                        headers=headers,
                        json={"archived": True}
                    )
                    if res.ok:
                        print(f"🗑️ Archived duplicate ({db_type}) row: {row_key} (page: {dup_page_id})")
                    else:
                        print(f"❌ Failed to archive duplicate: {dup_page_id} - {res.text}")
                    time.sleep(0.3)
        
        print(f"✅ [{db_type.upper()}] Duplicate cleanup completed.")

    def run_sync_for_db(db_type, db_id, source_df, headers, dependency_maps=None):
        """DB 동기화 실행"""
        if dependency_maps is None:
            dependency_maps = {}
        
        print("\n" + "="*30 + f"\n🛡️ Starting {db_type.upper()} DB Synchronization...\n" + "="*30)

        # 데이터 준비
        print(f"☁️ [{db_type.upper()}] Preparing data maps...")
        source_map = {generate_row_key(row, db_type): row for row in source_df.to_dict("records")}
        
        notion_pages = get_all_notion_pages(db_id, headers)
        notion_map = extract_notion_data_map(notion_pages, db_type, dependency_maps)
        
        group_name_to_id_map = dependency_maps.get("group_name_to_id", {})
        user_name_to_id_map = dependency_maps.get("user_name_to_id", {})

        print(f"Found {len(notion_map)} rows in Notion. Source has {len(source_map)} rows.")

        # 작업 목록 생성
        to_insert_keys = source_map.keys() - notion_map.keys()
        to_delete_keys = notion_map.keys() - source_map.keys()
        to_check_update_keys = source_map.keys() & notion_map.keys()

        print(f"Sync plan: {len(to_insert_keys)} to insert, {len(to_delete_keys)} to delete, {len(to_check_update_keys)} to check for updates.")

        # Insert
        print(f"\n🚀 [{db_type.upper()}] Inserting {len(to_insert_keys)} new rows...")
        for key in to_insert_keys:
            row_data = source_map[key]
            properties = build_properties_payload(row_data, db_type, group_name_to_id_map, user_name_to_id_map)
            payload = {"parent": {"database_id": db_id}, "properties": properties}
            res = requests.post("https://api.notion.com/v1/pages", headers=headers, json=payload)
            if not res.ok:
                print(f"❌ Insert failed ({key}): {res.text}")
            time.sleep(0.3)

        # Update
        print(f"\n🚀 [{db_type.upper()}] Checking {len(to_check_update_keys)} rows for updates...")
        update_count = 0
        for key in to_check_update_keys:
            source_row = source_map[key]
            notion_row = notion_map[key]
            if has_data_changed(source_row, notion_row['values'], db_type):
                update_count += 1
                properties = build_properties_payload(source_row, db_type, group_name_to_id_map, user_name_to_id_map)
                page_id = notion_row['page_id']
                res = requests.patch(
                    f"https://api.notion.com/v1/pages/{page_id}",
                    headers=headers,
                    json={"properties": properties}
                )
                if not res.ok:
                    print(f"❌ Update failed ({key}): {res.text}")
                time.sleep(0.3)
        print(f"Found and performed {update_count} updates.")

        # Delete
        print(f"\n🧹 [{db_type.upper()}] Deleting {len(to_delete_keys)} removed rows...")
        for key in to_delete_keys:
            page_id = notion_map[key]['page_id']
            res = requests.patch(
                f"https://api.notion.com/v1/pages/{page_id}",
                headers=headers,
                json={"archived": True}
            )
            if not res.ok:
                print(f"❌ Delete failed ({key}): {res.text}")
            time.sleep(0.3)

        # 중복 제거
        deduplicate_notion_rows(db_id, db_type, headers, dependency_maps)
        print(f"\n✅ {db_type.upper()} DB Synchronization Finished.")

    # ==================== Task 함수 ====================

    def load_user_permissions(**context):
        """사용자별 권한 데이터 로드"""
        print("📊 Loading user permissions from Databricks...")
        
        config = get_databricks_connection()
        query = """
        SELECT
            user_name,
            email,
            REPLACE(concat_ws('// ', collect_list(CONCAT_WS(' | ', object_type, object_name, privileges))), ',', ' &') AS assets_with_permissions,
            all_groups
        FROM datahub.injoy_ops_schema.user_permission_snapshot
        GROUP BY user_name, email, all_groups
        ORDER BY user_name
        """
        
        with sql.connect(**config) as conn:
            df = pd.read_sql(query, cast(Any, conn))
        
        df = df.rename(columns={
            "user_name": "유저 이름",
            "email": "email",
            "all_groups": "소속 그룹",
            "assets_with_permissions": "권한(에셋타입|에셋명|에셋권한)"
        })
        
        print(f"✅ Loaded {len(df)} user permission records")
        context['task_instance'].xcom_push(key='df_user_perm', value=df.to_json(orient='records'))

    def load_group_permissions(**context):
        """그룹별 권한 데이터 로드"""
        print("📊 Loading group permissions from Databricks...")
        
        config = get_databricks_connection()
        query = """
        SELECT 
          inherited_group_name,
          REPLACE(
            MIN(CASE 
                WHEN TRIM(inherited_entitlements) = '' THEN 'N/A'
                ELSE TRIM(inherited_entitlements)
                END)
            , ',', '//' ) AS inherited_entitlements,
          REPLACE(concat_ws('// ', collect_list(CONCAT_WS(' | ', object_type, object_name, privileges))), ',', ' &') AS assets_with_permissions 
        FROM (
          SELECT DISTINCT
            inherited_group_name,
            inherited_entitlements,
            object_type,
            object_name,
            privileges
          FROM datahub.injoy_ops_schema.user_permission_snapshot
        )
        WHERE TRIM(inherited_group_name) != ''
        GROUP BY inherited_group_name, inherited_entitlements
        ORDER BY inherited_group_name
        """
        
        with sql.connect(**config) as conn:
            df = pd.read_sql(query, cast(Any, conn))
        
        df = df.rename(columns={
            "inherited_group_name": "그룹이름",
            "inherited_entitlements": "그룹 권한", 
            "assets_with_permissions": "권한(에셋타입|에셋명|에셋권한)"
        })
        
        print(f"✅ Loaded {len(df)} group permission records")
        context['task_instance'].xcom_push(key='df_group_perm', value=df.to_json(orient='records'))

    def load_asset_permissions(**context):
        """에셋별 권한 데이터 로드"""
        print("📊 Loading asset permissions from Databricks...")
        
        config = get_databricks_connection()
        query = """
        SELECT DISTINCT 
            object_type, 
            object_name, 
            privileges,
            CASE WHEN TRIM(inherited_group_name) = '' THEN '개인'
                 ELSE TRIM(inherited_group_name)
            END AS inherited_group_name,
            IF(TRIM(inherited_group_name) = '', user_name, '') AS inherited_user_name 
        FROM datahub.injoy_ops_schema.user_permission_snapshot
        ORDER BY object_type, object_name, privileges, inherited_group_name
        """
        
        with sql.connect(**config) as conn:
            df = pd.read_sql(query, cast(Any, conn))
        
        df = df.rename(columns={
            "object_type": "에셋 타입",
            "object_name": "에셋명",
            "privileges": "에셋권한",
            "inherited_group_name": "권한 획득 그룹",
            "inherited_user_name": "권한 획득 개인"
        })
        
        print(f"✅ Loaded {len(df)} asset permission records")
        context['task_instance'].xcom_push(key='df_asset_perm', value=df.to_json(orient='records'))

    def sync_group_to_notion(**context):
        """그룹 데이터를 Notion에 동기화"""
        print("🔄 Syncing groups to Notion...")
        
        headers = get_notion_headers()
        db_id = get_var("NOTION_DB_ID_GROUP", "22aea67a568180cda6e4dae66bf7459b")
        
        df_json = context['task_instance'].xcom_pull(task_ids='load_group_permissions', key='df_group_perm')
        df = pd.read_json(df_json, orient='records')
        
        run_sync_for_db("group", db_id, df, headers)
        print("✅ Group sync completed")

    def sync_user_to_notion(**context):
        """사용자 데이터를 Notion에 동기화"""
        print("🔄 Syncing users to Notion...")
        
        headers = get_notion_headers()
        db_id_user = get_var("NOTION_DB_ID_USER", "22cea67a568180a7bfd6f070aa04fa30")
        db_id_group = get_var("NOTION_DB_ID_GROUP", "22aea67a568180cda6e4dae66bf7459b")
        
        df_json = context['task_instance'].xcom_pull(task_ids='load_user_permissions', key='df_user_perm')
        df = pd.read_json(df_json, orient='records')
        
        # 그룹 의존성 맵 준비
        final_group_pages = get_all_notion_pages(db_id_group, headers)
        final_group_map = extract_notion_data_map(final_group_pages, "group")
        group_id_to_name = {v['page_id']: v['values']['그룹이름'] for k, v in final_group_map.items()}
        group_name_to_id = {v: k for k, v in group_id_to_name.items()}
        
        user_dependency_maps = {
            "group": group_id_to_name,
            "group_name_to_id": group_name_to_id
        }
        
        run_sync_for_db("user", db_id_user, df, headers, user_dependency_maps)
        print("✅ User sync completed")

    def sync_asset_to_notion(**context):
        """에셋 데이터를 Notion에 동기화"""
        print("🔄 Syncing assets to Notion...")
        
        headers = get_notion_headers()
        db_id_asset = get_var("NOTION_DB_ID_ASSET", "22dea67a5681808d9672e4bfe621045c")
        db_id_group = get_var("NOTION_DB_ID_GROUP", "22aea67a568180cda6e4dae66bf7459b")
        db_id_user = get_var("NOTION_DB_ID_USER", "22cea67a568180a7bfd6f070aa04fa30")

        df_json = context['task_instance'].xcom_pull(task_ids='load_asset_permissions', key='df_asset_perm')
        df = pd.read_json(df_json, orient='records')
        
        # 그룹 의존성 맵 준비
        final_group_pages = get_all_notion_pages(db_id_group, headers)
        final_group_map = extract_notion_data_map(final_group_pages, "group")
        group_id_to_name = {v['page_id']: v['values']['그룹이름'] for k, v in final_group_map.items()}
        group_name_to_id = {v: k for k, v in group_id_to_name.items()}
        
        # 사용자 의존성 맵 준비
        user_dependency_maps = {"group": group_id_to_name, "group_name_to_id": group_name_to_id}
        final_user_pages = get_all_notion_pages(db_id_user, headers)
        final_user_map = extract_notion_data_map(final_user_pages, "user", user_dependency_maps)
        user_id_to_name = {v['page_id']: v['values']['유저 이름'] for k, v in final_user_map.items()}
        user_name_to_id = {v: k for k, v in user_id_to_name.items()}
        
        asset_dependency_maps = {
            "group": group_id_to_name,
            "user": user_id_to_name,
            "group_name_to_id": group_name_to_id,
            "user_name_to_id": user_name_to_id
        }
        
        run_sync_for_db("asset", db_id_asset, df, headers, asset_dependency_maps)
        print("✅ Asset sync completed")

    # ==================== Task 정의 ====================

    task_load_user = PythonOperator(
        task_id='load_user_permissions',
        python_callable=load_user_permissions,
        dag=dag,
    )

    task_load_group = PythonOperator(
        task_id='load_group_permissions',
        python_callable=load_group_permissions,
        dag=dag,
    )

    task_load_asset = PythonOperator(
        task_id='load_asset_permissions',
        python_callable=load_asset_permissions,
        dag=dag,
    )

    task_sync_group = PythonOperator(
        task_id='sync_group_to_notion',
        python_callable=sync_group_to_notion,
        dag=dag,
    )

    task_sync_user = PythonOperator(
        task_id='sync_user_to_notion',
        python_callable=sync_user_to_notion,
        dag=dag,
    )

    task_sync_asset = PythonOperator(
        task_id='sync_asset_to_notion',
        python_callable=sync_asset_to_notion,
        dag=dag,
    )

    # ==================== Task 의존성 ====================
    
    # 데이터 로드는 병렬 실행
    [task_load_user, task_load_group, task_load_asset]
    
    # 그룹 동기화 먼저 실행
    task_load_group >> task_sync_group
    
    # 사용자 동기화는 그룹 동기화 이후
    [task_load_user, task_sync_group] >> task_sync_user
    
    # 에셋 동기화는 그룹, 사용자 동기화 이후
    [task_load_asset, task_sync_user] >> task_sync_asset