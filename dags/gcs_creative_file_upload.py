# -*- coding: utf-8 -*-
"""
Google Sheets → GCS 링크 정규화 & Notion DB 업로드 DAG
일일 배치로 실행되며, 각 프로젝트별 시트를 병렬 처리합니다.
"""

from airflow import DAG, Dataset
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os
import sys
import json
try:
    import gspread
except ImportError:
    print("⚠️  gspread 모듈이 설치되지 않았습니다. 설치 중...")
    import subprocess
    subprocess.check_call(['pip', 'install', 'gspread'])
    import gspread
from typing import Any, Dict, cast


gcs_creative_file_upload = Dataset('gcs_creative_file_upload')

# 기본 DAG 설정
default_args = {
    'owner': 'ds_bi',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['65e43b85.joycity.com@kr.teams.ms'],  # 실패 시 알림 이메일
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    dag_id='gcs_creative_file_upload',
    default_args=default_args,
    description='Google Sheets 링크 정규화 및 Notion DB 자동 업로드',
    schedule='0 21 * * *',  # 매일 새벽 2시 실행 (KST 기준 조정 필요)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=['creative', 'notion', 'marketing'],
)

# =====================
# Airflow Variables에서 설정 값 로드
# =====================
def get_var(key: str, default: str | None= None) -> str:
    """환경 변수 또는 Airflow Variable 조회"""
    return os.environ.get(key) or Variable.get(key, default_var=default)

# 설정 값들
NETWORK_BASE_PATH = get_var('__network_base_path', '/mnt/creative/')
SPREADSHEET_ID = get_var('__spreadsheet_id', '1n2-pedl4-QH8Jo4poqPk62ynyaezMaM0dKuWitRqyDo')
CREDENTIALS_JSON = get_var('GOOGLE_CREDENTIAL_JSON')
GCS_BUCKET = get_var('__gcs_bucket', 'ua_creative_files')
NOTION_API_KEY = get_var('NOTION_TOKEN_FOR_CREATIVE')
NOTION_DB_ID = get_var('NOTION_DB_ID_UA_CREATIVE_FILE')

TARGET_SHEETS = ['POTC', 'GBTW', 'WWMC', 'DRSG', 'DRB', 'JTWN', 'BSTD', 'RESU']

cred_dict = json.loads(CREDENTIALS_JSON)

if 'private_key' in cred_dict:
    private_key = cred_dict['private_key']
    # 만약 \\n (이중 이스케이프)로 저장되어 있다면
    if '\\n' in private_key:
        cred_dict['private_key'] = private_key.replace('\\n', '\n')
    # 또는 공백이나 다른 문자로 깨진 경우
    elif 'BEGIN PRIVATE KEY-----' in private_key and '\n' not in private_key:
        print("⚠️ Private key에 줄바꿈이 없습니다. 수정 중...")
        # 수동으로 줄바꿈 추가
        cred_dict['private_key'] = private_key.replace(
            '-----BEGIN PRIVATE KEY-----', 
            '-----BEGIN PRIVATE KEY-----\n'
        ).replace(
            '-----END PRIVATE KEY-----', 
            '\n-----END PRIVATE KEY-----\n'
        )

def check_dependencies(**context):
    """필수 라이브러리 및 환경 확인"""
    import subprocess
    
    required_packages = ['gspread', 'google-cloud-storage', 'notion-client']
    
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
        except ImportError:
            print(f"⚠️  {package} 설치 중...")
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', package])
    
    # 네트워크 경로 확인
    if not os.path.exists(NETWORK_BASE_PATH):
        print(f"⚠️  네트워크 경로가 존재하지 않습니다: {NETWORK_BASE_PATH}")
    
    print("✅ 환경 검증 완료")


def initialize_clients(**context):
    """Google Sheets, GCS, Notion 클라이언트 초기화"""
    import gspread
    from google.oauth2.service_account import Credentials
    from google.cloud import storage
    from notion_client import Client as NotionClient
    
    # Google Sheets 인증
    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    
    sheet_creds = Credentials.from_service_account_info(cred_dict, scopes=SCOPES)
    gc = gspread.authorize(sheet_creds)
    
    # GCS 클라이언트
    storage_client = storage.Client.from_service_account_info(cred_dict)
    bucket = storage_client.bucket(GCS_BUCKET)
    
    # Notion 클라이언트
    notion = NotionClient(auth=NOTION_API_KEY)
    
    # XCom에 상태 저장
    context['ti'].xcom_push(key='clients_initialized', value=True)
    
    print("✅ 클라이언트 초기화 완료")
    return {
        'sheets_initialized': True,
        'gcs_initialized': True,
        'notion_initialized': True
    }


def fetch_notion_schema(**context):
    """Notion DB 스키마 조회 및 기존 소재명 목록 로드"""
    from notion_client import Client as NotionClient
    import unicodedata
    import re
    
    def nospace_lower(s: str) -> str:
        if s is None: return ""
        s = unicodedata.normalize('NFKC', str(s))
        s = re.sub(r'\s+', '', s)
        return s.lower()
    
    def get_plain_text_from_prop(prop_type: str, prop_value: dict) -> str:
        if not prop_value: return ""
        try:
            if prop_type in ("title", "rich_text"):
                arr = prop_value.get(prop_type, [])
                return "".join([t.get("plain_text", "") for t in arr]).strip()
            if prop_type == "select":
                opt = prop_value.get("select")
                return (opt or {}).get("name", "").strip() if opt else ""
            if prop_type == "multi_select":
                arr = prop_value.get("multi_select", [])
                return ", ".join([(o or {}).get("name", "").strip() for o in arr if o])
            if prop_type == "url":
                return (prop_value.get("url") or "").strip()
            if prop_type == "number":
                n = prop_value.get("number")
                return str(n).strip() if n is not None else ""
            if prop_type == "date":
                d = prop_value.get("date", {})
                return (d.get("start") or "").strip()
            return str(prop_value).strip()
        except Exception:
            return ""
    
    notion = NotionClient(auth=NOTION_API_KEY)
    
    # DB 스키마 조회
    info = cast(Dict[str, Any], notion.databases.retrieve(database_id=NOTION_DB_ID))
    props = info.get("properties", {}) # ignore

    

    prop_by_clean = {}
    title_prop_name = None
    project_prop = None
    sojae_prop_meta = None
    
    for name, meta in props.items():
        ptype = meta.get("type")
        clean = nospace_lower(name)
        if ptype == "title":
            title_prop_name = name
        if clean == nospace_lower('프로젝트'):
            project_prop = {"name": name, "type": ptype, "meta": meta}
        if clean == nospace_lower('소재명'):
            sojae_prop_meta = {"name": name, "type": ptype, "meta": meta}
        prop_by_clean[clean] = {"name": name, "type": ptype, "meta": meta}
    
    # 기존 소재명 목록 로드
    existing_sojae_names = set()
    if sojae_prop_meta:
        target_real = sojae_prop_meta["name"]
        target_type = sojae_prop_meta["type"]
        cursor = None
        
        while True:
            # 1. 쿼리 파라미터 준비
            query_params: Dict[str, Any] = {"database_id": NOTION_DB_ID}
            if cursor:
                query_params["start_cursor"] = cursor
                
            # 2. cast를 사용하여 resp를 dict으로 인식하게 만듦
            resp = cast(Dict[str, Any], notion.databases.query(**query_params))
            
            for page in resp.get("results", []):
                props_data = page.get("properties", {})
                if target_real in props_data:
                    raw = props_data[target_real]
                    val = get_plain_text_from_prop(target_type, raw)
                    if val:
                        existing_sojae_names.add(nospace_lower(val))
            
            if not resp.get("has_more"):
                break
            cursor = resp.get("next_cursor")
    
    schema_info = {
        'prop_by_clean': prop_by_clean,
        'title_prop_name': title_prop_name,
        'project_prop': project_prop,
        'sojae_prop_meta': sojae_prop_meta,
        'existing_sojae_names': list(existing_sojae_names)
    }
    
    context['ti'].xcom_push(key='notion_schema', value=schema_info)
    
    print(f"✅ Notion 스키마 로드 완료 (기존 소재명: {len(existing_sojae_names)}개)")
    return schema_info


def process_sheet(sheet_name, **context):
    """개별 시트 처리 - 링크 정규화 및 Notion 업로드"""
    import gspread
    from google.oauth2.service_account import Credentials
    from google.cloud import storage
    from notion_client import Client as NotionClient
    import time
    import re
    import os
    import unicodedata
    import mimetypes
    from datetime import timedelta
    from urllib.parse import quote
    
    # === 유틸 함수 ===
    def nospace_lower(s: str) -> str:
        if s is None: return ""
        s = unicodedata.normalize('NFKC', str(s))
        s = re.sub(r'\s+', '', s)
        return s.lower()
    
    def normalize(s: str) -> str:
        return unicodedata.normalize('NFKC', str(s)).strip().lower() if s is not None else ""
    
    def col_index_to_letter(idx: int) -> str:
        letters = ''
        while idx:
            idx, rem = divmod(idx - 1, 26)
            letters = chr(65 + rem) + letters
        return letters
    
    def looks_like_url(s: str) -> bool:
        if not s: return False
        s = str(s).strip()
        return bool(re.match(r'^(https?://|gs://|s3://|www\.)', s, re.IGNORECASE))
    
    def first_url(s: str) -> str:
        if not s: return ""
        raw = str(s).strip()
        for part in re.split(r'[,\n; ]+', raw):
            part = part.strip()
            if looks_like_url(part):
                return part
        m = re.search(r'(https?://[^\s",;]+)', raw, re.IGNORECASE)
        return m.group(1).strip() if m else ""
    
    def map_headers(ws, header_row: int = 3):
        headers = ws.row_values(header_row)
        mapping = {}
        for i, name in enumerate(headers, start=1):
            key = nospace_lower(name)
            if key and key not in mapping:
                mapping[key] = (i, name)
        return mapping
    
    def find_sheet_col_idx(mapping: dict, logical_name: str):
        key = nospace_lower(logical_name)
        if key in mapping:
            return mapping[key][0]
        # 동의어
        if key == nospace_lower('파일링크') and nospace_lower('파일 링크') in mapping:
            return mapping[nospace_lower('파일 링크')][0]
        if key == nospace_lower('파일 링크') and nospace_lower('파일링크') in mapping:
            return mapping[nospace_lower('파일링크')][0]
        # 퍼지 매칭
        for k, (idx, _orig) in mapping.items():
            if key and (key in k or k in key):
                return idx
        return None
    
    def last_nonempty_index(values):
        for i in range(len(values)-1, -1, -1):
            if str(values[i]).strip() != "":
                return i + 1
        return 0
    
    def is_shortcut_path(path: str) -> bool:
        try:
            name = os.path.basename(path).lower()
            if name.endswith('.lnk') or '바로가기' in name or 'shortcut' in name:
                return True
            if os.path.islink(path):
                return True
            if os.name == 'nt':
                import ctypes, ctypes.wintypes as wt
                GetFileAttributesW = ctypes.windll.kernel32.GetFileAttributesW
                GetFileAttributesW.argtypes = [wt.LPCWSTR]
                GetFileAttributesW.restype  = wt.DWORD
                FILE_ATTRIBUTE_REPARSE_POINT = 0x0400
                attrs = GetFileAttributesW(path)
                if attrs != 0xFFFFFFFF and (attrs & FILE_ATTRIBUTE_REPARSE_POINT):
                    return True
        except Exception:
            pass
        return False
    
    def extract_hyperlink_url(formula: str) -> str:
        if not formula: return ''
        m = re.match(r'^\s*=\s*HYPERLINK\s*\(\s*"([^"]+)"', formula, re.IGNORECASE)
        return m.group(1) if m else ''
    
    def public_url(bucket_name: str, object_path: str) -> str:
        return f"https://storage.googleapis.com/{bucket_name}/{quote(object_path, safe='/')}"
    
    def build_dest_path(sheet_name: str, filename: str) -> str:
        prefix = GCS_PREFIX_BY_SHEET.get(sheet_name, f"{sheet_name}/")
        if prefix and not prefix.endswith('/'):
            prefix += '/'
        return f"{prefix}{filename}"
    
    def upload_to_gcs_and_get_link(local_filepath: str, dest_path: str, bucket, uploaded_cache: dict) -> str:
        if dest_path in uploaded_cache:
            return uploaded_cache[dest_path]
        
        blob = bucket.blob(dest_path)
        if blob.exists():
            url = public_url(GCS_BUCKET, dest_path)
            uploaded_cache[dest_path] = url
            return url
        
        content_type = mimetypes.guess_type(local_filepath)[0] or 'application/octet-stream'
        try:
            blob.upload_from_filename(local_filepath, content_type=content_type)
        except Exception as e:
            print(f"❗ 업로드 실패: {local_filepath} → {e}")
            return '업로드 실패'
        
        url = public_url(GCS_BUCKET, dest_path)
        uploaded_cache[dest_path] = url
        return url
    
    def find_project_root(sheet_name: str) -> str | None:
        for a in ALIAS_PREFIX.get(sheet_name, []):
            p = os.path.join(NETWORK_BASE_PATH, a)
            if os.path.isdir(p):
                print(f"• [{sheet_name}] 별칭 폴더 사용: {p}")
                return p
        p = os.path.join(NETWORK_BASE_PATH, sheet_name)
        if os.path.isdir(p):
            print(f"• [{sheet_name}] 동일명 폴더 사용: {p}")
            return p
        return None
    
    # Notion 유틸 함수
    def notion_rich(s: str):
        s = (s or '')
        if len(s) > 1900:
            s = s[:1900] + '…'
        return [{"type": "text", "text": {"content": s}}]
    
    def to_bool(s: str):
        if s is None: return None
        t = str(s).strip().lower()
        if t in {"y","yes","true","1","t","o","ok","사용","활용","있음","포함","✔","✓"}:
            return True
        if t in {"n","no","false","0","f","x","미사용","없음","비포함"}:
            return False
        return None
    
    def to_number(s: str):
        if s is None: return None
        t = str(s).strip().replace(",", "")
        if t == "": return None
        try:
            return float(t)
        except:
            return None
    
    def to_date_from_yymmdd(s: str):
        if not s: return None
        t = re.sub(r'\D+', '', str(s))
        if len(t) == 6:
            try:
                from datetime import datetime
                dt = datetime.strptime(t, "%y%m%d")
                return dt.date().isoformat()
            except:
                return None
        try:
            from datetime import datetime
            dt = datetime.fromisoformat(str(s))
            return dt.date().isoformat()
        except:
            return None
    
    def split_multi(s: str):
        if not s: return []
        parts = re.split(r'[,\n/;]+', str(s))
        return [p.strip() for p in parts if p.strip()]
    
    def coerce_payload(prop_type: str, value: str):
        v = "" if value is None else str(value).strip()
        try:
            if prop_type == "title":
                return {"title": notion_rich(v)}
            if prop_type == "rich_text":
                return {"rich_text": notion_rich(v)}
            if prop_type == "url":
                if looks_like_url(v): return {"url": v}
                return {"rich_text": notion_rich(v)}
            if prop_type == "checkbox":
                b = to_bool(v)
                return {"checkbox": b} if b is not None else {"rich_text": notion_rich(v)}
            if prop_type == "number":
                n = to_number(v)
                return {"number": n} if n is not None else {"rich_text": notion_rich(v)}
            if prop_type == "date":
                iso = to_date_from_yymmdd(v)
                return {"date": {"start": iso}} if iso else {"rich_text": notion_rich(v)}
            if prop_type == "select":
                if v == "": return {"select": None}
                return {"select": {"name": v}}
            if prop_type == "multi_select":
                items = split_multi(v)
                return {"multi_select": [{"name": it} for it in items]}
            return {"rich_text": notion_rich(v)}
        except Exception:
            return {"rich_text": notion_rich(v)}
    
    def find_db_prop(prop_by_clean: dict, logical_name: str):
        target = nospace_lower(logical_name)
        if target in prop_by_clean:
            return prop_by_clean[target]
        for clean_key, meta in prop_by_clean.items():
            if target in clean_key or clean_key in target:
                return meta
        return None
    
    # === 설정 ===
    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    
    EXT_WHITELIST = ['.jpg', '.png', '.mp4']
    MAX_ROWS_PER_SHEET = 5000
    CHUNK_SIZE = 500
    DATA_START_ROW = 4
    MAX_FILES_SCAN = 20000
    MAX_SCAN_SECONDS = 120
    LOG_EVERY = 100
    GSPREAD_RETRY = 2
    
    GCS_PREFIX_BY_SHEET = {
        'POTC': 'POTC/', 'GBTW': 'GBTW/', 'WWMC': 'WWMC/', 'DRSG': 'DRSG/',
        'DRB': 'DRB/', 'JTWN': 'JTWN/', 'BSTD': 'BSTD/', 'RESU': 'RESU/',
    }
    
    ALIAS_PREFIX = {
        'DRSG': ['DS'],
        'WWMC': ['WWM'],
        'JTWN': ['JYTW'],
    }
    
    PROJECT_VIEW_URLS = {
        "POTC": "https://www.notion.so/joycity/273ea67a5681806880f2ff1faac3ec71?v=273ea67a568181269e9c000c18a91f2b",
        "GBTW": "https://www.notion.so/joycity/273ea67a5681806880f2ff1faac3ec71?v=273ea67a56818150a4c8000cc5dcd177",
        "WWMC": "https://www.notion.so/joycity/273ea67a5681806880f2ff1faac3ec71?v=273ea67a568181168968000c25c36c48",
        "DRSG": "https://www.notion.so/joycity/273ea67a5681806880f2ff1faac3ec71?v=273ea67a5681817bbfad000c84df1149",
        "DRB": "https://www.notion.so/joycity/273ea67a5681806880f2ff1faac3ec71?v=273ea67a568180f2a430000c03676a67",
        "JTWN": "https://www.notion.so/joycity/273ea67a5681806880f2ff1faac3ec71?v=273ea67a5681809991be000c24f6b232",
        "BSTD": "https://www.notion.so/joycity/273ea67a5681806880f2ff1faac3ec71?v=273ea67a5681809881d0000cab95e6b7",
        "RESU": "https://www.notion.so/joycity/273ea67a5681806880f2ff1faac3ec71?v=273ea67a568181079f38000c1192dc07"
    }
    
    # === 클라이언트 초기화 ===
    
    sheet_creds = Credentials.from_service_account_info(cred_dict, scopes=SCOPES)
    gc = gspread.authorize(sheet_creds)
    spreadsheet = gc.open_by_key(SPREADSHEET_ID)

    storage_client = storage.Client.from_service_account_info(cred_dict)
    bucket = storage_client.bucket(GCS_BUCKET)
    
    notion = NotionClient(auth=NOTION_API_KEY)
    
    # Notion 스키마 가져오기
    schema_info = context['ti'].xcom_pull(key='notion_schema', task_ids='fetch_notion_schema')
    prop_by_clean = schema_info['prop_by_clean']
    title_prop_name = schema_info['title_prop_name']
    project_prop = schema_info['project_prop']
    existing_sojae_names = set(schema_info['existing_sojae_names'])
    
    print(f"\n{'='*60}")
    print(f"[{sheet_name}] 처리 시작")
    print(f"{'='*60}")
    
    try:
        ws = spreadsheet.worksheet(sheet_name)
    except Exception as e:
        print(f"❗ 시트 '{sheet_name}' 없음 - 건너뜀: {e}")
        return {'status': 'skipped', 'reason': 'sheet_not_found'}
    
    # 헤더 매핑
    headers_map = map_headers(ws, header_row=3)
    
    # 필수 열 확인
    link_idx = find_sheet_col_idx(headers_map, '파일 링크')
    if link_idx is None:
        print("❗ '파일 링크/파일링크' 열을 찾지 못했습니다.")
        return {'status': 'skipped', 'reason': 'no_link_column'}
    
    link_col_letter = col_index_to_letter(link_idx)
    yt_idx = find_sheet_col_idx(headers_map, 'Youtube URL')
    so_idx = find_sheet_col_idx(headers_map, '소재명')
    past_idx = find_sheet_col_idx(headers_map, '과거소재명')
    cat_idx = find_sheet_col_idx(headers_map, '대분류')
    
    # 링크 열 포맷 설정
    try:
        spreadsheet.batch_update({
            "requests": [{
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": ws.id,
                        "dimension": "COLUMNS",
                        "startIndex": link_idx - 1,
                        "endIndex": link_idx
                    },
                    "properties": {"pixelSize": 70},
                    "fields": "pixelSize"
                }
            }, {
                "repeatCell": {
                    "range": {
                        "sheetId": ws.id,
                        "startRowIndex": DATA_START_ROW - 1,
                        "endRowIndex": ws.row_count,
                        "startColumnIndex": link_idx - 1,
                        "endColumnIndex": link_idx
                    },
                    "cell": {"userEnteredFormat": {"wrapStrategy": "CLIP"}},
                    "fields": "userEnteredFormat.wrapStrategy"
                }
            }]
        })
        print(f"✓ '{link_col_letter}'열 너비 70px + CLIP 적용")
    except Exception as e:
        print(f"⚠️ 열 포맷 적용 실패: {e}")
    
    # 데이터 로드
    link_display_vals = ws.col_values(link_idx)[3:]
    youtube_vals = ws.col_values(yt_idx)[3:] if yt_idx else []
    so_vals_disp = ws.col_values(so_idx)[3:] if so_idx else []
    past_vals_disp = ws.col_values(past_idx)[3:] if past_idx else []
    
    # 네트워크 파일 스캔
    all_files = []
    uploaded_file_cache = {}
    project_root = find_project_root(sheet_name)
    
    if project_root:
        t0 = time.perf_counter()
        scan_files = 0
        try:
            candidate_dirs = []
            for name in os.listdir(project_root):
                path = os.path.join(project_root, name)
                if not os.path.isdir(path): continue
                if is_shortcut_path(path): continue
                if re.search(r'backup|백업|old|temp', name, re.I): continue
                if '소재' in name:
                    candidate_dirs.append(path)
            
            for base_dir in candidate_dirs:
                print(f"  · [{sheet_name}] 스캔 시작: {base_dir}")
                for root, dirs, files in os.walk(base_dir):
                    dirs[:] = [d for d in dirs
                               if not is_shortcut_path(os.path.join(root, d))
                               and not re.search(r'backup|백업|old|temp', d, re.I)]
                    for fname in files:
                        if is_shortcut_path(os.path.join(root, fname)):
                            continue
                        scan_files += 1
                        if scan_files % 2000 == 0:
                            print(f"    .. 스캔 진행: {scan_files}개 확인, 유효 {len(all_files)}개")
                        if time.perf_counter() - t0 > MAX_SCAN_SECONDS:
                            print(f"  ⚠️ [{sheet_name}] 인덱싱 시간 초과({time.perf_counter()-t0:.1f}s) 중단")
                            break
                        if len(all_files) >= MAX_FILES_SCAN:
                            print(f"  ⚠️ [{sheet_name}] 인덱싱 상한 {MAX_FILES_SCAN} 도달")
                            break
                        
                        ext = os.path.splitext(fname)[1].lower()
                        if ext in EXT_WHITELIST:
                            all_files.append({
                                'name': os.path.splitext(fname)[0],
                                'original': fname,
                                'path': os.path.join(root, fname)
                            })
                    else:
                        continue
                    break
        except Exception as e:
            print(f"ℹ️ [{sheet_name}] 루트 스캔 중 예외: {e}")
        print(f"• [{sheet_name}] 인덱싱 결과: 유효 {len(all_files)}개, 파일확인 {scan_files}개, {time.perf_counter()-t0:.1f}s")
    else:
        print(f"ℹ️ 프로젝트 폴더 없음: {sheet_name} — 파일 매칭 생략")
    
    # 링크 업데이트 범위 계산
    last_link = last_nonempty_index(link_display_vals)
    last_yt = last_nonempty_index(youtube_vals)
    last_so = last_nonempty_index(so_vals_disp)
    last_past = last_nonempty_index(past_vals_disp)
    real_last = max(last_link, last_yt, last_so, last_past)
    
    if real_last == 0:
        print("ℹ️ 처리할 데이터가 없습니다.")
        return {'status': 'completed', 'sheet_name': sheet_name, 'updated_links': 0, 'notion_rows': 0}
    
    end_row = min(3 + real_last, 3 + MAX_ROWS_PER_SHEET)
    start_row = DATA_START_ROW
    total_rows = max(0, end_row - start_row + 1)
    print(f"• [{sheet_name}] 링크 업데이트 대상: {total_rows}행 (4~{end_row})")
    
    def get_safe(lst, idx): return lst[idx] if idx < len(lst) else ""
    
    rows_done = 0
    new_links_count = 0
    t_sheet = time.perf_counter()
    
    # 링크 업데이트 배치 처리
    while rows_done < total_rows:
        t_batch = time.perf_counter()
        batch_start = start_row + rows_done
        batch_end = min(batch_start + CHUNK_SIZE - 1, end_row)
        batch_len = batch_end - batch_start + 1
        link_range = f'{link_col_letter}{batch_start}:{link_col_letter}{batch_end}'
        print(f"  ▶ [{sheet_name}] 배치 시작 {batch_start}~{batch_end} (len={batch_len})")
        
        batch_values = []
        for i in range(batch_len):
            gi = rows_done + i
            disp = get_safe(link_display_vals, gi)
            yt_url = first_url(str(get_safe(youtube_vals, gi)))
            
            so_name = get_safe(so_vals_disp, gi)
            past_name = get_safe(past_vals_disp, gi)
            
            tokens = [normalize(t) for t in (so_name, past_name) if t and normalize(t)]
            tokens = [t for t in tokens if len(t) > 2]
            
            chosen_url = None
            
            if yt_url:
                chosen_url = yt_url
            elif tokens and all_files:
                selected = None
                for f in all_files:
                    if any(tok in normalize(f['name']) for tok in tokens):
                        selected = f
                        break
                if selected:
                    filename = os.path.basename(selected['path'])
                    dest_path = build_dest_path(sheet_name, filename)
                    try:
                        gcs_link = upload_to_gcs_and_get_link(selected['path'], dest_path, bucket, uploaded_file_cache)
                    except Exception as e:
                        print(f"    ❗ 업로드 예외({selected['path']}) → {e}")
                        gcs_link = '업로드 실패'
                    if gcs_link and gcs_link != '업로드 실패':
                        chosen_url = gcs_link
                        new_links_count += 1
            
            if chosen_url:
                batch_values.append(chosen_url)
            else:
                if looks_like_url(disp):
                    batch_values.append(str(disp).strip())
                else:
                    batch_values.append(disp)
            
            if (i % LOG_EVERY == 0) and i > 0:
                print(f"    · 진행 {i}/{batch_len} (전체 {rows_done+i}/{total_rows})")
        
        # 배치 커밋
        commit_ok = False
        for attempt in range(1, GSPREAD_RETRY+2):
            try:
                cells = ws.range(link_range)
                for i, cell in enumerate(cells):
                    cell.value = batch_values[i]
                ws.update_cells(cells, value_input_option=cast(Any, 'USER_ENTERED'))
                ws.format(link_range, {
                    "horizontalAlignment": "LEFT",
                    "wrapStrategy": "CLIP"
                })
                print(f"  ✓ [{sheet_name}] 커밋 완료 {batch_start}~{batch_end}")
                commit_ok = True
                break
            except Exception as e:
                print(f"  ⚠️ 커밋 실패({attempt}) {batch_start}~{batch_end} → {e}")
                time.sleep(1.2)
        
        if not commit_ok:
            print(f"  ❗ [{sheet_name}] 커밋 최종 실패: {link_range}")
        
        rows_done += batch_len
    
    print(f"✅ [{sheet_name}] 링크 업데이트 완료: {rows_done}행 (신규 {new_links_count}개)")
    
    # === Notion 업로드 ===
    if not so_idx:
        print("⚠️ '소재명' 컬럼을 찾지 못해 Notion 업로드 생략")
        return {'status': 'completed', 'sheet_name': sheet_name, 'updated_links': new_links_count, 'notion_rows': 0}
    
    headers_row = ws.row_values(3)
    last_col_letter = col_index_to_letter(len(headers_row))
    rng_disp = f"A3:{last_col_letter}{end_row}"
    grid_disp = ws.get(rng_disp, value_render_option=cast(Any, 'UNFORMATTED_VALUE')) or []
    grid_form = ws.get(rng_disp, value_render_option=cast(Any, 'FORMULA')) or []
    
    if not grid_disp:
        return {'status': 'completed', 'sheet_name': sheet_name, 'updated_links': new_links_count, 'notion_rows': 0}
    
    header = grid_disp[0]
    data_rows_disp = grid_disp[1:]
    data_rows_form = grid_form[1:] if grid_form else []
    
    print(f"  ▶ [{sheet_name}] Notion 업로드 시작 — 데이터 행 수 {len(data_rows_disp)}")
    
    notion_rows = 0
    for r_i, row in enumerate(data_rows_disp):
        if (r_i % LOG_EVERY == 0) and r_i > 0:
            print(f"    · Notion 진행 {r_i}/{len(data_rows_disp)}")
        
        # 소재명 비면 스킵
        if (len(row) < so_idx) or (str(row[so_idx-1]).strip() == ""):
            continue
        
        # 대분류 비면 스킵
        if cat_idx:
            if (len(row) < cat_idx) or (str(row[cat_idx-1]).strip() == ""):
                continue
        
        # 행 dict 구성
        row_dict = {}
        for cidx, h in enumerate(header, start=1):
            val = row[cidx-1] if len(row) >= cidx else ""
            if cidx == link_idx:
                ff = data_rows_form[r_i][link_idx-1] if (r_i < len(data_rows_form) and len(data_rows_form[r_i]) >= link_idx) else ""
                url_only = extract_hyperlink_url(ff) or (val if looks_like_url(val) else "")
                row_dict[h] = str(url_only)
            else:
                row_dict[h] = str(val if val is not None else "")
        
        # 중복 체크
        current_sojae_name = ""
        for k, v in row_dict.items():
            if nospace_lower(k) == nospace_lower("소재명"):
                current_sojae_name = str(v).strip()
                break
        if current_sojae_name and nospace_lower(current_sojae_name) in existing_sojae_names:
            continue
        
        # Title 값: 과거소재명 우선
        title_value = ""
        for k, v in row_dict.items():
            if nospace_lower(k) == nospace_lower('과거소재명') and str(v).strip():
                title_value = str(v).strip()
                break
        
        properties: Dict[str, Any] = {title_prop_name: {"title": notion_rich(title_value)}}
        
        # 프로젝트 속성
        if project_prop:
            pname = project_prop["name"]
            ptype = project_prop["type"]
            if ptype == "select":
                properties[pname] = {"select": {"name": sheet_name}}
            elif ptype == "multi_select":
                properties[pname] = {"multi_select": [{"name": sheet_name}]}
            elif ptype == "rich_text":
                properties[pname] = {"rich_text": notion_rich(sheet_name)}
            else:
                try: properties[pname] = coerce_payload(ptype, sheet_name)
                except: properties[pname] = {"rich_text": notion_rich(sheet_name)}
        
        # 나머지 열
        for sh_col_name, val in row_dict.items():
            clean = nospace_lower(sh_col_name)
            if clean in {nospace_lower(title_prop_name), nospace_lower(project_prop["name"]) if project_prop else ""}:
                continue
            meta = prop_by_clean.get(clean) or find_db_prop(prop_by_clean, clean)
            if meta:
                real_name = meta["name"]
                ptype = meta["type"]
                properties[real_name] = coerce_payload(ptype, val)
        
        try:
            notion.pages.create(parent={"database_id": NOTION_DB_ID}, properties=properties)
            if current_sojae_name:
                existing_sojae_names.add(nospace_lower(current_sojae_name))
            notion_rows += 1
        except Exception as e:
            print(f"    ❗ Notion 실패 r{r_i+DATA_START_ROW} → {e}")
        
        time.sleep(0.35)
    
    if sheet_name in PROJECT_VIEW_URLS:
        print(f"🔗 {sheet_name} 뷰: {PROJECT_VIEW_URLS[sheet_name]}")
    
    print(f"✅ [{sheet_name}] 완료 - 링크 {new_links_count}개, Notion {notion_rows}행")
    
    return {
        'status': 'completed',
        'sheet_name': sheet_name,
        'updated_links': new_links_count,
        'notion_rows': notion_rows
    }

def generate_summary_report(**context):
    """전체 처리 결과 요약 리포트 생성"""
    results = []
    
    for sheet_name in TARGET_SHEETS:
        task_id = f'process_{sheet_name.lower()}'
        result = context['ti'].xcom_pull(task_ids=task_id)
        if result:
            results.append(result)
    
    total_links = sum(r.get('updated_links', 0) for r in results if r.get('status') == 'completed')
    total_notion = sum(r.get('notion_rows', 0) for r in results if r.get('status') == 'completed')
    
    summary = f"""
{'='*60}
처리 완료 요약
{'='*60}
실행 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
총 시트 수: {len(TARGET_SHEETS)}
처리 완료: {len([r for r in results if r.get('status') == 'completed'])}개
링크 갱신: {total_links}개
Notion 업로드: {total_notion}행

시트별 상세:
"""
    
    for result in results:
        if result:
            summary += f"\n  • {result.get('sheet_name', 'Unknown')}: "
            summary += f"링크 {result.get('updated_links', 0)}개, "
            summary += f"Notion {result.get('notion_rows', 0)}행"
    
    print(summary)
    
    summary_data = {
        'total_sheets': len(TARGET_SHEETS),
        'completed': len([r for r in results if r.get('status') == 'completed']),
        'total_links_updated': total_links,
        'total_notion_rows': total_notion,
        'timestamp': datetime.now().isoformat(),
        'results': results,
        'summary_text': summary
    }
    
    # XCom에 요약 데이터 저장 (이메일 발송용)
    context['ti'].xcom_push(key='summary_report', value=summary_data)
    
    return summary_data


def send_summary_email(**context):
    """요약 리포트를 이메일로 발송"""
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    from email.utils import formatdate
    
    # Airflow Variables에서 SMTP 설정 가져오기
    try:
        smtp_host = Variable.get('SMTP_HOST', default_var='smtp.gmail.com')
        smtp_port = int(Variable.get('SMTP_PORT', default_var='587'))
        smtp_user = Variable.get('EMAIL_FROM', default_var='ds_bi@joycity.com')
        smtp_password = Variable.get('SMTP_PASSWORD')
        email_from = Variable.get('EMAIL_FROM', default_var=smtp_user)
        email_to = Variable.get('EMAIL_TO', default_var='65e43b85.joycity.com@kr.teams.ms')

    except Exception as e:
        print(f"⚠️  SMTP 설정 로드 실패: {e}")
        print("이메일 발송을 건너뜁니다.")
        return {'status': 'skipped', 'reason': 'smtp_config_missing'}
    
    # 요약 리포트 가져오기
    summary_data = context['ti'].xcom_pull(key='summary_report', task_ids='generate_summary_report')
    
    if not summary_data:
        print("❗ 요약 리포트를 찾을 수 없습니다.")
        return {'status': 'failed', 'reason': 'no_summary_data'}
    
    # 실행 날짜
    execution_date = context.get('execution_date', datetime.now())
    date_str = execution_date.strftime('%Y-%m-%d')
    
    # 이메일 제목
    total_links = summary_data.get('total_links_updated', 0)
    total_notion = summary_data.get('total_notion_rows', 0)
    completed = summary_data.get('completed', 0)
    total_sheets = summary_data.get('total_sheets', 0)
    
    subject = f"[Creative DB Sync] 일일 배치 완료 ({date_str}) - 링크 {total_links}개, Notion {total_notion}행"
    
    # 이메일 본문 (HTML)
    results = summary_data.get('results', [])
    
    # 시트별 상세 테이블 생성
    sheet_details_html = ""
    for result in results:
        if result:
            status = result.get('status', 'unknown')
            status_emoji = '✅' if status == 'completed' else '⚠️'
            sheet_name = result.get('sheet_name', 'Unknown')
            links = result.get('updated_links', 0)
            notion = result.get('notion_rows', 0)
            
            sheet_details_html += f"""
            <tr>
                <td style="padding: 8px; border: 1px solid #ddd;">{status_emoji} {sheet_name}</td>
                <td style="padding: 8px; border: 1px solid #ddd; text-align: center;">{links}</td>
                <td style="padding: 8px; border: 1px solid #ddd; text-align: center;">{notion}</td>
                <td style="padding: 8px; border: 1px solid #ddd; text-align: center;">{status}</td>
            </tr>
            """
    
    html_body = f"""
    <html>
    <head>
        <style>
            body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }}
            .container {{ max-width: 800px; margin: 0 auto; padding: 20px; }}
            .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                      color: white; padding: 30px; border-radius: 10px 10px 0 0; }}
            .header h1 {{ margin: 0; font-size: 24px; }}
            .header p {{ margin: 5px 0 0 0; opacity: 0.9; }}
            .content {{ background: #ffffff; padding: 30px; border: 1px solid #e0e0e0; }}
            .summary-box {{ background: #f8f9fa; padding: 20px; border-radius: 8px; 
                           margin: 20px 0; border-left: 4px solid #667eea; }}
            .metric {{ display: inline-block; margin: 10px 20px 10px 0; }}
            .metric-value {{ font-size: 32px; font-weight: bold; color: #667eea; }}
            .metric-label {{ font-size: 14px; color: #666; }}
            table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
            th {{ background: #667eea; color: white; padding: 12px; text-align: left; }}
            td {{ padding: 8px; border: 1px solid #ddd; }}
            tr:nth-child(even) {{ background: #f8f9fa; }}
            .footer {{ background: #f8f9fa; padding: 20px; text-align: center; 
                      color: #666; font-size: 12px; border-radius: 0 0 10px 10px; }}
            .success {{ color: #28a745; font-weight: bold; }}
            .warning {{ color: #ffc107; font-weight: bold; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🎨 Creative DB Sync 일일 배치 리포트</h1>
                <p>실행 일시: {summary_data.get('timestamp', 'N/A')}</p>
            </div>
            
            <div class="content">
                <div class="summary-box">
                    <h2 style="margin-top: 0;">📊 처리 요약</h2>
                    <div class="metric">
                        <div class="metric-value">{completed}/{total_sheets}</div>
                        <div class="metric-label">시트 처리 완료</div>
                    </div>
                    <div class="metric">
                        <div class="metric-value">{total_links}</div>
                        <div class="metric-label">링크 갱신</div>
                    </div>
                    <div class="metric">
                        <div class="metric-value">{total_notion}</div>
                        <div class="metric-label">Notion 업로드</div>
                    </div>
                </div>
                
                <h2>📋 시트별 처리 결과</h2>
                <table>
                    <thead>
                        <tr>
                            <th>프로젝트</th>
                            <th style="text-align: center;">링크 갱신</th>
                            <th style="text-align: center;">Notion 업로드</th>
                            <th style="text-align: center;">상태</th>
                        </tr>
                    </thead>
                    <tbody>
                        {sheet_details_html}
                    </tbody>
                </table>
            </div>
            
            <div class="footer">
                <p>이 메일은 Airflow DAG에서 자동으로 발송되었습니다.</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    # 플레인 텍스트 본문 (HTML 미지원 클라이언트용)
    plain_body = summary_data.get('summary_text', '요약 정보를 가져올 수 없습니다.')
    
    # 이메일 메시지 생성
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = email_from
    msg['To'] = ', '.join(email_to)
    msg['Date'] = formatdate(localtime=True)
    
    # 본문 추가
    part1 = MIMEText(plain_body, 'plain', 'utf-8')
    part2 = MIMEText(html_body, 'html', 'utf-8')
    msg.attach(part1)
    msg.attach(part2)
    
    # SMTP 발송
    try:
        print(f"📧 이메일 발송 중... (받는 사람: {', '.join(email_to)})")
        
        server = smtplib.SMTP(smtp_host, smtp_port)
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(smtp_user, smtp_password)
        server.sendmail(email_from, email_to, msg.as_string())
        server.quit()
        
        print(f"✅ 이메일 발송 완료!")
        return {
            'status': 'success',
            'recipients': email_to,
            'subject': subject,
            'timestamp': datetime.now().isoformat()
        }
        
    except smtplib.SMTPAuthenticationError:
        print("❗ SMTP 인증 실패: 사용자명 또는 비밀번호를 확인하세요.")
        return {'status': 'failed', 'reason': 'authentication_failed'}
    except smtplib.SMTPException as e:
        print(f"❗ SMTP 오류: {e}")
        return {'status': 'failed', 'reason': str(e)}
    except Exception as e:
        print(f"❗ 이메일 발송 실패: {e}")
        return {'status': 'failed', 'reason': str(e)}


# =====================
# DAG 태스크 정의
# =====================

# 1. 환경 검증
task_check_deps = PythonOperator(
    task_id='check_dependencies',
    python_callable=check_dependencies,
    dag=dag,
)

# 2. 클라이언트 초기화
task_init_clients = PythonOperator(
    task_id='initialize_clients',
    python_callable=initialize_clients,
    dag=dag,
)

# 3. Notion 스키마 조회
task_fetch_schema = PythonOperator(
    task_id='fetch_notion_schema',
    python_callable=fetch_notion_schema,
    dag=dag,
)

# 4. 각 시트별 병렬 처리
sheet_tasks = []
for sheet in TARGET_SHEETS:
    task = PythonOperator(
        task_id=f'process_{sheet.lower()}',
        python_callable=process_sheet,
        op_kwargs={'sheet_name': sheet},
        # # Pool을 사용한 동시 실행 제한
        # pool='sheets_api_pool',  # 미리 생성 필요
        # pool_slots=2,
        
        # # 재시도 설정
        # retries=5,
        # retry_delay=timedelta(seconds=30),
        # retry_exponential_backoff=True,
        # max_retry_delay=timedelta(minutes=15),
        dag=dag,
    )
    sheet_tasks.append(task)

# 5. 최종 요약 리포트
task_summary = PythonOperator(
    task_id='generate_summary_report',
    python_callable=generate_summary_report,
    dag=dag,
)

# 6. 이메일 발송
task_send_email = PythonOperator(
    task_id='send_summary_email',
    python_callable=send_summary_email,
    dag=dag,
)

bash_task = BashOperator(
    task_id = 'bash_task',
    outlets = [gcs_creative_file_upload],
    bash_command = 'echo "gcs_creative_file_upload 완료!"',
    dag=dag,
)

# =====================
# DAG 의존성 설정
# =====================
task_check_deps >> task_init_clients >> task_fetch_schema >> sheet_tasks >> task_summary >> task_send_email >> bash_task