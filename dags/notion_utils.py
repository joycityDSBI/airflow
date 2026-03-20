"""
수정된 notion_utils.py
- Notion 설정 검증 강화
- 에러 처리 개선 (raise 추가)
- 로깅 강화
"""

import os
import time
import json
import random
import logging
from datetime import datetime
from typing import Dict, Set, Tuple, List
import requests
import pandas as pd
from airflow.models import Variable

logging.basicConfig(level=logging.INFO)


def get_var(key: str, default: str | None = None) -> str:
    """환경 변수 또는 Airflow Variable 조회"""
    return os.environ.get(key) or Variable.get(key, default_var=default)


# ===== Notion ENV (이 4개만 환경변수로 사용) =====
NOTION_TOKEN = get_var("NOTION_TOKEN")
DB_A_ID = get_var("NOTION_DB_A")  # 테이블 정보용 (키: Hub 테이블명)
DB_B_ID = get_var("NOTION_DB_B")  # 컬럼 정보용 (키: Hub 컬럼명)
DB_C_ID = get_var("NOTION_DB_C")  # 테이블-컬럼 매핑용 (relation: A/B)

# 📌 설정 검증 함수
def validate_notion_config() -> bool:
    """Notion 설정 검증"""
    if not NOTION_TOKEN:
        logging.error("❌ NOTION_TOKEN이 없습니다")
        return False
    
    if not DB_A_ID:
        logging.error("❌ NOTION_DB_A가 없습니다")
        return False
    
    if not DB_B_ID:
        logging.error("❌ NOTION_DB_B가 없습니다")
        return False
    
    if not DB_C_ID:
        logging.error("❌ NOTION_DB_C가 없습니다")
        return False
    
    logging.info("✅ Notion 설정 검증 완료")
    logging.info(f"   Token: {NOTION_TOKEN[:20]}...")
    logging.info(f"   DB_A: {DB_A_ID[:20]}...")
    logging.info(f"   DB_B: {DB_B_ID[:20]}...")
    logging.info(f"   DB_C: {DB_C_ID[:20]}...")
    
    return True


# 초기화 시 검증
if not (NOTION_TOKEN and DB_A_ID and DB_B_ID and DB_C_ID):
    logging.error("⚠️ Notion 설정 누락! update_notion_databases 호출 시 실패합니다.")

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}" if NOTION_TOKEN else "Bearer INVALID",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

# ===== 고정 파라미터(하드코딩) =====
PAGE_SIZE        = 100      # Notion API 최대 100
MAX_PAGES_SCAN   = 200      # 100 * 200 = 최대 20,000건 스캔
MAX_RETRY        = 5        # 429/5xx 재시도 횟수
RATE_LIMIT_SLEEP = 0.35     # 3 rps 가드
REQ_TIMEOUT      = 30       # 요청 타임아웃(초)


# ===== 공통 HTTP 요청 (재시도/백오프) =====
def request_with_retry(method: str, url: str, **kwargs) -> requests.Response:
    """HTTP 요청 (재시도 로직 포함)"""
    if "headers" not in kwargs:
        kwargs["headers"] = HEADERS
    if "timeout" not in kwargs:
        kwargs["timeout"] = REQ_TIMEOUT

    last = None
    for attempt in range(1, MAX_RETRY + 1):
        try:
            res = requests.request(method, url, **kwargs)
        except requests.RequestException as e:
            wait = min(2 ** attempt, 16) + random.random()
            logging.warning(f"⏳ 요청 예외 재시도 {attempt}/{MAX_RETRY}: {e} — {wait:.1f}s 대기")
            time.sleep(wait)
            continue

        # 📌 상태 코드별 처리
        if res.status_code in (429, 500, 502, 503, 504):
            wait = min(2 ** attempt, 16) + random.random()
            logging.warning(f"⏳ {res.status_code} 재시도 {attempt}/{MAX_RETRY}: {res.text[:200]} — {wait:.1f}s 대기")
            time.sleep(wait)
            last = res
            continue
        
        # 📌 4xx 에러 (401/403/404 등) - 명시적 로깅
        if 400 <= res.status_code < 500:
            logging.error(f"❌ API 요청 실패: {res.status_code} {method} {url[:60]}...")
            logging.error(f"   응답: {res.text[:500]}")
        
        return res
    
    # 📌 모든 재시도 실패
    if last is None:
        raise requests.RequestException(f"모든 재시도 실패 ({MAX_RETRY}회) 및 응답 없음")
    
    return last # 이제 last는 무조건 Response 객체임이 보장됨


# ===== 텍스트 추출 유틸 =====
def extract_text(prop: dict) -> str | None:
    """Notion 속성에서 텍스트 추출"""
    arr = prop.get("title") or prop.get("rich_text")
    if not arr:
        return None
    parts = []
    for item in arr:
        t = item.get("text", {})
        if "content" in t:
            parts.append(t["content"])
    txt = "".join(parts).strip()
    return txt or None


# ===== DB 페이징 =====
def notion_paginate(database_id: str, page_size: int = PAGE_SIZE, max_pages: int = MAX_PAGES_SCAN):
    """Notion DB 페이징 조회"""
    next_cursor = None
    pages = 0
    while True:
        body = {"page_size": page_size}
        if next_cursor:
            body["start_cursor"] = next_cursor

        res = request_with_retry("POST", f"https://api.notion.com/v1/databases/{database_id}/query", json=body)
        if not res or res.status_code != 200:
            logging.error(f"❌ DB 조회 실패: {res.status_code if res else 'ERR'} - {res.text[:200] if res else ''}")
            break

        data = res.json()
        for page in data.get("results", []):
            yield page

        if not data.get("has_more"):
            break
        next_cursor = data.get("next_cursor")
        pages += 1
        if pages >= max_pages:
            logging.warning(f"⚠️ 페이지 상한 도달(max_pages={max_pages})")
            break
        time.sleep(RATE_LIMIT_SLEEP)


# ===== A/B ID 맵 조회 =====
def notion_query_id_map(database_id: str, target_column: str, *, silent: bool = False, label: str = "") -> dict:
    """Notion DB에서 ID 맵 조회 (키: 컬럼값, 값: page_id)"""
    id_map: dict[str, str] = {}
    for page in notion_paginate(database_id):
        pid = page["id"]
        prop = page["properties"].get(target_column, {})
        key = extract_text(prop)
        if key:
            id_map[key] = pid
    if not silent:
        suffix = f" {label}" if label else ""
        logging.info(f"🔎 ID 맵 로드{suffix}: {target_column} {len(id_map)}건")
    return id_map


# ===== 단일 Insert =====
def notion_insert(database_id: str, field_name: str, value: str, is_title: bool = True) -> bool:
    """Notion DB에 항목 추가"""
    field_type = "title" if is_title else "rich_text"
    payload = {
        "parent": {"database_id": database_id},
        "properties": {
            field_name: {field_type: [{"text": {"content": value}}]}
        }
    }
    res = request_with_retry("POST", "https://api.notion.com/v1/pages", json=payload)
    if res and res.status_code == 200:
        logging.info(f"✅ Insert 성공: {value}")
        return True
    logging.error(f"❌ Insert 실패: {res.status_code if res else 'ERR'} - {res.text[:200] if res else ''}")
    return False


# ===== 관계(Relation) 헬퍼 =====
def get_relation_ids(page_props: dict, prop_name: str) -> List[str]:
    """Relation 속성에서 page ID 추출"""
    rel = page_props.get(prop_name, {})
    return [r.get("id") for r in rel.get("relation", []) if "id" in r]


def archive_page(page_id: str) -> bool:
    """페이지 아카이브"""
    res = request_with_retry("PATCH", f"https://api.notion.com/v1/pages/{page_id}", json={"archived": True})
    code = res.status_code if res else "ERR"
    logging.info(f"🗄️ archive page {page_id} -> {code}")
    return bool(res and res.status_code == 200)


def _parse_iso8601_to_ts(s: str) -> float:
    """ISO8601 → epoch seconds"""
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s).timestamp()
    except Exception:
        return 0.0


def cleanup_db_c_orphans(a_id_map: Dict[str, str], b_id_map: Dict[str, str], *, dry_run: bool = False) -> dict:
    """
    C 테이블 사전 정리:
    1) relation 비었으면 → archive
    2) relation id가 A/B의 유효 id 집합에 없으면 → archive
    3) (a_id, b_id) 중복이 여러 건이면 가장 오래된 1건만 남기고 나머지 → archive
    """
    valid_a: Set[str] = set(a_id_map.values())
    valid_b: Set[str] = set(b_id_map.values())

    orphan: List[str] = []      # relation 비어있는 C 페이지
    invalid: List[str] = []     # A/B에 존재하지 않는 id를 참조하는 C 페이지
    dedup: List[str] = []       # (a,b) 중복 제거 대상

    best_for_pair: Dict[Tuple[str, str], Tuple[str, float]] = {}

    # 전체 C 스캔
    for page in notion_paginate(DB_C_ID):
        pid = page["id"]
        props = page["properties"]
        a_list = get_relation_ids(props, "Hub 테이블명")
        b_list = get_relation_ids(props, "Hub 컬럼명")

        # 1) orphan
        if not a_list or not b_list:
            orphan.append(pid)
            continue

        a_pid, b_pid = a_list[0], b_list[0]

        # 2) invalid ref
        if a_pid not in valid_a or b_pid not in valid_b:
            invalid.append(pid)
            continue

        # 3) dedup
        pair = (a_pid, b_pid)
        created_ts = _parse_iso8601_to_ts(page.get("created_time", "1970-01-01T00:00:00Z"))
        if pair not in best_for_pair:
            best_for_pair[pair] = (pid, created_ts)
        else:
            keep_pid, keep_ts = best_for_pair[pair]
            if created_ts < keep_ts:
                dedup.append(keep_pid)
                best_for_pair[pair] = (pid, created_ts)
            else:
                dedup.append(pid)

    # 최종 아카이브 대상
    to_archive = set(orphan) | set(invalid) | set(dedup)
    logging.info(
        f"🧹 C pre-clean: orphan={len(orphan)}, invalid_ref={len(invalid)}, "
        f"dedup={len(dedup)}, unique_archive={len(to_archive)}"
    )

    if dry_run:
        logging.info("🟡 DRY-RUN: archive 실행 안 함")
        return {
            "orphan": len(orphan),
            "invalid_ref": len(invalid),
            "dedup": len(dedup),
            "archived": 0
        }

    # 실제 아카이브
    archived = 0
    for pid in to_archive:
        if archive_page(pid):
            archived += 1
        time.sleep(0.2)

    logging.info(f"✅ C pre-clean archived: {archived}")
    return {
        "orphan": len(orphan),
        "invalid_ref": len(invalid),
        "dedup": len(dedup),
        "archived": archived
    }


# ===== C 테이블 UPSERT(diff) =====
def sync_db_c(df: pd.DataFrame, a_id_map: Dict[str, str], b_id_map: Dict[str, str]):
    """C 테이블 UPSERT"""
    # 역매핑
    a_pid_to_name = {pid: name for name, pid in a_id_map.items()}
    b_pid_to_name = {pid: name for name, pid in b_id_map.items()}

    # 타깃 쌍 생성
    unique_pairs = df.drop_duplicates(subset=["full_table_id", "column_name"])
    desired: Set[Tuple[str, str]] = set()
    for row in unique_pairs.itertuples():
        tid = str(row.full_table_id)
        cname = str(row.column_name)
        if tid in a_id_map and cname in b_id_map:
            desired.add((a_id_map[tid], b_id_map[cname]))

    # 현재 존재하는 쌍 수집
    existing: Set[Tuple[str, str]] = set()
    page_by_pair: Dict[Tuple[str, str], str] = {}
    for page in notion_paginate(DB_C_ID):
        pid = page["id"]
        props = page["properties"]
        a_list = get_relation_ids(props, "Hub 테이블명")
        b_list = get_relation_ids(props, "Hub 컬럼명")
        if a_list and b_list:
            pair = (a_list[0], b_list[0])
            existing.add(pair)
            page_by_pair[pair] = pid

    to_add = desired - existing
    to_remove = existing - desired
    logging.info(f"📊 C diff — add:{len(to_add)} / remove:{len(to_remove)} / keep:{len(existing & desired)}")

    # 추가
    idx_base = len(existing & desired) + 1
    for i, (a_pid, b_pid) in enumerate(to_add, start=idx_base):
        tname = a_pid_to_name.get(a_pid, "UNKNOWN_TABLE")
        cname = b_pid_to_name.get(b_pid, "UNKNOWN_COLUMN")

        payload = {
            "parent": {"database_id": DB_C_ID},
            "properties": {
                "idx": {"title": [{"text": {"content": str(i)}}]},
                "Hub 테이블명": {"relation": [{"id": a_pid}]},
                "Hub 컬럼명": {"relation": [{"id": b_pid}]}
            }
        }
        res = request_with_retry("POST", "https://api.notion.com/v1/pages", json=payload)
        code = res.status_code if res else "ERR"
        logging.info(f"➕ C add [{tname}] × [{cname}] -> {code}")
        time.sleep(0.25)

    # 제거(archive)
    for pair in to_remove:
        pid = page_by_pair.get(pair)
        res = request_with_retry("PATCH", f"https://api.notion.com/v1/pages/{pid}", json={"archived": True})
        code = res.status_code if res else "ERR"
        tname = a_pid_to_name.get(pair[0], "UNKNOWN_TABLE")
        cname = b_pid_to_name.get(pair[1], "UNKNOWN_COLUMN")
        logging.info(f"➖ C remove [{tname}] × [{cname}] -> {code}")
        time.sleep(0.2)


# ===== 전체 동기화 엔트리 =====
def update_notion_databases(df: pd.DataFrame) -> bool:
    """
    BigQuery 메타데이터를 Notion에 동기화
    
    Parameters:
    -----------
    df : pd.DataFrame
        컬럼: full_table_id, column_name, ... (기타는 무시됨)
    
    Returns:
    --------
    bool : 성공 여부
    
    Raises:
    -------
    ValueError : 설정 또는 데이터 검증 실패
    """
    
    logging.info("=" * 70)
    logging.info("🧠 Notion DB 업데이트 시작")
    logging.info("=" * 70)
    
    try:
        # 📌 Step 1: Notion 설정 검증
        logging.info("📌 Step 1: Notion 설정 검증")
        if not validate_notion_config():
            raise ValueError("Notion 설정이 완료되지 않았습니다. 환경변수를 확인하세요.")
        
        # 📌 Step 2: DataFrame 검증
        logging.info("📌 Step 2: DataFrame 검증")
        if df is None or df.empty:
            raise ValueError("DataFrame이 비어있습니다")
        
        required_cols = ["full_table_id", "column_name"]
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(
                    f"필수 컬럼 '{col}'이 없습니다.\n"
                    f"사용 가능한 컬럼: {df.columns.tolist()}"
                )
        
        logging.info(f"  ✅ DataFrame: {len(df)} rows × {len(df.columns)} cols")
        logging.info(f"     컬럼: {df.columns.tolist()}")
        logging.info(f"     샘플:\n{df[['full_table_id', 'column_name']].head(3).to_string()}")
        
        # 📌 Step 3: A 등록 (테이블)
        logging.info("📌 Step 3: DB_A (테이블) 등록")
        table_ids = sorted(set(df["full_table_id"]))
        logging.info(f"  등록할 테이블: {len(table_ids)}개")
        
        a_id_map = notion_query_id_map(DB_A_ID, "Hub 테이블명", label="DB_A 초기")
        
        new_tables = 0
        for tid in table_ids:
            if tid not in a_id_map:
                if notion_insert(DB_A_ID, "Hub 테이블명", tid):
                    new_tables += 1
                time.sleep(0.15)
        
        a_id_map = notion_query_id_map(DB_A_ID, "Hub 테이블명", label="DB_A 업데이트")
        logging.info(f"  ✅ DB_A 완료: {len(a_id_map)}개 (신규: {new_tables}개)")
        
        # 📌 Step 4: B 등록 (컬럼)
        logging.info("📌 Step 4: DB_B (컬럼) 등록")
        column_names = sorted(set(df["column_name"]))
        logging.info(f"  등록할 컬럼: {len(column_names)}개")
        
        b_id_map = notion_query_id_map(DB_B_ID, "Hub 컬럼명", label="DB_B 초기")
        
        new_columns = 0
        for cname in column_names:
            if cname not in b_id_map:
                if notion_insert(DB_B_ID, "Hub 컬럼명", cname):
                    new_columns += 1
                time.sleep(0.15)
        
        b_id_map = notion_query_id_map(DB_B_ID, "Hub 컬럼명", label="DB_B 업데이트")
        logging.info(f"  ✅ DB_B 완료: {len(b_id_map)}개 (신규: {new_columns}개)")
        
        # 📌 Step 5: C 사전 정리
        logging.info("📌 Step 5: DB_C pre-clean (고아/무효참조/중복 제거)")
        stats = cleanup_db_c_orphans(a_id_map, b_id_map, dry_run=False)
        logging.info(f"  ✅ Pre-clean 완료: {stats}")
        
        # 📌 Step 6: C UPSERT(diff)
        logging.info("📌 Step 6: DB_C (테이블-컬럼 관계) UPSERT")
        sync_db_c(df, a_id_map, b_id_map)
        logging.info(f"  ✅ DB_C 완료")
        
        logging.info("=" * 70)
        logging.info("✅ Notion DB 업데이트 완료!")
        logging.info("=" * 70)
        
        return True  # 📌 성공!
        
    except Exception as e:
        logging.error("=" * 70)
        logging.exception(f"🔥 Notion 적재 실패: {e}")
        logging.error("=" * 70)
        # 📌 raise하면 DAG에서 실패로 감지됨!
        raise ValueError(f"Notion 업데이트 실패: {e}") from e