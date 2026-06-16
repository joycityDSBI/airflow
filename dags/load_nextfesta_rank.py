"""
Steam Next Fest 페이지에서 FSF2(FreeStyle Football 2)의 순위를 1시간 마다 수집하여
BigQuery datahub-478802.external_data.FSF2_steam_nextfest_rank 테이블에 1행 APPEND.

Steam의 'Show more' AJAX 엔드포인트(saleaction/ajaxgetsaledynamicappquery)를 직접
호출하므로 헤드리스 브라우저 불필요.
"""

import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Iterator, Optional

import requests
from google.cloud import bigquery
from google.oauth2 import service_account

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator


logger = logging.getLogger(__name__)

# ── BigQuery 설정 ─────────────────────────────────────────────────────────
PROJECT_ID        = "datahub-478802"
BQ_DATASET_ID     = "external_data"
BQ_TABLE_ID       = "FSF2_steam_nextfest_rank"
BQ_DATASET_REGION = "US"
TARGET_TABLE      = f"{PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}"

# ── Steam Next Fest 설정 ──────────────────────────────────────────────────
NEXTFEST_URL = "https://store.steampowered.com/sale/nextfest"
AJAX_URL     = "https://store.steampowered.com/saleaction/ajaxgetsaledynamicappquery"

TAB = 23

# flavor 별 수집 대상 (appid, 표기명)
# - trendingwishlisted, topwishlisted: 메인 게임 FreeStyle Football 2
# - dailyactiveuserdemo: 데모 빌드 FreeStyle Football 2 Demo (별도 Steam 앱)
FSF2_APPID      = 4004820
FSF2_DEMO_APPID = 4599930  # TODO: FreeStyle Football 2 Demo 의 Steam appid 입력 필요

FLAVOR_TARGETS = {
    "trendingwishlisted":  {"appid": FSF2_APPID,      "name": "FreeStyle Football 2"},
    "topwishlisted":       {"appid": FSF2_APPID,      "name": "FreeStyle Football 2"},
    "dailyactiveuserdemo": {"appid": FSF2_DEMO_APPID, "name": "FreeStyle Football 2 Demo"},
}
FLAVORS = list(FLAVOR_TARGETS.keys())

# 장르별 facet 필터 — AJAX 의 strFacetFilter 파라미터로 전달.
# 페이지의 sale_item_browser 정의에서 facetValues[*].rgStoreTagFilter 와 동일한 형식
#   {"type": 2, "value": <Steam Store TagID>}
# 추가 장르가 필요하면 Steam Store TagID 만 알면 됨.
# 참고 tagID: Sports=701, Racing=699, RPG=122, Action=19, eSports=5055,
#             Co-op=1685, Local Co-Op=3841, Online Co-Op=3843
GENRE_FACETS = {
    "all":         None,
    "sports":      {"type": 2, "value": 701},
    "esports":     {"type": 2, "value": 5055},
    "online_coop": {"type": 2, "value": 3843},
}
GENRES = list(GENRE_FACETS.keys())

PAGE_SIZE      = 50
MAX_PAGES      = 100   # 안전 상한 (4358 / 50 ≈ 88)
REQUEST_DELAY  = 0.3   # Steam 부하 회피
HTTP_TIMEOUT   = 15

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0 Safari/537.36"
)


def get_gcp_credentials():
    """Airflow Variable에서 GCP 자격 증명을 로드합니다."""
    credentials_json = Variable.get("GOOGLE_CREDENTIAL_JSON")
    cred_dict = json.loads(credentials_json)
    if "private_key" in cred_dict:
        cred_dict["private_key"] = cred_dict["private_key"].replace("\\n", "\n")

    scopes = [
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/bigquery",
    ]
    return service_account.Credentials.from_service_account_info(cred_dict, scopes=scopes)


def fetch_event_context(flavor: str) -> dict:
    """nextfest 페이지 HTML에서 매 행사마다 바뀌는 식별자들을 파싱."""
    logger.info("Next Fest 페이지에서 이벤트 컨텍스트 파싱 중 (flavor=%s) …", flavor)
    resp = requests.get(
        NEXTFEST_URL,
        params={"tab": TAB, "flavor": flavor},
        headers={"User-Agent": USER_AGENT, "Accept-Language": "en-US,en;q=0.9"},
        timeout=HTTP_TIMEOUT,
    )
    resp.raise_for_status()
    html = resp.text

    def find_one(pattern: str, label: str, group: int = 1) -> str:
        m = re.search(pattern, html)
        if not m:
            raise RuntimeError(f"{label} 파싱 실패 — Steam 페이지 구조 변경 가능성")
        return m.group(group)

    # data-* 속성의 값들은 &quot;로 인코딩되어 있음. 일부 값은 JSON 문자열 안에 다시
    # 인코딩되어 \&quot; (백슬래시 + &quot;) 형태인 경우도 있어 둘 다 허용한다.
    Q = r'\\?&quot;'  # \"  또는 "  (HTML-encoded)
    clan_account_id   = int(find_one(rf'{Q}CLANACCOUNTID{Q}:(\d+)', "CLANACCOUNTID"))
    announcement_gid  = find_one(rf'{Q}ANNOUNCEMENT_GID{Q}:{Q}(\d+){Q}', "ANNOUNCEMENT_GID")
    section_unique_id = int(find_one(
        rf'{Q}section_type{Q}:{Q}sale_item_browser{Q},{Q}unique_id{Q}:(\d+)',
        "section_unique_id",
    ))
    event_name = find_one(rf'{Q}event_name{Q}:{Q}([^&\\]+?){Q}', "event_name")

    ctx = {
        "clan_account_id": clan_account_id,
        "announcement_gid": announcement_gid,
        "section_unique_id": section_unique_id,
        "event_name": event_name,
    }
    logger.info("  파싱 결과: %s", ctx)
    return ctx


def iter_ranked_appids(
    ctx: dict,
    flavor: str,
    facet_filter: Optional[dict] = None,
) -> Iterator[tuple]:
    """(rank, appid, match_count)를 순서대로 yield. dedup 적용.

    facet_filter: rgStoreTagFilter 형식의 dict. 예: {"type": 2, "value": 701}.
                  None 이면 필터 미적용.
    """
    seen: set = set()
    cursor = 0
    rank = 0

    str_facet_filter = json.dumps(facet_filter) if facet_filter else ""
    base_params = {
        "cc": "US",
        "l": "english",
        "clanAccountID": ctx["clan_account_id"],
        "clanAnnouncementGID": ctx["announcement_gid"],
        "flavor": flavor,
        "strFacetFilter": str_facet_filter,
        "tabuniqueid": TAB,
        "sectionuniqueid": ctx["section_unique_id"],
        "return_capsules": "false",
        "count": PAGE_SIZE,
    }

    for page_idx in range(MAX_PAGES):
        params = {**base_params, "start": cursor}
        r = requests.get(
            AJAX_URL,
            params=params,
            headers={"User-Agent": USER_AGENT},
            timeout=HTTP_TIMEOUT,
        )
        r.raise_for_status()
        data = r.json()
        if data.get("success") != 1:
            raise RuntimeError(f"AJAX 응답 실패: {data}")

        appids = data.get("appids") or []
        match_count = data.get("match_count")

        new_in_page = 0
        for appid in appids:
            if appid in seen:
                continue
            seen.add(appid)
            rank += 1
            new_in_page += 1
            yield rank, appid, match_count

        logger.info(
            "  page %d: start=%d returned=%d new=%d cumulative_rank=%d",
            page_idx, cursor, len(appids), new_in_page, rank,
        )

        if not data.get("possible_has_more"):
            logger.info("  possible_has_more=false → 페이지네이션 종료")
            return
        if new_in_page == 0:
            logger.warning("  새 appid 0개 → 무한 루프 회피, 종료")
            return

        cursor = data.get("solr_index", cursor + PAGE_SIZE)
        time.sleep(REQUEST_DELAY)


def find_rank_in_context(
    ctx: dict,
    target_appid: int,
    flavor: str,
    genre: str,
    facet_filter: Optional[dict] = None,
):
    """미리 fetch 한 ctx 위에서 target_appid의 순위를 찾는다. 없으면 (None, total)."""
    last_match_count: Optional[int] = None
    for rank, appid, match_count in iter_ranked_appids(ctx, flavor, facet_filter):
        last_match_count = match_count
        if appid == target_appid:
            logger.info(
                "✓ [%s/%s] target appid=%d found at rank=%d",
                flavor, genre, target_appid, rank,
            )
            return rank, match_count
    logger.warning(
        "[%s/%s] target appid=%d 을(를) 리스트 안에서 찾지 못함",
        flavor, genre, target_appid,
    )
    return None, last_match_count


def get_schema():
    return [
        bigquery.SchemaField("collected_at",      "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("event_name",        "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("tab",               "INT64",     mode="REQUIRED"),
        bigquery.SchemaField("flavor",            "STRING",    mode="REQUIRED"),
        # 기존 테이블에 ALLOW_FIELD_ADDITION 으로 컬럼을 추가하려면 NULLABLE 이어야 한다.
        bigquery.SchemaField("genre",             "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("app_id",            "INT64",     mode="REQUIRED"),
        bigquery.SchemaField("app_name",          "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("rank",              "INT64",     mode="NULLABLE"),
        bigquery.SchemaField("total_match_count", "INT64",     mode="NULLABLE"),
    ]


def upload_rows(bq_client: bigquery.Client, rows: list) -> None:
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=get_schema(),
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        # 이전 스키마(genre 컬럼이 없던 시절) 테이블에도 새 컬럼을 자동 추가.
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )
    job = bq_client.load_table_from_json(rows, TARGET_TABLE, job_config=job_config)
    job.result()
    if job.errors:
        raise RuntimeError(f"BigQuery 업로드 오류: {job.errors}")


def collect_and_load_nextfest_rank():
    """Next Fest의 모든 (flavor × genre) 조합에서 FSF2 순위를 수집하여 BigQuery에 일괄 APPEND."""
    logger.info(
        "=== Next Fest rank 수집 시작 (flavors=%s, genres=%s) ===",
        FLAVORS, GENRES,
    )

    # 동일 실행에서 수집된 모든 행은 같은 collected_at 을 공유한다.
    collected_at = datetime.now(timezone.utc).isoformat()

    rows = []
    for flavor in FLAVORS:
        target = FLAVOR_TARGETS[flavor]
        target_appid = target["appid"]
        target_name  = target["name"]

        # 페이지 HTML이 주는 event-level 식별자(clan_account_id 등)는 장르 필터와
        # 무관하므로 flavor 당 1번만 fetch 하여 모든 genre 에 재사용한다.
        ctx = fetch_event_context(flavor)

        for genre in GENRES:
            facet_filter = GENRE_FACETS[genre]
            rank, total_match_count = find_rank_in_context(
                ctx, target_appid, flavor, genre, facet_filter,
            )
            rows.append({
                "collected_at":      collected_at,
                "event_name":        ctx["event_name"],
                "tab":               TAB,
                "flavor":            flavor,
                "genre":             genre,
                "app_id":            target_appid,
                "app_name":          target_name,
                "rank":              rank,
                "total_match_count": total_match_count,
            })
            logger.info(
                "  → flavor=%s genre=%s app=%s(%d) rank=%s total=%s",
                flavor, genre, target_name, target_appid, rank, total_match_count,
            )

    logger.info("BigQuery 클라이언트 초기화 중 …")
    creds = get_gcp_credentials()
    bq_client = bigquery.Client(project=PROJECT_ID, credentials=creds)
    upload_rows(bq_client, rows)

    logger.info(
        "✓ 적재 완료: collected_at=%s rows=%d",
        collected_at, len(rows),
    )


# ==========================================
# DAG 정의
# ==========================================
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="FSF2_steam_nextfest_rank",
    default_args=default_args,
    description="Steam Next Fest에서 FSF2 순위를 1시간 마다 수집하여 BigQuery에 적재",
    schedule="7 * * * *",  # 매시 7분 실행 (분 0 회피로 다른 cron 과의 부하 spike 분산)
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    tags=["fsf2", "steam", "nextfest", "bigquery"],
) as dag:

    collect_and_load_task = PythonOperator(
        task_id="collect_and_load_nextfest_rank",
        python_callable=collect_and_load_nextfest_rank,
    )
