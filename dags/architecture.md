# DAG Architecture

```mermaid
flowchart TD

  %% -- 외부 데이터 소스 --
  subgraph SRC["외부 데이터 소스"]
    DP["dataplatform-services\n(ServiceData / reporting)"]
    NOTION_API["Notion API"]
    ST_API["SensorTower API"]
    GCS["GCS Bucket"]
    YT_API["YouTube API"]
    STEAM_API["Steam API"]
  end

  %% -- Core ETL Chain --
  subgraph CORE["Core ETL Pipeline (Dataset-triggered)"]
    direction TB
    DIM["ETL_dimension\nKST 06:30\n───────────\ndim_exchange\ndim_ip4_country\ndim_d_game 등"]

    FACT["ETL_ALL_Fact\n(ETL_dimension 완료 후)\n───────────\nf_tracker_install\nf_common_register/access\nf_common_payment\nf_funnel_access_first\nf_IAA_*\nf_user_map / f_user_map_char"]

    STATICS["ETL_statics\n(ETL_ALL_Fact 완료 후)\n───────────\nstatics_daily_kpi\nstatics_weekly_kpi\nstatics_monthly_kpi"]

    DIM -->|"Dataset: ETL_dimension"| FACT
    FACT -->|"Dataset: ETL_ALL_Fact"| STATICS
  end

  %% -- 모니터링 --
  subgraph MON["Monitoring"]
    MON_ETL["ETL_Fact_Monitoring_daily\nKST 08:20\nfact 테이블 간 수치 오차 -> Email"]
    MON_PG["postgres_dag_run_monitoring\nKST 09:40"]
    MON_MAIL["postgres_monitoring_Marketing_Mailing\nKST 14:20"]
  end

  %% -- 마케팅 메일 --
  subgraph MAIL["Marketing Mailing (KST 14:xx)"]
    MAIL_RESU["Marketing_Mailing_RESU\n14:01"]
    MAIL_POTC["Marketing_Mailing_POTC\n14:02"]
    MAIL_DS["Marketing_Mailing_DS\n14:03"]
    MAIL_GW["Marketing_Mailing_GW\n14:04"]
    MAIL_WWM["Marketing_Mailing_WWM\n14:05"]
    MAIL_ALL["Marketing_Mailing_ALL_Project\n14:10"]
  end

  %% -- Game Framework --
  subgraph GF["Game Framework (BQ -> Gemini -> Notion)"]
    GF_GBTW_H["game_framework_gbtw_history\nKST 05:00"]
    GF_GBTW_M["game_framework_gbtw_main\nKST 05:30"]
    GF_DAILY["game_framework_daily"]
    GF_SUMMARY["game_framework_summary\n(Gemini AI 요약 -> Notion)"]
    GF_GLOBAL["game_framework_global_ua"]
    GF_INHOUSE["game_framework_inhouse"]
    GF_LT["game_framework_longterm_sales"]
    GF_ROAS["game_framework_newuser_roas"]
    GF_RGRP["game_framework_rgroup_IAP_gem_ruby"]
  end

  %% -- Notion 연동 --
  subgraph NOTION_GRP["Notion Integration"]
    NP_PROD["injoy_notion_permission_producer\nKST 08:10"]
    NP_CONS["injoy_notion_permission_consumer\n(triggered)"]
    NM_PROD["injoy_monitoringdata_producer_ver2\nKST 09:05"]
    NM_C1["injoy_monitoringdata_consumer_1\n(triggered)"]
    NM_C2["injoy_monitoringdata_consumer_2\n(triggered)"]
    NDB["notiondb_bq\nKST 06:00"]
    NML["notion_master_list\nKST 02:00"]
    BQ_NS["bq_notion_metadata_sync\n매 5분"]

    NP_PROD -->|"Dataset"| NP_CONS
    NM_PROD -->|"Dataset"| NM_C1
    NM_PROD -->|"Dataset"| NM_C2
  end

  %% -- SensorTower --
  subgraph ST_GRP["SensorTower"]
    ST_DL["SENSORTOWER_downloads_revenue\nKST 06:30"]
    ST_SRC2["SENSORTOWER_downloads_by_source\nKST 03:00"]
    ST_AU["SENSORTOWER_active_users\nKST 06:40"]
  end

  %% -- Creative GCS -> BQ --
  subgraph CREATIVE["Creative GCS -> BQ"]
    GCS_UP["gcs_creative_file_upload\nKST 06:00"]
    BQ_CREATE["bq_create_ms_table\n(triggered)"]
    BQ_INSERT["bq_insert_ms_table\n(triggered)"]

    GCS_UP -->|"Dataset"| BQ_CREATE
    BQ_CREATE -->|"Dataset"| BQ_INSERT
  end

  %% -- Crawling --
  subgraph CRAWL["Crawling"]
    CR_IH["inhouse_account_ETL\nKST 04:30"]
    CR_MG["monthly_goal_ETL\nKST 04:00"]
    CR_PKG["Package_Info_ETL\nKST 04:00"]
  end

  %% -- 기타 독립 DAG --
  subgraph MISC["기타"]
    FSF2_ETL["fsf2_cbt_pre_register_etl\nKST 09:30"]
    FSF2_MAIL["fsf2_cbt_pre_register_report\n(triggered)"]
    JOYPLE["joyple_survey_ETL\nKST 06:00"]
    STEAM["STEAM_wishlist_FSF2\nKST 06:30"]
    YT["youtube_FSF2_ETL\nKST 05:00"]
    UA["ua_data_for_agency\nKST 08:00"]
    RESU["RESU_launchReport_easia_main"]
    BQ_DB["notion_bq_to_db_dag\nKST 08:30"]

    FSF2_ETL -->|"Dataset"| FSF2_MAIL
  end

  %% -- 데이터 흐름 --
  DP --> CORE
  NOTION_API --> NOTION_GRP
  ST_API --> ST_GRP
  GCS --> CREATIVE
  YT_API --> YT
  STEAM_API --> STEAM

  STATICS --> MON_ETL
  FACT -.->|"참조"| MAIL
  FACT -.->|"참조"| GF
```

## Dataset 트리거 체인 요약

| Producer DAG | Dataset 키 | Consumer DAG |
|---|---|---|
| `ETL_dimension` | `ETL_dimension` | `ETL_ALL_Fact` |
| `ETL_ALL_Fact` | `ETL_ALL_Fact` | `ETL_statics` |
| `gcs_creative_file_upload` | `gcs_creative_file_upload` | `bq_create_ms_table` |
| `bq_create_ms_table` | `bq_create_ms_table` | `bq_insert_ms_table` |
| `injoy_notion_permission_producer` | `injoy_notion_permission_producer` | `injoy_notion_permission_consumer` |
| `injoy_monitoringdata_producer_ver2` | `injoy_monitoringdata_producer2` | `injoy_monitoringdata_consumer_1/2` |
| `fsf2_cbt_pre_register_etl` | `fsf2_cbt_pre_register_etl` | `fsf2_cbt_pre_register_report` |
