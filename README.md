<div align="center">

# 🌐 Multi-Cloud Data Lakehouse Platform

### Production-grade data lakehouse spanning AWS · Azure · GCP

[![AWS](https://img.shields.io/badge/AWS-S3%20%7C%20Glue%20%7C%20Lake%20Formation-FF9900?style=flat-square&logo=amazon-aws&logoColor=white)](https://aws.amazon.com/)
[![Azure](https://img.shields.io/badge/Azure-ADF%20%7C%20Synapse%20%7C%20ADLS-0078D4?style=flat-square&logo=microsoft-azure&logoColor=white)](https://azure.microsoft.com/)
[![GCP](https://img.shields.io/badge/GCP-BigQuery%20%7C%20Dataflow%20%7C%20Pub%2FSub-4285F4?style=flat-square&logo=google-cloud&logoColor=white)](https://cloud.google.com/)
[![Terraform](https://img.shields.io/badge/IaC-Terraform-7B42BC?style=flat-square&logo=terraform&logoColor=white)](https://terraform.io/)
[![dbt](https://img.shields.io/badge/Transform-dbt-FF694B?style=flat-square&logo=dbt&logoColor=white)](https://getdbt.com/)
[![Airflow](https://img.shields.io/badge/Orchestrate-Airflow-017CEE?style=flat-square&logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Storage-Delta%20Lake-00ADD8?style=flat-square&logo=databricks&logoColor=white)](https://delta.io/)
[![CI/CD](https://img.shields.io/badge/CI%2FCD-GitHub%20Actions-2088FF?style=flat-square&logo=github-actions&logoColor=white)](https://github.com/features/actions)

<br/>

> Designed and implemented a unified multi-cloud data lakehouse combining Delta Lake on AWS S3, Azure Data Factory + Synapse Analytics, GCP BigQuery + Dataflow, dbt transformation layers, and Apache Airflow orchestration — demonstrating end-to-end data engineering across all three major cloud platforms.

</div>

---

## 📋 Table of Contents

- [Overview](#-overview)
- [High-Level Architecture](#-high-level-architecture)
- [Data Flow: Bronze → Silver → Gold](#-data-flow-bronze--silver--gold)
- [Component Deep-Dives](#-component-deep-dives)
  - [AWS — Delta Lake on S3](#1-aws--delta-lake-on-s3)
  - [Azure — ADF + Synapse Analytics](#2-azure--adf--synapse-analytics)
  - [GCP — BigQuery + Dataflow](#3-gcp--bigquery--dataflow)
  - [dbt — Transformation Layer](#4-dbt--transformation-layer)
  - [Airflow — Orchestration](#5-airflow--orchestration)
  - [Governance & Security](#6-governance--security)
- [CI/CD Pipeline](#-cicd-pipeline)
- [Repository Structure](#-repository-structure)
- [Technology Stack](#-technology-stack)
- [Local Development](#-local-development)
- [Skills Demonstrated](#-skills-demonstrated)

---

## 🔭 Overview

This project delivers a **production-grade, vendor-neutral data lakehouse** that unifies three hyperscaler clouds into one coherent data platform. It solves a real enterprise problem: organizations that need AWS for compute, Azure for identity and enterprise tooling, and GCP for analytics cannot afford data silos between them.

**Key outcomes this platform delivers:**

| Outcome | How |
|---|---|
| **ACID reliability on cheap object storage** | Delta Lake on S3 — merge, time travel, schema enforcement |
| **Enterprise ETL at scale** | Azure Data Factory parameterized pipelines with watermark-based incremental loads |
| **Serverless petabyte analytics** | BigQuery partitioned + clustered tables with in-warehouse ML scoring |
| **Exactly-once streaming ingestion** | Apache Beam / Dataflow with Pub/Sub and dead-letter routing |
| **Consistent SQL transformation layer** | dbt targeting both BigQuery and Snowflake with full lineage |
| **Zero-downtime deployments** | GitHub Actions CI/CD with OIDC keyless auth to all three clouds |

---

## 🏗 High-Level Architecture

```
╔══════════════════════════════════════════════════════════════════════════════════════╗
║                        MULTI-CLOUD DATA LAKEHOUSE PLATFORM                          ║
╚══════════════════════════════════════════════════════════════════════════════════════╝

┌─────────────────────────┐    ┌─────────────────────────┐    ┌─────────────────────────┐
│        DATA SOURCES     │    │      DATA SOURCES        │    │       DATA SOURCES      │
│                         │    │                          │    │                         │
│  • On-Premises SQL      │    │  • SaaS Applications     │    │  • Mobile / Web Apps    │
│  • ERP Systems          │    │  • REST APIs             │    │  • IoT Devices          │
│  • CRM Databases        │    │  • File Shares           │    │  • Clickstream Events   │
└────────────┬────────────┘    └───────────┬──────────────┘    └────────────┬────────────┘
             │                             │                                │
             ▼                             ▼                                ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
║                              INGESTION LAYER                                        ║
║                                                                                     ║
║  ┌──────────────────────┐  ┌──────────────────────┐  ┌───────────────────────────┐ ║
║  │   AZURE DATA         │  │   AWS GLUE           │  │   GCP DATAFLOW +          │ ║
║  │   FACTORY (ADF)      │  │   JOBS               │  │   PUB/SUB                 │ ║
║  │                      │  │                      │  │                           │ ║
║  │  ✦ Parameterized     │  │  ✦ Delta table        │  │  ✦ Apache Beam pipeline   │ ║
║  │    pipeline templates│  │    crawler & register │  │  ✦ Exactly-once delivery  │ ║
║  │  ✦ Watermark-based   │  │  ✦ Schema detection   │  │  ✦ PII masking in-flight  │ ║
║  │    incremental loads │  │  ✦ Partition pruning  │  │  ✦ Dead-letter routing    │ ║
║  │  ✦ Linked services   │  │                      │  │  ✦ 60s tumbling windows   │ ║
║  │    (SQL, ADLS, REST) │  │                      │  │                           │ ║
║  └──────────┬───────────┘  └──────────┬───────────┘  └─────────────┬─────────────┘ ║
╚═════════════╪══════════════════════════╪═══════════════════════════╪═══════════════╝
              │                          │                            │
              ▼                          ▼                            ▼
╔═════════════════════════════════════════════════════════════════════════════════════╗
║                              STORAGE LAYER                                          ║
║                                                                                     ║
║  ┌──────────────────────────┐  ┌───────────────────────┐  ┌──────────────────────┐ ║
║  │  AZURE ADLS GEN2         │  │  AWS S3 (DELTA LAKE)  │  │  GCP CLOUD STORAGE   │ ║
║  │                          │  │                       │  │                      │ ║
║  │  bronze/  ◄─ raw ingest  │  │  raw/                 │  │  raw-landing/        │ ║
║  │  silver/  ◄─ normalized  │  │  bronze/              │  │  dataflow-temp/      │ ║
║  │  gold/    ◄─ aggregated  │  │  silver/  ◄─ upserts  │  │                      │ ║
║  │                          │  │  gold/    ◄─ Z-ORDER  │  │  ✦ CMEK encryption   │ ║
║  │  ✦ Hierarchical namespace│  │  checkpoints/         │  │  ✦ Uniform bucket    │ ║
║  │  ✦ GRS replication       │  │                       │  │    level access      │ ║
║  │  ✦ Soft delete 30d       │  │  ✦ ACID transactions  │  │                      │ ║
║  │  ✦ CMK via Key Vault     │  │  ✦ Time travel        │  │                      │ ║
║  │                          │  │  ✦ Schema enforcement │  │                      │ ║
║  └──────────────────────────┘  └───────────────────────┘  └──────────────────────┘ ║
╚═════════════════════════════════════════════════════════════════════════════════════╝
              │                          │                            │
              └──────────────────────────┼────────────────────────────┘
                                         ▼
╔═════════════════════════════════════════════════════════════════════════════════════╗
║                           TRANSFORMATION LAYER (dbt)                                ║
║                                                                                     ║
║   Sources (AWS + Azure + GCP)                                                       ║
║        │                                                                            ║
║        ├──► staging.*   (Views)    — normalize, cast, deduplicate, mask PII         ║
║        │         │                                                                  ║
║        │         ├──► intermediate.* (Ephemeral) — reusable CTEs                   ║
║        │         │                                                                  ║
║        │         └──► marts.*      (Tables)   — fact tables, dimensions, aggs       ║
║        │                    │                                                       ║
║        │                    └──► BigQuery ML scoring (in-warehouse inference)        ║
║        │                                                                            ║
║   Targets: BigQuery (GCP)  ·  Snowflake (cross-platform)                           ║
╚═════════════════════════════════════════════════════════════════════════════════════╝
                                         │
                                         ▼
╔═════════════════════════════════════════════════════════════════════════════════════╗
║                            ANALYTICS & SERVING LAYER                                ║
║                                                                                     ║
║  ┌──────────────────────┐   ┌──────────────────────┐   ┌──────────────────────┐   ║
║  │   GCP BIGQUERY       │   │  AZURE SYNAPSE        │   │  AWS ATHENA /        │   ║
║  │                      │   │  ANALYTICS            │   │  GLUE CATALOG        │   ║
║  │  ✦ Partitioned by    │   │                       │   │                      │   ║
║  │    event_date (DAY)  │   │  ✦ Dedicated SQL Pool │   │  ✦ Glue-catalogued   │   ║
║  │  ✦ Clustered on      │   │  ✦ Apache Spark Pool  │   │    Delta tables      │   ║
║  │    event_type,       │   │  ✦ Power BI Direct    │   │  ✦ Partition pruning │   ║
║  │    country, device   │   │    Query              │   │  ✦ Parquet + Snappy  │   ║
║  │  ✦ BigQuery ML       │   │  ✦ Auto-pause 60 min  │   │                      │   ║
║  │    churn scoring     │   │                       │   │                      │   ║
║  └──────────────────────┘   └──────────────────────┘   └──────────────────────┘   ║
╚═════════════════════════════════════════════════════════════════════════════════════╝

╔═════════════════════════════════════════════════════════════════════════════════════╗
║                    ORCHESTRATION — Apache Airflow (CeleryExecutor)                  ║
║                                                                                     ║
║   Daily @ 03:00 UTC  ──►  Ingestion  ──►  dbt  ──►  BQ Load  ──►  Quality Gate    ║
╚═════════════════════════════════════════════════════════════════════════════════════╝

╔═════════════════════════════════════════════════════════════════════════════════════╗
║                    GOVERNANCE, SECURITY & COMPLIANCE (Cross-Cloud)                  ║
║                                                                                     ║
║   AWS: IAM least-privilege · Lake Formation · CloudTrail · Macie (PII discovery)   ║
║   Azure: AAD RBAC · Key Vault · Purview · Azure Monitor · Managed Identity         ║
║   GCP: Service Accounts · CMEK · Cloud KMS · VPC-SC · Cloud Logging → BQ sink      ║
╚═════════════════════════════════════════════════════════════════════════════════════╝
```

---

## 🔄 Data Flow: Bronze → Silver → Gold

```
 RAW DATA SOURCES
 ════════════════
 On-Prem SQL Server ──┐
 Cloud REST APIs    ──┤──► [ INGESTION ] ──► BRONZE (raw, append-only)
 Streaming Events   ──┘

                              │
                              │  Schema enforcement
                              │  Partition by date
                              │  ACID guarantees (Delta)
                              ▼

                       ┌─────────────────────────────────────────────┐
                       │              BRONZE LAYER                    │
                       │                                              │
                       │  • Raw, unmodified source records            │
                       │  • Append-only writes                        │
                       │  • Schema-on-write (Delta Lake)              │
                       │  • Partitioned by _partition_date            │
                       │  • Full audit trail via Delta transaction log │
                       └──────────────────┬──────────────────────────┘
                                          │
                                          │  dbt staging models
                                          │  • Column normalization
                                          │  • Type casting & validation
                                          │  • PII masking (SHA-256 hash)
                                          │  • Source deduplication
                                          │  • Multi-source UNION + dedup
                                          ▼

                       ┌─────────────────────────────────────────────┐
                       │              SILVER LAYER                    │
                       │                                              │
                       │  • Standardized, conformed schema            │
                       │  • Deduplicated (event_id as unique key)     │
                       │  • PII hashed / masked                       │
                       │  • ACID upserts via Delta MERGE              │
                       │  • Time-travel queryable (30-day window)     │
                       └──────────────────┬──────────────────────────┘
                                          │
                                          │  dbt mart models
                                          │  • Business fact tables
                                          │  • Dimension joins
                                          │  • Window functions
                                          │  • Cross-cloud enrichment
                                          │  • BigQuery ML scoring
                                          ▼

                       ┌─────────────────────────────────────────────┐
                       │               GOLD LAYER                     │
                       │                                              │
                       │  • Business-ready fact + dimension tables    │
                       │  • Incremental MERGE materialization         │
                       │  • BigQuery: partitioned (DAY) + clustered   │
                       │  • Delta: Z-ORDER optimized for fast reads   │
                       │  • ML predictions joined inline              │
                       │  • Powers BI dashboards, ad-hoc SQL, APIs    │
                       └─────────────────────────────────────────────┘
```

---

## 🔬 Component Deep-Dives

### 1. AWS — Delta Lake on S3

**Stack:** `delta-rs` (Rust engine) · AWS Glue Data Catalog · AWS Lake Formation · AWS KMS

Delta Lake brings **data warehouse reliability** to cheap S3 object storage. The `delta_utils.py` module implements the full pattern set:

```
┌─────────────────────────────────────────────────────────────────┐
│                   DELTA LAKE OPERATION PATTERNS                  │
├───────────────────┬─────────────────────────────────────────────┤
│ ACID Upsert/Merge │ matched rows updated · unmatched inserted    │
│                   │ atomically — no duplicates, no partial writes │
├───────────────────┼─────────────────────────────────────────────┤
│ Schema Enforcement│ writes rejected if column names/types differ  │
│                   │ from the registered schema — zero bad data    │
├───────────────────┼─────────────────────────────────────────────┤
│ Schema Evolution  │ additive columns via schema_mode=merge        │
│                   │ backward-compatible, zero-downtime            │
├───────────────────┼─────────────────────────────────────────────┤
│ Time Travel       │ read_as_of(as_of=datetime(2024,1,1))          │
│                   │ any historical snapshot without duplication   │
├───────────────────┼─────────────────────────────────────────────┤
│ OPTIMIZE + VACUUM │ bin-pack + Z-ORDER on user_id, event_type     │
│                   │ reduces query data scan by 60–80%             │
└───────────────────┴─────────────────────────────────────────────┘
```

**S3 bucket layout:**
```
s3://mc-lakehouse-delta-lake-{env}/
├── raw/            ← landing zone (schema-on-read, append-only)
├── bronze/         ← schema-enforced, partitioned by _partition_date
├── silver/         ← deduplicated, upserted, normalized
├── gold/           ← aggregated, business-ready, Glue-catalogued
└── checkpoints/    ← Spark / Delta streaming checkpoints
```

**Glue Crawler** runs every 6 hours on the gold layer and registers Delta tables into the Glue Data Catalog, making them queryable via Amazon Athena, EMR, and Redshift Spectrum without any data movement.

**Lake Formation** enforces column-level access control — analysts can query `gold/events` but cannot read the `ip_hash` or `email_hash` columns without explicit grants.

---

### 2. Azure — ADF + Synapse Analytics

**Stack:** Azure Data Factory v2 · Azure Synapse Analytics · ADLS Gen2 · Key Vault · Microsoft Purview

**ADF Pipeline: `PL_Ingest_OnPrem_to_ADLS`**

A fully parameterized, reusable ingestion template — a single pipeline definition handles any source table by swapping parameters:

```
┌──────────────────────────────────────────────────────────────────────┐
│              ADF PIPELINE EXECUTION FLOW                              │
│                                                                       │
│  ┌─────────────────┐                                                  │
│  │ LookupWatermark │ — reads last successful load timestamp           │
│  │                 │   from [dbo].[pipeline_control] control table    │
│  └────────┬────────┘                                                  │
│           │                                                           │
│           ▼                                                           │
│  ┌─────────────────────────────────────────────────────────────────┐ │
│  │ CopySourceToAdls                                                 │ │
│  │                                                                  │ │
│  │  Source: SQL Server (Self-Hosted IR)                             │ │
│  │  Query:  WHERE updated_at > {watermark} AND updated_at <= {date} │ │
│  │  Mode:   Dynamic Range Partitioning (parallel reads)             │ │
│  │  Sink:   ADLS Gen2 bronze/ container                             │ │
│  │  Format: Parquet + Snappy compression                            │ │
│  │  DIUs:   8 (auto-scaled data integration units)                  │ │
│  └────────┬────────────────────────────────────────────────────────┘ │
│           │                                                           │
│           ▼                                                           │
│  ┌─────────────────────────────────────────────────────────────────┐ │
│  │ CopyAdlsToSynapse                                                │ │
│  │                                                                  │ │
│  │  Source: Parquet files in bronze/ container                      │ │
│  │  Sink:   Synapse Dedicated SQL Pool (COPY command — fastest)     │ │
│  │  Mode:   UPSERT on primary key                                   │ │
│  │  Stage:  ADLS Gen2 staging area (PolyBase pattern)               │ │
│  └────────┬────────────────────────────────────────────────────────┘ │
│           │                                                           │
│           ▼                                                           │
│  ┌─────────────────┐                                                  │
│  │ UpdateWatermark │ — writes new high-watermark + rows_copied        │
│  │                 │   to control table for next run                  │
│  └─────────────────┘                                                  │
└──────────────────────────────────────────────────────────────────────┘
```

**Synapse Analytics configuration:**

| Component | Config | Purpose |
|---|---|---|
| Dedicated SQL Pool | DW500c (prod) / DW100c (dev) | Columnar MPP warehouse for BI |
| Apache Spark Pool | MemoryOptimized, 2–10 nodes | PySpark complex transforms |
| Auto-pause | 60 minutes idle | Cost optimization |
| ADLS Integration | Direct lake access | No data movement for Spark |
| AAD Admin | Managed Identity | No password-based auth |

---

### 3. GCP — BigQuery + Dataflow

**Stack:** BigQuery · Apache Beam / Cloud Dataflow · Cloud Pub/Sub · BigQuery ML · Cloud KMS

**Dataflow Streaming Pipeline architecture:**

```
┌──────────────────────────────────────────────────────────────────────┐
│              DATAFLOW PIPELINE: Pub/Sub → BigQuery                    │
│                                                                       │
│  Cloud Pub/Sub                                                        │
│  Subscription ──► ReadFromPubSub                                      │
│                        │                                             │
│                        ▼                                             │
│              WindowInto(FixedWindows(60s))                            │
│                        │                                             │
│                        ▼                                             │
│              ParseAndValidateEvent                                    │
│               ├── valid ──────────────────────────────────┐          │
│               └── dead_letter ─────────────────────┐      │          │
│                                                     │      ▼          │
│                                              BigQuery DLQ  MaskPII   │
│                                              Table         │          │
│                                                            ▼          │
│                                                   FormatForBigQuery   │
│                                                            │          │
│                                                            ▼          │
│                                                  WriteToBigQuery      │
│                                                  (Storage Write API   │
│                                                   exactly-once)       │
│                                                            │          │
│                                                            ▼          │
│                                               BigQuery events table   │
│                                               (partitioned by day,    │
│                                                clustered on type/user) │
└──────────────────────────────────────────────────────────────────────┘
```

**PII masking applied in-flight (before any data lands in BigQuery):**

| Field | Action |
|---|---|
| `ip_address` | SHA-256 hashed → stored as `ip_hash` |
| `email` | SHA-256 hashed → stored as `email_hash` |
| `raw_email`, `phone_number`, `ssn`, `credit_card` | Dropped entirely — never written |

**BigQuery table design:**
```sql
-- Partition: event_date (DAY granularity)
-- Cluster:   event_type, country_code, device_type
-- Filter:    partition filter REQUIRED — no accidental full-table scans
-- Encrypt:   CMEK via Cloud KMS key ring
-- Expire:    30 days in dev/staging, never in prod
```

**BigQuery ML — in-warehouse model scoring:**
```sql
-- No data movement to an external ML platform
-- churn_classifier model trained and served entirely within BigQuery
INSERT INTO ml_predictions
SELECT user_id, event_date, predicted_label, predicted_label_probs
FROM ML.PREDICT(
    MODEL `project.dataset.churn_classifier`,
    (SELECT * FROM fact_events WHERE event_date = CURRENT_DATE())
)
```

---

### 4. dbt — Transformation Layer

**Stack:** dbt-bigquery 1.7 · dbt-snowflake 1.7 · dbt-utils

**Model DAG (lineage):**

```
 SOURCES
 ═══════
 aws_delta.raw_events          ─────────────────────────┐
 gcp_streaming.events          ─────────────────────────┤
 azure_synapse.transactions    ────────────┐             │
 aws_delta.raw_customers       ───────┐    │             │
                                      │    │             │
                                      ▼    ▼             ▼
 STAGING (Views)              stg_customers  stg_transactions  stg_events
 ════════════════                      │         │              │
                                       └────────┬┘              │
                                                │               │
                                                ▼               ▼
 MARTS (Tables)                              dim_customers   fct_events
 ══════════════                                         (incremental MERGE
                                                         partitioned + clustered
                                                         + BigQuery ML join)
```

**Incremental strategy on `fct_events`:**
```
On full refresh: REPLACE entire table partition
On incremental:  MERGE — matched rows updated, unmatched inserted
Lookback window: 3 days (late-arriving data tolerance)
Unique key:      [event_date, user_id, event_type]
```

**dbt test coverage:**

| Test Type | Example | Severity |
|---|---|---|
| `not_null` | `event_id`, `event_timestamp` | error |
| `unique` | `event_id` per source | error |
| `accepted_values` | `event_type` in allowed list | error |
| Source freshness | warn >12h, error >24h | warn/error |
| Custom data | Cross-cloud record count reconciliation | error |

---

### 5. Airflow — Orchestration

**Stack:** Airflow 2.8 · CeleryExecutor · PostgreSQL metadata DB · Redis broker

**Master DAG: `multi_cloud_lakehouse_orchestration`** — runs daily at 03:00 UTC

```
START
  │
  ├─────────────────────────────────────────────────────────────────┐
  │              PHASE 1: INGESTION  (parallel TaskGroup)            │
  │                                                                  │
  │   AWS ──► S3KeySensor ──────────────► GlueJobOperator           │
  │           (wait for raw files)         (register Delta tables)   │
  │                                                                  │
  │   Azure ──► SimpleHttpOperator ─────────────────────────────    │
  │             (trigger ADF REST API — PL_Ingest_OnPrem_to_ADLS)   │
  │                                                                  │
  │   GCP ──► PubSubPullSensor ─────► DataflowStartFlexTemplate     │
  │           (confirm messages)        (launch Beam pipeline)       │
  └──────────────────────────────────────────────────────────┬───────┘
                                                             │
  ┌──────────────────────────────────────────────────────────▼───────┐
  │            PHASE 2: DBT TRANSFORMS  (sequential TaskGroup)        │
  │                                                                   │
  │   @task dbt_run_staging  ──►  @task dbt_run_marts  ──►  @task   │
  │   (stg_events,               (fct_events,              dbt_test  │
  │    stg_customers,             dim_customers,            (all      │
  │    stg_transactions)          ml_predictions)            tests)   │
  └──────────────────────────────────────────────────────────┬───────┘
                                                             │
  ┌──────────────────────────────────────────────────────────▼───────┐
  │           PHASE 3: BIGQUERY LOAD  (TaskGroup)                     │
  │                                                                   │
  │   BigQueryInsertJobOperator ──► BigQueryInsertJobOperator         │
  │   (load fact_events partition)   (ML.PREDICT churn scoring)       │
  └──────────────────────────────────────────────────────────┬───────┘
                                                             │
  ┌──────────────────────────────────────────────────────────▼───────┐
  │           PHASE 4: QUALITY GATE  (TaskGroup)                      │
  │                                                                   │
  │   BigQueryValueCheckOperator ──► BigQueryValueCheckOperator       │
  │   (row count ≥ 1,000 ±5%)        (NULL user_id count = 0)        │
  │                      │                                            │
  │                      └──► @task cross_cloud_reconciliation        │
  │                           (AWS S3 count vs GCP BQ count < 1% diff)│
  └──────────────────────────────────────────────────────────┬───────┘
                                                             │
                                                            END
```

---

### 6. Governance & Security

Security is applied at every layer across all three clouds following the principle of **least privilege, encrypt everything, audit everything.**

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    CROSS-CLOUD SECURITY MATRIX                           │
├──────────────────┬──────────────────┬──────────────────┬────────────────┤
│ CONCERN          │ AWS              │ AZURE            │ GCP            │
├──────────────────┼──────────────────┼──────────────────┼────────────────┤
│ Identity         │ IAM Roles        │ Managed Identity │ Service        │
│                  │ (no static keys) │ + AAD RBAC       │ Accounts +     │
│                  │                  │                  │ OIDC           │
├──────────────────┼──────────────────┼──────────────────┼────────────────┤
│ Key Management   │ AWS KMS          │ Azure Key Vault  │ Cloud KMS      │
│                  │ 90-day rotation  │ Purge protection │ Key Ring       │
│                  │                  │ Soft delete 90d  │ 90-day rotate  │
├──────────────────┼──────────────────┼──────────────────┼────────────────┤
│ Encryption       │ SSE-KMS on all   │ Storage Account  │ CMEK on BQ     │
│ at rest          │ S3 buckets       │ CMK via KV       │ + GCS buckets  │
├──────────────────┼──────────────────┼──────────────────┼────────────────┤
│ Encryption       │ TLS 1.2+         │ HTTPS only +     │ VPC Service    │
│ in transit       │ enforced         │ Private Endpoints│ Controls       │
├──────────────────┼──────────────────┼──────────────────┼────────────────┤
│ PII Masking      │ Column-level via │ Dynamic Data     │ SHA-256 hash   │
│                  │ Glue + LF grants │ Masking (Synapse) │ in Dataflow    │
├──────────────────┼──────────────────┼──────────────────┼────────────────┤
│ Audit Logging    │ CloudTrail       │ Azure Monitor +  │ Cloud Logging  │
│                  │ multi-region,    │ Log Analytics    │ → BQ sink      │
│                  │ log validation   │ 90d retention    │                │
├──────────────────┼──────────────────┼──────────────────┼────────────────┤
│ Fine-grained     │ Lake Formation   │ Synapse RBAC +   │ BQ column-     │
│ access control   │ column grants    │ Row-Level Sec.   │ level security │
├──────────────────┼──────────────────┼──────────────────┼────────────────┤
│ Public access    │ S3 Block Public  │ Network rules    │ Public access  │
│ prevention       │ Access (all 4)   │ default Deny     │ prevention     │
│                  │                  │ bypass AzSvcs    │ enforced       │
├──────────────────┼──────────────────┼──────────────────┼────────────────┤
│ PII Discovery    │ AWS Macie        │ Microsoft        │ Cloud DLP      │
│                  │ (prod only)      │ Purview          │ (via policy)   │
└──────────────────┴──────────────────┴──────────────────┴────────────────┘
```

---

## 🚀 CI/CD Pipeline

**OIDC keyless authentication** to all three clouds — zero static credentials stored anywhere.

```
 TRIGGER: push to main/develop · PR to main · manual workflow_dispatch
                              │
                              ▼
          ┌───────────────────────────────────────┐
          │         lint-and-test                  │
          │                                        │
          │  • Ruff linting (Python)               │
          │  • mypy type checking                  │
          │  • pytest unit tests                   │
          │  • Coverage report → Codecov           │
          └──────────────────┬────────────────────┘
                             │
              ┌──────────────┴──────────────┐
              │                             │
              ▼                             ▼
  ┌───────────────────────┐   ┌───────────────────────────┐
  │    dbt-validate        │   │    terraform-validate      │
  │                        │   │                            │
  │  • dbt deps            │   │  • OIDC auth (AWS+AZ+GCP) │
  │  • dbt compile         │   │  • terraform init          │
  │  • dbt parse           │   │  • terraform validate      │
  │    (YAML schema check) │   │  • terraform fmt -check    │
  │                        │   │  • terraform plan          │
  │                        │   │  • PR comment with diff    │
  └───────────────────────┘   └───────────────────────────┘
              │                             │
              └──────────────┬──────────────┘
                             │
                             ▼
          ┌───────────────────────────────────────┐
          │           docker-build                 │
          │                                        │
          │  • Build custom Airflow image          │
          │  • Push to GHCR (GitHub Container Reg) │
          │  • GHA cache for fast rebuilds         │
          └──────────────────┬────────────────────┘
                             │
              ┌──────────────┴──────────────┐
              │                             │
              ▼                             ▼
  ┌───────────────────────┐   ┌───────────────────────────┐
  │   deploy-staging       │   │    deploy-production       │
  │   (develop branch)     │   │    (main branch)           │
  │                        │   │                            │
  │  • Sync DAGs to GCS    │   │  • terraform apply         │
  │  • dbt run (staging)   │   │  • Sync DAGs to GCS        │
  │  • dbt test (staging)  │   │  • Slack notification      │
  └───────────────────────┘   └───────────────────────────┘
```

---

## 📁 Repository Structure

```
multi-cloud-data-lakehouse/
│
├── README.md                                   ← You are here
├── requirements.txt                            ← Python dependencies (all providers)
├── .gitignore
│
├── .github/
│   └── workflows/
│       └── ci_cd.yml                           ← Full CI/CD pipeline (OIDC auth)
│
├── terraform/                                  ← Infrastructure as Code
│   ├── main.tf                                 ← Root module: wires AWS + Azure + GCP
│   ├── variables.tf                            ← Input variable definitions
│   ├── outputs.tf                              ← Infrastructure outputs
│   ├── dev.tfvars                              ← Dev environment variable values
│   ├── aws/
│   │   ├── main.tf                             ← S3 Delta Lake · Glue · Lake Formation · KMS · CloudTrail
│   │   └── variables.tf
│   ├── azure/
│   │   ├── main.tf                             ← ADLS Gen2 · ADF · Synapse · Key Vault · Purview · RBAC
│   │   └── variables.tf
│   └── gcp/
│       ├── main.tf                             ← BigQuery · Dataflow · Pub/Sub · GCS · KMS · IAM SAs
│       └── variables.tf
│
├── airflow/
│   └── dags/
│       └── multi_cloud_lakehouse_dag.py        ← Master 4-phase orchestration DAG
│
├── dbt/
│   ├── dbt_project.yml                         ← Project config: materialization · tags · layers
│   ├── profiles.yml                            ← BigQuery + Snowflake connection profiles
│   └── models/
│       ├── staging/
│       │   ├── sources.yml                     ← Source definitions + freshness checks
│       │   └── stg_events.sql                  ← Bronze→Silver: normalize · deduplicate · mask
│       └── marts/
│           └── fct_events.sql                  ← Gold: incremental fact table (BQ partitioned)
│
├── aws/
│   └── delta_lake/
│       └── delta_utils.py                      ← ACID upserts · time travel · schema enforcement · vacuum
│
├── azure/
│   └── adf_pipelines/
│       └── PL_Ingest_OnPrem_to_ADLS.json       ← Parameterized ADF pipeline (watermark + upsert)
│
├── gcp/
│   ├── dataflow/
│   │   └── pubsub_to_bigquery.py               ← Apache Beam exactly-once streaming pipeline
│   └── bigquery/
│       └── schemas/
│           └── events.json                     ← BigQuery events table schema definition
│
└── docker/
    ├── Dockerfile.airflow                      ← Custom Airflow image (all cloud SDKs included)
    └── docker-compose.yml                      ← Full local dev stack (Airflow + Postgres + Redis + dbt)
```

---

## 🛠 Technology Stack

| Layer | Technology | Version | Role |
|---|---|---|---|
| **Orchestration** | Apache Airflow | 2.8.x | DAG scheduling, cross-cloud coordination |
| **Transformation** | dbt | 1.7.x | SQL models, testing, documentation, lineage |
| **AWS Storage** | Delta Lake on S3 | 3.1.x | ACID open table format on object storage |
| **AWS Catalog** | AWS Glue + Lake Formation | — | Metadata catalog, fine-grained column access |
| **Azure ETL** | Azure Data Factory | v2 | Parameterized ingestion pipelines, linked services |
| **Azure Warehouse** | Azure Synapse Analytics | — | MPP SQL pool + Spark pool |
| **Azure Storage** | ADLS Gen2 | — | Hierarchical namespace blob storage |
| **GCP Analytics** | BigQuery | — | Serverless analytics engine + in-warehouse ML |
| **GCP Streaming** | Apache Beam / Dataflow | 2.54.x | Exactly-once Pub/Sub to BigQuery pipeline |
| **IaC** | Terraform | 1.6.x | Multi-cloud resource provisioning |
| **Containers** | Docker + Compose | 24.x | Local dev environment, CI/CD build artifacts |
| **CI/CD** | GitHub Actions | — | Lint, test, Terraform plan, zero-downtime deploy |
| **Secrets (AWS)** | AWS KMS | — | Encryption key management, 90-day rotation |
| **Secrets (Azure)** | Azure Key Vault | — | Secrets, CMK, purge protection |
| **Secrets (GCP)** | Cloud KMS | — | CMEK for BigQuery + GCS |
| **Language** | Python | 3.11 | Airflow DAGs, Dataflow pipeline, Delta utilities |

---

## 💻 Local Development

### Prerequisites

- Docker Desktop 4.x+
- Terraform 1.6+
- Python 3.11+
- GCP service account JSON keyfile (for BigQuery access)
- AWS credentials configured (`~/.aws/credentials` or env vars)

### 1. Clone the repository

```bash
git clone https://github.com/YOUR_USERNAME/multi-cloud-data-lakehouse.git
cd multi-cloud-data-lakehouse
```

### 2. Start the full local stack

```bash
# Copy and configure environment variables
cp .env.example .env
# Edit .env with your GCP_PROJECT_ID, AWS credentials, Azure credentials

# Build and start all services
docker compose -f docker/docker-compose.yml up -d

# Follow init logs
docker compose -f docker/docker-compose.yml logs -f airflow-init
```

Airflow UI: **http://localhost:8080** (username: `admin` / password: `admin`)
Celery Flower: **http://localhost:5555**

### 3. Run dbt transformations

```bash
# Open interactive dbt container
docker compose -f docker/docker-compose.yml run --rm dbt bash

# Inside the container:
dbt deps                          # install packages
dbt compile --target dev          # syntax check (no DB connection needed)
dbt run --select staging.*        # run staging models
dbt test                          # run all schema + data tests
dbt docs generate && dbt docs serve  # generate lineage docs at localhost:8080
```

### 4. Provision cloud infrastructure

```bash
cd terraform/

# Authenticate to all three clouds
aws configure
az login
gcloud auth application-default login

# Initialize with remote state backend
terraform init

# Preview changes
terraform plan \
  -var="environment=dev" \
  -var-file="dev.tfvars" \
  -var="gcp_project_id=YOUR_PROJECT" \
  -var="azure_subscription_id=YOUR_SUB_ID" \
  -var="synapse_admin_password=YourP@ssw0rd"

# Apply
terraform apply -var="environment=dev" -var-file="dev.tfvars" ...
```

### 5. Run the Dataflow pipeline locally (DirectRunner)

```bash
cd gcp/dataflow/

pip install apache-beam[gcp]==2.54.0

python pubsub_to_bigquery.py \
  --runner DirectRunner \
  --input_subscription projects/YOUR_PROJECT/subscriptions/YOUR_SUB \
  --output_table YOUR_PROJECT:mc_lakehouse_dev.events \
  --dead_letter_table YOUR_PROJECT:mc_lakehouse_dev.events_dlq
```

---

## ✅ Skills Demonstrated

| Skill Area | What's Implemented |
|---|---|
| **Delta Lake on AWS** | ACID upserts (MERGE), schema enforcement, time travel, Z-ORDER OPTIMIZE, VACUUM, schema evolution, Glue Catalog registration |
| **Azure Data Factory** | Parameterized pipeline templates, watermark-based incremental loads, linked services (SQL, ADLS, Synapse), dynamic range partitioning |
| **Azure Synapse Analytics** | Dedicated SQL Pool provisioning, Spark Pool configuration, COPY command bulk-load, ADLS Gen2 integration, auto-pause |
| **GCP BigQuery** | Partitioned + clustered table design, partition filter enforcement, CMEK encryption, BigQuery ML in-warehouse scoring, audit log sink |
| **GCP Dataflow** | Apache Beam Flex Template, exactly-once semantics (Storage Write API), tumbling windows, dead-letter pattern, THROUGHPUT_BASED autoscaling |
| **dbt** | Multi-target profiles (BigQuery + Snowflake), staging/marts layer separation, incremental MERGE materialization, schema tests, source freshness |
| **Apache Airflow** | TaskGroups, S3/PubSub sensors, cross-cloud operators, CeleryExecutor, quality gate tasks, `TriggerRule.ALL_SUCCESS` |
| **Terraform IaC** | Multi-cloud module abstraction, remote S3 state + DynamoDB lock, OIDC provider auth, environment-parameterized deployments |
| **Data Governance** | IAM least-privilege (all 3 clouds), RBAC, CMEK, column-level masking, PII hashing, CloudTrail + Azure Monitor + Cloud Logging audit |
| **CI/CD** | GitHub Actions, OIDC keyless cloud auth, parallel job execution, Terraform plan as PR comment, zero-downtime deploy gates |

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.

---

<div align="center">

Built across ☁️ **AWS** · ☁️ **Azure** · ☁️ **GCP**

*Production-grade multi-cloud data engineering — Delta Lake · Synapse · BigQuery · dbt · Airflow · Terraform*

</div>
