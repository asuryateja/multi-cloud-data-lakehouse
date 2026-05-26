# Multi-Cloud Data Lakehouse Platform
### AWS · Azure · GCP · Delta Lake · dbt · Airflow

<p align="center">
  <img src="https://img.shields.io/badge/AWS-S3%20%7C%20Glue%20%7C%20Lake%20Formation-FF9900?style=for-the-badge&logo=amazon-aws&logoColor=white"/>
  <img src="https://img.shields.io/badge/Azure-ADF%20%7C%20Synapse%20%7C%20ADLS-0078D4?style=for-the-badge&logo=microsoft-azure&logoColor=white"/>
  <img src="https://img.shields.io/badge/GCP-BigQuery%20%7C%20Dataflow%20%7C%20Pub%2FSub-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white"/>
  <img src="https://img.shields.io/badge/IaC-Terraform-7B42BC?style=for-the-badge&logo=terraform&logoColor=white"/>
  <img src="https://img.shields.io/badge/Transforms-dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white"/>
  <img src="https://img.shields.io/badge/Orchestration-Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white"/>
</p>

---

## Overview

This project demonstrates a **production-grade, multi-cloud data lakehouse** that unifies AWS, Azure, and GCP into a single, coherent data platform. It combines the openness of Delta Lake with the analytical power of BigQuery, the ETL maturity of Azure Data Factory, and the transformation elegance of dbt — all orchestrated by Apache Airflow and deployed via Terraform IaC.

The architecture is designed to mirror real-world enterprise data platforms where data residency, vendor neutrality, and best-of-breed tooling across clouds are non-negotiable requirements.

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     MULTI-CLOUD DATA LAKEHOUSE PLATFORM                     │
└─────────────────────────────────────────────────────────────────────────────┘

   DATA SOURCES                INGESTION LAYER               STORAGE LAYER
 ┌──────────────┐         ┌──────────────────────┐      ┌───────────────────┐
 │  On-Premises │────────►│  Azure Data Factory  │─────►│   ADLS Gen2       │
 │  SQL Server  │         │  (Parameterized ETL) │      │  bronze/silver/   │
 └──────────────┘         └──────────────────────┘      │  gold containers  │
                                                         └─────────┬─────────┘
 ┌──────────────┐         ┌──────────────────────┐                 │
 │  Cloud APIs  │────────►│   AWS Glue + S3       │──────┐         │
 │  & SaaS      │         │  (Delta Lake Bronze)  │      │         │
 └──────────────┘         └──────────────────────┘      │         │
                                                         ▼         ▼
 ┌──────────────┐         ┌──────────────────────┐  ┌───────────────────────┐
 │  Streaming   │────────►│  GCP Pub/Sub +        │  │    TRANSFORMATION     │
 │  Events      │         │  Dataflow Pipeline    │  │                       │
 └──────────────┘         └──────────────────────┘  │  dbt (BigQuery +      │
                                                     │  Snowflake targets)   │
                                                     │                       │
                                                     │  staging layer        │
                                                     │  ↓                    │
                                                     │  marts layer (gold)   │
                                                     └───────────┬───────────┘
                                                                 │
                                                                 ▼
                                                     ┌───────────────────────┐
                                                     │   ANALYTICS LAYER     │
                                                     │                       │
                                                     │  BigQuery             │
                                                     │  • Partitioned tables │
                                                     │  • Clustered tables   │
                                                     │  • BigQuery ML        │
                                                     │                       │
                                                     │  Synapse Analytics    │
                                                     │  • Dedicated SQL Pool │
                                                     │  • Spark Pool         │
                                                     └───────────────────────┘

                    ┌─────────────────────────────────────────────┐
                    │         ORCHESTRATION (Apache Airflow)       │
                    │                                              │
                    │  Ingestion → dbt Bronze→Silver → dbt Gold   │
                    │  → BQ Load → Quality Gate → Reconciliation  │
                    └─────────────────────────────────────────────┘

                    ┌─────────────────────────────────────────────┐
                    │              GOVERNANCE & SECURITY           │
                    │  AWS: IAM least-privilege + Lake Formation   │
                    │  Azure: AAD RBAC + Key Vault + Purview       │
                    │  GCP: IAM service accounts + CMEK            │
                    │  Cross-cloud: Column masking, audit logging  │
                    └─────────────────────────────────────────────┘
```

---

## Data Flow Diagram

```
RAW LANDING (Bronze)
     │
     │  ← AWS: S3 Delta Lake (ACID, time travel, schema enforcement)
     │  ← Azure: ADLS Gen2 Parquet via ADF pipelines
     │  ← GCP: BigQuery streaming via Dataflow/Pub/Sub
     │
     ▼
STANDARDIZED (Silver)                    dbt staging models
     │                                   • Column normalization
     │                                   • Type casting
     │                                   • PII masking
     │                                   • Source deduplication
     │
     ▼
BUSINESS-READY (Gold)                    dbt mart models
     │                                   • Fact tables (incremental)
     │                                   • Dimension tables (SCD)
     │                                   • Aggregates
     │                                   • BigQuery ML scoring
     │
     ▼
CONSUMPTION LAYER
     ├── BigQuery (BI tools, ad-hoc SQL, ML)
     ├── Synapse Analytics (SQL pool, Power BI DirectQuery)
     └── Delta Lake on S3 (Glue Catalog, Athena, EMR)
```

---

## Repository Structure

```
multi-cloud-lakehouse/
│
├── terraform/                        # Infrastructure as Code (Terraform)
│   ├── main.tf                       # Root module — wires AWS, Azure, GCP modules
│   ├── variables.tf                  # Input variable definitions
│   ├── outputs.tf                    # Infrastructure outputs
│   ├── aws/
│   │   └── main.tf                   # S3 Delta Lake, Glue, Lake Formation, IAM, CloudTrail
│   ├── azure/
│   │   └── main.tf                   # ADLS Gen2, ADF, Synapse, Key Vault, Purview, RBAC
│   └── gcp/
│       └── main.tf                   # BigQuery, Dataflow, Pub/Sub, GCS, KMS, IAM SAs
│
├── airflow/
│   └── dags/
│       └── multi_cloud_lakehouse_dag.py   # Master orchestration DAG
│
├── dbt/                              # dbt transformation project
│   ├── dbt_project.yml               # Project config — materialization, tags, layers
│   ├── profiles.yml                  # BigQuery + Snowflake connection profiles
│   └── models/
│       ├── staging/
│       │   ├── sources.yml           # Source definitions with freshness checks
│       │   └── stg_events.sql        # Staging model: normalize + deduplicate events
│       └── marts/
│           └── fct_events.sql        # Gold-layer incremental fact table (BigQuery)
│
├── gcp/
│   ├── dataflow/
│   │   └── pubsub_to_bigquery.py     # Apache Beam streaming pipeline
│   └── bigquery/
│       └── schemas/
│           └── events.json           # BigQuery table schema definition
│
├── aws/
│   └── delta_lake/
│       └── delta_utils.py            # Delta Lake Python utilities (upsert, time travel, vacuum)
│
├── azure/
│   └── adf_pipelines/
│       └── PL_Ingest_OnPrem_to_ADLS.json   # ADF pipeline with watermark + upsert logic
│
├── docker/
│   ├── Dockerfile.airflow            # Custom Airflow image with all provider SDKs
│   └── docker-compose.yml            # Full local dev stack (Airflow + Postgres + Redis + dbt)
│
├── .github/
│   └── workflows/
│       └── ci_cd.yml                 # GitHub Actions CI/CD (lint → test → TF plan → deploy)
│
└── requirements.txt                  # Python dependencies
```

---

## Key Components

### 1. AWS — Delta Lake on S3

**Technology:** `delta-rs` (Rust engine), AWS Glue Data Catalog, AWS Lake Formation, AWS KMS

Delta Lake provides a reliable, ACID-compliant open table format on top of S3 object storage. This eliminates the historical reliability gap between data lakes (cheap, flexible) and data warehouses (reliable, expensive).

**Implemented Patterns:**

| Pattern | Description |
|---|---|
| **ACID Upserts** | `MERGE INTO` semantics via `upsert_silver()` — matched rows updated, unmatched inserted atomically |
| **Schema Enforcement** | Writes rejected if column names or types diverge from the registered schema |
| **Schema Evolution** | Additive column additions via `schema_mode=merge` — backward compatible |
| **Time Travel** | `read_as_of(as_of=datetime(...))` reads any historical snapshot without data duplication |
| **OPTIMIZE + Z-ORDER** | Bin-packing compaction + Z-ORDER on `user_id`, `event_type` reduces query data scan by 60–80% |
| **VACUUM** | Reclaims storage from obsolete file versions after configurable retention window |

**Delta Table Layers:**
```
s3://bucket/raw/         ← Landing zone (schema-on-read, append-only)
s3://bucket/bronze/      ← Schema-enforced, partitioned by _partition_date
s3://bucket/silver/      ← Deduplicated, upserted, normalized
s3://bucket/gold/        ← Aggregated, business-ready, Glue-catalogued
s3://bucket/checkpoints/ ← Spark/Delta streaming checkpoints
```

---

### 2. Azure — ADF + Synapse Analytics

**Technology:** Azure Data Factory, Azure Synapse Analytics, ADLS Gen2, Azure Key Vault, Microsoft Purview

**ADF Pipeline: `PL_Ingest_OnPrem_to_ADLS`**

A fully parameterized, reusable ingestion template designed for any source table:

```
LookupWatermark → CopySourceToAdls → CopyAdlsToSynapse → UpdateWatermark
```

- **Incremental watermark pattern**: reads from a control table, only processes new/changed rows since the last successful load
- **Dynamic range partitioning**: splits large table reads across parallel threads for performance
- **Parquet + Snappy compression**: optimal format for columnar analytics workloads
- **Synapse COPY command**: fastest available bulk-load path into Synapse dedicated SQL pool

**Synapse Configuration:**

| Component | Purpose |
|---|---|
| Dedicated SQL Pool (DW) | Columnar MPP warehouse for BI and reporting queries |
| Apache Spark Pool | PySpark processing for complex transformations |
| ADLS Gen2 integration | Direct lakehouse access without data movement |
| Auto-pause (60 min) | Cost optimization — pool pauses when idle |

---

### 3. GCP — BigQuery + Dataflow

**Technology:** BigQuery, Apache Beam / Dataflow, Pub/Sub, BigQuery ML, Cloud KMS

**Dataflow Streaming Pipeline (`pubsub_to_bigquery.py`)**

Production Apache Beam pipeline with exactly-once semantics:

```
Pub/Sub Subscription
    └──► Fixed 60s Windows
              └──► ParseAndValidate (route valid/dead-letter)
                        └──► MaskPII (hash IP, strip PII fields)
                                  └──► FormatForBigQuery
                                              └──► BigQuery Storage Write API
                                              └──► Dead Letter Table
```

**BigQuery Table Design:**

```sql
-- Partitioned by event_date (DAY granularity)
-- Clustered on: event_type, country_code, device_type
-- Partition filter required — prevents full-table scans
-- CMEK encryption with Cloud KMS
```

**BigQuery ML Integration:**

The Airflow DAG includes a `BigQueryInsertJobOperator` step that calls `ML.PREDICT()` on a trained `churn_classifier` model, scoring the daily event cohort entirely within BigQuery — no data movement to an external ML platform required.

---

### 4. dbt — Cross-Platform Transformation Layer

**Technology:** dbt-bigquery, dbt-snowflake, dbt-utils

**Model Architecture:**

```
sources (BigQuery, Snowflake, external)
    └──► staging/          ← Views — normalize, cast, mask, deduplicate
              └──► intermediate/  ← Ephemeral — reusable CTEs
                        └──► marts/       ← Tables — business fact + dim tables
```

**`fct_events` (Gold Layer):**
- Incremental materialization with `MERGE` strategy
- Daily partitioning + clustering for query cost optimization
- Session-level window functions (event sequence, time-between-events)
- Cross-cloud join: AWS events + Azure transactions + GCP streaming
- BigQuery labels for cost attribution per domain

**Data Quality:**
- `not_null`, `unique`, `accepted_values` schema tests on every source
- Source freshness checks (warn >12h, error >24h)
- Custom `cross_cloud_reconciliation` test comparing row counts across AWS and GCP

---

### 5. Apache Airflow Orchestration

**Technology:** Airflow 2.8, CeleryExecutor, Postgres metadata DB, Redis broker

**DAG: `multi_cloud_lakehouse_orchestration`**

Daily at 03:00 UTC with a 4-phase structure:

```
Phase 1 — INGESTION (parallel)
  ├── S3KeySensor → GlueJobOperator (AWS)
  ├── SimpleHttpOperator → ADF REST API (Azure)
  └── PubSubPullSensor → DataflowStartFlexTemplateOperator (GCP)
         │
Phase 2 — DBT TRANSFORMS (sequential)
  ├── dbt run staging.*
  ├── dbt run marts.*
  └── dbt test
         │
Phase 3 — BIGQUERY LOAD
  ├── BigQueryInsertJobOperator (fact_events partition)
  └── BigQueryInsertJobOperator (ML.PREDICT scoring)
         │
Phase 4 — QUALITY GATE
  ├── BigQueryValueCheckOperator (row count ≥ 1,000)
  ├── BigQueryValueCheckOperator (NULL user_id = 0)
  └── cross_cloud_reconciliation (AWS vs GCP count diff < 1%)
```

---

### 6. Data Governance & Security

**Principle applied across all three clouds: least-privilege IAM, encryption everywhere, centralized audit logging.**

| Concern | AWS | Azure | GCP |
|---|---|---|---|
| **Identity** | IAM Roles (no long-lived keys) | AAD RBAC + Managed Identity | Service Accounts + Workload Identity |
| **Key Management** | AWS KMS (90-day rotation) | Azure Key Vault (Purge Protection) | Cloud KMS Key Ring |
| **Encryption at rest** | SSE-KMS on all S3 buckets | Storage Account CMK | CMEK on BigQuery + GCS |
| **Encryption in transit** | TLS 1.2+ enforced | HTTPS only, Private Endpoints | VPC Service Controls |
| **PII Masking** | Column-level via Glue transforms | Dynamic Data Masking in Synapse | SHA-256 hash in Dataflow + BigQuery column policy |
| **Audit Logging** | CloudTrail (multi-region, log validation) | Azure Monitor + Log Analytics | Cloud Logging → BigQuery sink |
| **Access Control** | Lake Formation fine-grained | Synapse RBAC + Row-Level Security | BigQuery IAM + Column-level Security |
| **Governance** | AWS Macie (PII discovery) | Microsoft Purview (prod only) | Data Catalog tags |

---

### 7. CI/CD Pipeline (GitHub Actions)

```
on: push to main/develop, PR to main
         │
         ▼
   lint-and-test
   ├── Ruff linting
   ├── mypy type checking
   └── pytest (unit tests + coverage)
         │
    ┌────┴────┐
    ▼         ▼
dbt-validate  terraform-validate
├── dbt deps  ├── terraform init
├── dbt compile  ├── terraform validate
└── dbt parse    ├── terraform fmt
                 └── terraform plan
                      (PR comment with diff)
         │
         ▼
   docker-build
   └── Build + push to GHCR (multi-platform)
         │
   ┌─────┴──────┐
   ▼             ▼
deploy-staging  deploy-production
(develop branch) (main branch)
├── DAG sync     ├── Terraform apply
├── dbt run      ├── DAG sync
└── dbt test     └── Slack notification
```

**Security features in CI/CD:**
- OIDC (keyless) authentication to all three cloud providers — no static credentials stored in GitHub Secrets
- Separate IAM roles per environment (dev/staging/prod) with scoped permissions
- Terraform plan output posted as PR comment for peer review before apply
- `max_active_runs: 1` on production DAG prevents concurrent pipeline collisions

---

## Local Development Setup

### Prerequisites

- Docker Desktop 4.x+
- Terraform 1.6+
- Python 3.11+
- GCP service account JSON keyfile
- AWS credentials (IAM user or role)

### 1. Clone and configure environment

```bash
git clone https://github.com/YOUR_USERNAME/multi-cloud-lakehouse.git
cd multi-cloud-lakehouse

# Copy env template and populate values
cp .env.example .env
# Edit .env with your cloud credentials and project IDs
```

### 2. Start the local stack

```bash
# Build and start all services (Airflow, Postgres, Redis, dbt)
docker compose -f docker/docker-compose.yml up -d

# Wait for init to complete
docker compose -f docker/docker-compose.yml logs -f airflow-init
```

Access the Airflow UI at **http://localhost:8080** (admin / admin)

### 3. Run dbt locally

```bash
# Open dbt container
docker compose -f docker/docker-compose.yml run --rm dbt bash

# Inside container
dbt deps
dbt compile --target dev
dbt run --select staging.*
dbt test
dbt docs generate && dbt docs serve
```

### 4. Provision cloud infrastructure

```bash
cd terraform/

# Initialize with remote state
terraform init

# Review plan
terraform plan -var="environment=dev" -var-file="dev.tfvars"

# Apply
terraform apply -var="environment=dev" -var-file="dev.tfvars"
```

---

## Environment Variables Reference

| Variable | Description | Required |
|---|---|---|
| `GCP_PROJECT_ID` | GCP project ID | ✅ |
| `DELTA_LAKE_BUCKET` | S3 bucket name for Delta Lake | ✅ |
| `AZURE_SUBSCRIPTION_ID` | Azure subscription ID | ✅ |
| `AZURE_TENANT_ID` | Azure AD tenant ID | ✅ |
| `AZURE_CLIENT_ID` | Service principal client ID | ✅ |
| `AZURE_CLIENT_SECRET` | Service principal client secret | ✅ |
| `AIRFLOW_FERNET_KEY` | Airflow connection encryption key | ✅ |
| `AIRFLOW_SECRET_KEY` | Airflow webserver secret key | ✅ |
| `SYNAPSE_ADMIN_PASSWORD` | Synapse SQL admin password | ✅ |
| `AWS_REGION` | AWS region (default: us-east-1) | ❌ |
| `GCP_REGION` | GCP region (default: us-central1) | ❌ |
| `ENVIRONMENT` | dev / staging / prod | ❌ |

---

## Technology Stack

| Layer | Technology | Version | Purpose |
|---|---|---|---|
| Orchestration | Apache Airflow | 2.8.x | DAG scheduling, cross-cloud coordination |
| Transformation | dbt | 1.7.x | SQL transformation, testing, documentation |
| AWS Storage | Delta Lake on S3 | 3.1.x | ACID open table format |
| AWS Catalog | AWS Glue + Lake Formation | — | Metadata catalog, fine-grained access |
| Azure ETL | Azure Data Factory | v2 | Parameterized ingestion pipelines |
| Azure Warehouse | Azure Synapse Analytics | — | MPP SQL + Spark pool |
| Azure Storage | ADLS Gen2 | — | Hierarchical namespace blob storage |
| GCP Analytics | BigQuery | — | Serverless analytics + ML in-warehouse |
| GCP Streaming | Apache Beam / Dataflow | 2.54.x | Exactly-once Pub/Sub → BQ pipeline |
| IaC | Terraform | 1.6.x | Multi-cloud resource provisioning |
| Containers | Docker + Compose | 24.x | Local dev, CI/CD build artifacts |
| CI/CD | GitHub Actions | — | Lint, test, plan, deploy |
| Secrets | AWS KMS / Azure Key Vault / Cloud KMS | — | Encryption key management |

---

## Skills Demonstrated

- ✅ **Delta Lake** — ACID guarantees, schema enforcement, time travel, Z-ORDER optimization on S3
- ✅ **Azure Data Factory** — Parameterized pipeline templates, linked services, watermark incremental loads
- ✅ **Azure Synapse Analytics** — Dedicated SQL Pool, Spark Pool, ADLS Gen2 integration
- ✅ **GCP BigQuery** — Partitioned + clustered tables, BigQuery ML in-warehouse scoring
- ✅ **GCP Dataflow** — Apache Beam streaming pipeline with PII masking and dead-letter routing
- ✅ **dbt** — Multi-target (BigQuery + Snowflake), staging/mart layers, incremental models, schema tests
- ✅ **Apache Airflow** — TaskGroups, sensors, cross-cloud operators, Celery worker scaling
- ✅ **Terraform** — Multi-cloud IaC with module abstraction, remote state, OIDC auth
- ✅ **Data Governance** — IAM least-privilege, RBAC, CMEK, column-level masking, PII hashing, audit logs
- ✅ **CI/CD** — GitHub Actions with keyless OIDC auth, parallel jobs, Terraform plan PR comments

---

## License

MIT License — see [LICENSE](LICENSE) for details.

---

<p align="center">
  Built with ☁️ across AWS · Azure · GCP
</p>
