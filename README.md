# Multi-Cloud Data Lakehouse

A data lakehouse platform built across AWS, Azure, and GCP. The goal was to combine the best tools from each cloud into one unified pipeline: Delta Lake on S3 for reliable open-format storage, Azure Data Factory and Synapse for enterprise ETL, BigQuery and Dataflow for analytics and streaming, dbt for transformations, and Airflow to tie everything together.

---

## Why This Project

Most real companies don't live on a single cloud. Data engineers need to move data between systems that don't naturally talk to each other, apply consistent governance, and keep pipelines running reliably across all of it. This project simulates that environment and implements production patterns for each cloud.

---

## Architecture

```
Data Sources
    |
    +-- On-Prem SQL / ERP
    +-- SaaS APIs
    +-- Streaming Events (mobile, web, IoT)
    |
    v
Ingestion Layer
    |
    +-- Azure Data Factory       (on-prem and cloud batch ingestion)
    +-- AWS Glue                 (Delta table registration and crawling)
    +-- GCP Dataflow + Pub/Sub   (real-time streaming into BigQuery)
    |
    v
Storage Layer
    |
    +-- AWS S3 (Delta Lake)      raw / bronze / silver / gold / checkpoints
    +-- Azure ADLS Gen2          bronze / silver / gold containers
    +-- GCP Cloud Storage        raw landing and Dataflow temp buckets
    |
    v
Transformation Layer
    |
    +-- dbt staging models       normalize, cast, deduplicate, mask PII
    +-- dbt mart models          fact tables, dimensions, aggregations
    +-- BigQuery ML              churn model scoring inside the warehouse
    |
    v
Analytics and Serving
    |
    +-- GCP BigQuery             partitioned + clustered tables, ML scoring
    +-- Azure Synapse Analytics  dedicated SQL pool, Spark pool, Power BI
    +-- AWS Athena / Glue        query Delta tables via Glue catalog
    |
    v
Orchestration
    |
    +-- Apache Airflow           daily DAG wiring all phases together
```

---

## Data Flow (Medallion Architecture)

```
BRONZE  raw records, append only, schema enforced by Delta Lake
   |
   |  dbt staging
   |    - column normalization and type casting
   |    - PII hashing (SHA-256)
   |    - deduplication on event_id
   |
SILVER  clean, deduplicated, upserted via Delta MERGE
   |
   |  dbt marts
   |    - fact table joins across all three clouds
   |    - session-level window functions
   |    - BigQuery ML prediction join
   |
GOLD    business-ready, partitioned, clustered, ready for dashboards
```

---

## Airflow DAG Phases

```
START
  |
  +-- Phase 1: Ingestion (runs in parallel)
  |     |
  |     +-- AWS:   S3KeySensor -> GlueJobOperator
  |     +-- Azure: HTTP trigger to ADF pipeline
  |     +-- GCP:   PubSubPullSensor -> DataflowStartFlexTemplateOperator
  |
  +-- Phase 2: dbt Transforms (sequential)
  |     |
  |     +-- dbt run staging
  |     +-- dbt run marts
  |     +-- dbt test
  |
  +-- Phase 3: BigQuery Load
  |     |
  |     +-- Load fact_events partition
  |     +-- Run ML.PREDICT churn scoring
  |
  +-- Phase 4: Quality Gate
        |
        +-- Row count check (must be >= 1,000)
        +-- NULL user_id check (must be 0)
        +-- Cross-cloud reconciliation (AWS vs GCP count diff < 1%)
```

---

## Repo Structure

```
multi-cloud-data-lakehouse/
|
+-- terraform/
|     +-- main.tf                    root module wiring all three cloud modules
|     +-- variables.tf
|     +-- outputs.tf
|     +-- aws/
|     |     +-- main.tf              S3, Glue, Lake Formation, KMS, CloudTrail
|     |     +-- variables.tf
|     +-- azure/
|     |     +-- main.tf              ADLS Gen2, ADF, Synapse, Key Vault, Purview
|     |     +-- variables.tf
|     +-- gcp/
|           +-- main.tf              BigQuery, Dataflow, Pub/Sub, GCS, KMS, IAM
|           +-- variables.tf
|
+-- airflow/
|     +-- dags/
|           +-- multi_cloud_lakehouse_dag.py
|
+-- dbt/
|     +-- dbt_project.yml
|     +-- profiles.yml               BigQuery and Snowflake targets
|     +-- models/
|           +-- staging/
|           |     +-- sources.yml
|           |     +-- stg_events.sql
|           +-- marts/
|                 +-- fct_events.sql
|
+-- aws/
|     +-- delta_lake/
|           +-- delta_utils.py       upserts, time travel, optimize, vacuum
|
+-- azure/
|     +-- adf_pipelines/
|           +-- PL_Ingest_OnPrem_to_ADLS.json
|
+-- gcp/
|     +-- dataflow/
|     |     +-- pubsub_to_bigquery.py
|     +-- bigquery/
|           +-- schemas/
|                 +-- events.json
|
+-- docker/
|     +-- Dockerfile.airflow
|     +-- docker-compose.yml
|
+-- .github/
|     +-- workflows/
|           +-- ci_cd.yml
|
+-- requirements.txt
+-- .gitignore
```

---

## Key Technical Decisions

**Delta Lake on S3 instead of a native AWS format**
Delta gives ACID guarantees, time travel, and schema enforcement on plain S3. Any engine (Spark, Athena, Trino, Flink) can read it without vendor lock-in. The Glue crawler registers the gold layer tables automatically every 6 hours.

**ADF watermark pattern for incremental loads**
Rather than full table refreshes, every ADF pipeline reads the last successful load timestamp from a control table, queries only new or changed rows, and writes that timestamp back on success. This keeps load times fast and idempotent.

**Dataflow with Storage Write API for exactly-once delivery**
The Pub/Sub to BigQuery pipeline uses Apache Beam's Storage Write API mode which gives exactly-once semantics at scale. PII fields are hashed or dropped before any data lands in BigQuery so sensitive fields never touch the warehouse.

**dbt incremental models with a 3-day lookback**
Late-arriving events are common in streaming pipelines. The fact table uses a MERGE strategy with a 3-day lookback window so late data lands in the right partition without requiring a full refresh.

---

## Security Across All Three Clouds

All three clouds use the same principles: no static credentials, encrypt everything with customer-managed keys, log all access, and apply least-privilege access at the column level where possible.

AWS uses IAM roles with Lake Formation for column-level grants and CloudTrail for audit logging. Azure uses Managed Identity, Key Vault for all secrets, and Azure Monitor for logs. GCP uses Workload Identity Federation in CI/CD (no service account keys on disk), CMEK on all BigQuery datasets and GCS buckets, and routes all audit logs into a BigQuery sink for analysis.

---

## CI/CD

GitHub Actions runs on every push and pull request. All three cloud providers authenticate via OIDC so there are no static credentials stored in GitHub Secrets.

```
push / PR
  |
  +-- lint and test      Ruff, mypy, pytest with coverage
  |
  +-- dbt validate       dbt compile and parse (no DB connection needed)
  |
  +-- terraform plan     init, validate, fmt check, plan
  |                      plan output posted as a PR comment
  |
  +-- docker build       custom Airflow image pushed to GHCR
  |
  +-- deploy staging     on develop branch push
  |
  +-- deploy production  on main branch push with Slack notification
```

---

## Running Locally

You need Docker Desktop, Terraform 1.6+, and Python 3.11+.

```bash
git clone https://github.com/YOUR_USERNAME/multi-cloud-data-lakehouse.git
cd multi-cloud-data-lakehouse

# start Airflow + Postgres + Redis + dbt
docker compose -f docker/docker-compose.yml up -d

# Airflow UI at http://localhost:8080  (admin / admin)
```

To run dbt:
```bash
docker compose -f docker/docker-compose.yml run --rm dbt bash
dbt deps
dbt run --select staging.*
dbt test
```

To provision infrastructure:
```bash
cd terraform
terraform init
terraform plan -var="environment=dev" -var-file="dev.tfvars"
terraform apply -var="environment=dev" -var-file="dev.tfvars"
```

---

## Stack

| Layer | Tools |
|---|---|
| Orchestration | Apache Airflow 2.8 |
| Transformation | dbt 1.7 (BigQuery + Snowflake) |
| AWS | S3, Delta Lake, Glue, Lake Formation, KMS, CloudTrail |
| Azure | ADLS Gen2, Data Factory, Synapse Analytics, Key Vault, Purview |
| GCP | BigQuery, Dataflow, Pub/Sub, Cloud KMS, Cloud Logging |
| IaC | Terraform 1.6 |
| Containers | Docker, Docker Compose |
| CI/CD | GitHub Actions with OIDC auth |
| Language | Python 3.11 |

---

## License

MIT
