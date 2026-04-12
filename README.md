# Insurance Data Platform

An end-to-end insurance data pipeline built with a modern data engineering stack. Implements the **Medallion Architecture** (Bronze → Silver → Gold) with cloud data warehouse, dbt transformations, Airflow orchestration, and CI/CD.

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.9.3 |
| Transformation | dbt (data build tool) |
| Cloud Data Warehouse | Google BigQuery |
| Local Database | PostgreSQL 15 (Docker) |
| Data Processing | Python, Pandas |
| Visualization | Metabase |
| Infrastructure | Docker, Docker Compose |
| CI/CD | GitHub Actions |

## Architecture

```
CSV Files (data/raw/)
1,000 customers / 3,000 policies / 2,576 claims
        ↓  [Python + Pandas ingestion]
BRONZE / RAW Layer
  BigQuery: raw.customers_raw, raw.policies_raw, raw.claims_raw
        ↓  [dbt silver models — standardize, normalize]
SILVER / CLEAN Layer
  BigQuery: clean.clean_customers, clean.clean_policies, clean.clean_claims
        ↓  [dbt gold models — aggregate, calculate business metrics]
GOLD / MART Layer
  BigQuery: mart.mart_policy_summary, mart.mart_channel_performance,
            mart.mart_customer_claim_summary, mart.mart_claim_status_summary,
            mart.mart_loss_ratio_by_policy_type
        ↓  [dbt test — 11 automated data quality tests]
CI/CD (GitHub Actions) — runs dbt run + dbt test on every push
```

## Project Structure

```
insurance_data_platform/
├── .github/workflows/
│   └── dbt_ci.yml               # GitHub Actions CI/CD
├── data/raw/
│   ├── customers.csv            # 1,000 synthetic customers
│   ├── policies.csv             # 3,000 synthetic policies
│   └── claims.csv               # 2,576 synthetic claims
├── scripts/
│   ├── db.py                    # PostgreSQL connection factory
│   ├── generate_synthetic_data.py  # Synthetic data generator
│   ├── ingest_customers.py      # CSV → raw.customers_raw (PostgreSQL)
│   ├── ingest_policies.py       # CSV → raw.policies_raw (PostgreSQL)
│   ├── ingest_claims.py         # CSV → raw.claims_raw (PostgreSQL)
│   ├── ingest_to_bigquery.py    # CSV → BigQuery raw dataset
│   ├── run_sql.py               # SQL file executor
│   ├── quality_checks.py        # Data validation (11 tables)
│   └── run_pipeline.py          # Manual pipeline runner
├── sql/
│   ├── 01_create_schema.sql     # Create raw/clean/mart schemas
│   ├── 02_create_raw_tables.sql # Raw table DDL (reference)
│   ├── 03_create_clean_tables.sql  # Bronze → Silver transformations
│   ├── 04_create_mart_tables.sql   # Silver → Gold (policy_summary)
│   ├── 05_quality_checks.sql    # Quality check queries (reference)
│   └── 06_create_business_marts.sql # Silver → Gold (business marts)
├── insurance_dbt/
│   ├── models/
│   │   ├── silver/              # dbt Silver layer models
│   │   │   ├── clean_customers.sql
│   │   │   ├── clean_policies.sql
│   │   │   ├── clean_claims.sql
│   │   │   ├── sources.yml      # BigQuery source definitions
│   │   │   └── schema.yml       # dbt tests (unique, not_null, accepted_values)
│   │   └── gold/                # dbt Gold layer models
│   │       ├── mart_policy_summary.sql
│   │       ├── mart_channel_performance.sql
│   │       ├── mart_customer_claim_summary.sql
│   │       ├── mart_claim_status_summary.sql
│   │       └── mart_loss_ratio_by_policy_type.sql
│   ├── macros/
│   │   └── generate_schema_name.sql
│   └── dbt_project.yml
├── dags/
│   └── insurance_pipeline_dag.py   # Airflow DAG (7 tasks)
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Airflow Pipeline DAG

7 sequential tasks:

```
init_schema → ingest_customers → ingest_policies → ingest_claims
    → dbt_run → dbt_test → run_quality_checks
```

## dbt Models

**Silver layer** — standardize raw data:
- `TRIM()` whitespace, `INITCAP()` text casing
- Normalize status values via `CASE WHEN` (e.g. `M` → `Male`)

**Gold layer** — aggregate and calculate business metrics:
- Loss ratio = `SUM(claims) / SUM(premium)` per dimension
- `COALESCE(..., 0)` for policies with no claims
- `NULLIF(..., 0)` to prevent division by zero

**dbt tests (11 total):**
- `unique` + `not_null` on all primary keys
- `accepted_values` on status columns (gender, policy_status, claim_status)

## Business Metrics Computed

- **Loss Ratio** = Total Claims / Total Premium
- **Channel Performance** — Agency vs Bancassurance vs Digital vs Broker vs Direct
- **Customer Claim Summary** — claim frequency and severity per customer
- **Claim Status Distribution** — Approved / Rejected / Pending breakdown
- **Policy Type Profitability** — which products are loss-making

## Synthetic Data

Generated via `scripts/generate_synthetic_data.py` (no external dependencies):

| Table | Rows | Details |
|---|---|---|
| customers | 1,000 | Indonesian names, 20 cities |
| policies | 3,000 | 5 policy types, 5 channels, 2022-2024 |
| claims | ~2,576 | 60% claim rate, weighted status distribution |

## How to Run

### Local (Docker + PostgreSQL)

**Prerequisites:** Docker & Docker Compose installed.

```bash
# Start all services
docker compose up -d

# Access Airflow UI
# URL: http://localhost:8080 | Username: admin | Password: admin

# Trigger DAG: insurance_data_pipeline
```

### Cloud (BigQuery)

```bash
# Set credentials
export GOOGLE_APPLICATION_CREDENTIALS=path/to/credentials.json

# Ingest raw data to BigQuery
python scripts/ingest_to_bigquery.py

# Run dbt transformations
cd insurance_dbt
dbt run --profiles-dir ~/.dbt
dbt test --profiles-dir ~/.dbt
```

## CI/CD

GitHub Actions workflow (`.github/workflows/dbt_ci.yml`) triggers on every push to `main`:

1. Spin up PostgreSQL service
2. Install dependencies
3. Create schemas + ingest CSV data
4. `dbt run` — build all models
5. `dbt test` — validate data quality

## Roadmap

- [x] Medallion Architecture (Bronze → Silver → Gold)
- [x] Synthetic data generation (1k customers, 3k policies, 2.5k claims)
- [x] dbt transformations with automated tests
- [x] Apache Airflow orchestration
- [x] Google BigQuery cloud data warehouse
- [x] GitHub Actions CI/CD
- [ ] Streaming ingestion with Apache Kafka
