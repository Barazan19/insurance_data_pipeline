# Insurance Data Platform

A mini data warehouse project for insurance data built with a modern data engineering stack. Implements the **Medallion Architecture** (Bronze → Silver → Gold) with full pipeline orchestration via Apache Airflow.

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.9.3 |
| Database | PostgreSQL 15 |
| Data Processing | Python, Pandas, SQLAlchemy |
| Visualization | Metabase |
| Infrastructure | Docker, Docker Compose |

## Architecture

```
CSV Files (data/raw/)
        ↓
BRONZE / RAW Layer      → raw.customers_raw, raw.policies_raw, raw.claims_raw
        ↓  (standardize, normalize)
SILVER / CLEAN Layer    → clean.customers, clean.policies, clean.claims
        ↓  (aggregate, calculate business metrics)
GOLD / MART Layer       → mart.policy_summary, mart.channel_performance,
                          mart.customer_claim_summary, mart.claim_status_summary,
                          mart.loss_ratio_by_policy_type
        ↓
Data Quality Checks (11 tables validated)
        ↓
Metabase (BI Dashboard)
```

## Project Structure

```
insurance_data_platform/
├── data/raw/
│   ├── customers.csv
│   ├── policies.csv
│   └── claims.csv
├── scripts/
│   ├── db.py                    # PostgreSQL connection factory
│   ├── ingest_customers.py      # CSV → raw.customers_raw
│   ├── ingest_policies.py       # CSV → raw.policies_raw
│   ├── ingest_claims.py         # CSV → raw.claims_raw
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
├── dags/
│   └── insurance_pipeline_dag.py   # Airflow DAG (8 tasks)
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Pipeline DAG

The Airflow DAG runs 8 sequential tasks:

```
init_schema → ingest_customers → ingest_policies → ingest_claims
    → build_clean_layer → build_mart_layer → build_business_marts
    → run_quality_checks
```

## Business Metrics Computed

- **Loss Ratio** = Total Claims / Total Premium (per policy type, per channel, per customer)
- **Channel Performance** — Agency vs Bancassurance vs Digital
- **Customer Claim Summary** — claim frequency and severity per customer
- **Claim Status Distribution** — Approved / Rejected / Pending breakdown

## How to Run

**Prerequisites:** Docker & Docker Compose installed.

**1. Start all services**
```bash
docker compose up -d
```

**2. Access Airflow UI**
```
URL:      http://localhost:8080
Username: admin
Password: admin
```

**3. Trigger the pipeline**

Go to DAGs → `insurance_data_pipeline` → Trigger DAG

**4. Access Metabase**
```
URL: http://localhost:3000
```
Connect to PostgreSQL host `insurance_postgres`, database `insurance_db`.

---

**Run manually (without Airflow)**
```bash
python scripts/run_pipeline.py
```

## Data Quality

Quality checks validate all 11 tables across all layers after each pipeline run:

```
[PASS] raw_policies_count: 4 rows
[PASS] raw_claims_count: 4 rows
[PASS] raw_customers_count: 3 rows
[PASS] clean_policies_count: 4 rows
[PASS] clean_claims_count: 4 rows
[PASS] clean_customers_count: 3 rows
[PASS] mart_policy_summary_count: 3 rows
[PASS] mart_channel_performance_count: 3 rows
[PASS] mart_customer_claim_summary_count: 3 rows
[PASS] mart_claim_status_summary_count: 3 rows
[PASS] mart_loss_ratio_by_policy_type_count: 3 rows
```

If any table is empty, the pipeline raises an exception and the DAG task is marked as failed.

## Roadmap

- [ ] Migrate transformations to dbt
- [ ] Scale to cloud (BigQuery / AWS Redshift)
- [ ] Generate realistic synthetic data (10k+ rows)
- [ ] Add streaming ingestion with Apache Kafka
- [ ] CI/CD with GitHub Actions
