import sys
sys.path.insert(0, "/opt/airflow/scripts")

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from ingest_customers import run as ingest_customers
from ingest_policies import run as ingest_policies
from ingest_claims import run as ingest_claims
from run_sql import execute_sql_file
from quality_checks import run as run_quality_checks


def init_schema():
    execute_sql_file("/opt/airflow/sql/01_create_schema.sql")


def build_clean_layer():
    execute_sql_file("/opt/airflow/sql/03_create_clean_tables.sql")


def build_mart_layer():
    execute_sql_file("/opt/airflow/sql/04_create_mart_tables.sql")


def build_business_marts():
    execute_sql_file("/opt/airflow/sql/06_create_business_marts.sql")


default_args = {
    "owner": "data_engineer",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="insurance_data_pipeline",
    default_args=default_args,
    schedule=None,
    catchup=False,
    description="Insurance Data Platform Pipeline",
) as dag:

    task_init_schema = PythonOperator(
        task_id="init_schema",
        python_callable=init_schema,
    )

    task_ingest_customers = PythonOperator(
        task_id="ingest_customers",
        python_callable=ingest_customers,
    )

    task_ingest_policies = PythonOperator(
        task_id="ingest_policies",
        python_callable=ingest_policies,
    )

    task_ingest_claims = PythonOperator(
        task_id="ingest_claims",
        python_callable=ingest_claims,
    )

    task_build_clean_layer = PythonOperator(
        task_id="build_clean_layer",
        python_callable=build_clean_layer,
    )

    task_build_mart_layer = PythonOperator(
        task_id="build_mart_layer",
        python_callable=build_mart_layer,
    )

    task_build_business_marts = PythonOperator(
        task_id="build_business_marts",
        python_callable=build_business_marts,
    )

    task_run_quality_checks = PythonOperator(
        task_id="run_quality_checks",
        python_callable=run_quality_checks,
    )

    (
        task_init_schema
        >> task_ingest_customers
        >> task_ingest_policies
        >> task_ingest_claims
        >> task_build_clean_layer
        >> task_build_mart_layer
        >> task_build_business_marts
        >> task_run_quality_checks
    )
