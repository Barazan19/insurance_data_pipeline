from ingest_customers import run as run_ingest_customers
from ingest_policies import run as run_ingest_policies
from ingest_claims import run as run_ingest_claims
from run_sql import execute_sql_file
from quality_checks import run as run_quality_checks


def main():
    print("=== START INSURANCE DATA PLATFORM PIPELINE ===")

    print("\n[STEP 1] Ingest raw CSV files")
    run_ingest_customers()
    run_ingest_policies()
    run_ingest_claims()

    print("\n[STEP 2] Build clean layer")
    execute_sql_file("sql/03_create_clean_tables.sql")

    print("\n[STEP 3] Build mart layer")
    execute_sql_file("sql/04_create_mart_tables.sql")

    print("\n[STEP 4] Run data quality checks")
    run_quality_checks()

    print("=== PIPELINE COMPLETED SUCCESSFULLY ===")


if __name__ == "__main__":
    main()