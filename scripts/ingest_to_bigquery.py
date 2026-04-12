import os
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery

PROJECT_ID  = "insurance-data-platform-493115"
DATASET     = "raw"
KEYFILE     = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS",
    "d:/DE/Project/insurance_data_platform/insurance-data-platform-493115-30ab031da49d.json"
)

credentials = service_account.Credentials.from_service_account_file(KEYFILE)
client = bigquery.Client(project=PROJECT_ID, credentials=credentials)


def load_csv_to_bq(csv_path: str, table_name: str):
    df = pd.read_csv(csv_path)
    table_id = f"{PROJECT_ID}.{DATASET}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # replace existing data
        autodetect=True,                     # auto-detect schema from DataFrame
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # wait for job to complete

    print(f"[OK] Loaded {len(df):,} rows -> {table_id}")


if __name__ == "__main__":
    print("=== Ingesting data to BigQuery ===\n")

    load_csv_to_bq("data/raw/customers.csv", "customers_raw")
    load_csv_to_bq("data/raw/policies.csv",  "policies_raw")
    load_csv_to_bq("data/raw/claims.csv",    "claims_raw")

    print("\n=== Done! ===")
