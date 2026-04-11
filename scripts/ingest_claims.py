import pandas as pd
from db import get_engine


def run():
    df = pd.read_csv("/opt/airflow/data/raw/claims.csv")
    engine = get_engine()

    df.to_sql(
        name="claims_raw",
        con=engine,
        schema="raw",
        if_exists="replace",
        index=False
    )

    print(f"[OK] Loaded {len(df)} rows into raw.claims_raw")


if __name__ == "__main__":
    run()
