import pandas as pd
from db import get_engine


def run():
    df = pd.read_csv("/opt/airflow/data/raw/customers.csv")
    engine = get_engine()

    df.to_sql(
        name="customers_raw",
        con=engine,
        schema="raw",
        if_exists="replace",
        index=False
    )

    print(f"[OK] Loaded {len(df)} rows into raw.customers_raw")


if __name__ == "__main__":
    run()
