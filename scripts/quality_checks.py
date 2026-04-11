import pandas as pd
from db import get_engine


CHECKS = [
    ("raw.policies_raw",                  "raw_policies_count"),
    ("raw.claims_raw",                    "raw_claims_count"),
    ("raw.customers_raw",                 "raw_customers_count"),
    ("clean.policies",                    "clean_policies_count"),
    ("clean.claims",                      "clean_claims_count"),
    ("clean.customers",                   "clean_customers_count"),
    ("mart.policy_summary",               "mart_policy_summary_count"),
    ("mart.loss_ratio_by_policy_type",    "mart_loss_ratio_count"),
    ("mart.channel_performance",          "mart_channel_performance_count"),
    ("mart.customer_claim_summary",       "mart_customer_claim_summary_count"),
    ("mart.claim_status_summary",         "mart_claim_status_summary_count"),
]


def run():
    engine = get_engine()
    failed = []

    print("\n=== DATA QUALITY CHECKS ===")

    for table, check_name in CHECKS:
        query = f"SELECT COUNT(*) AS cnt FROM {table}"
        result = pd.read_sql(query, engine)
        count = int(result["cnt"].iloc[0])
        status = "PASS" if count > 0 else "FAIL"
        print(f"  [{status}] {check_name}: {count} rows")

        if count == 0:
            failed.append(check_name)

    print("=== END CHECKS ===\n")

    if failed:
        raise ValueError(
            f"Quality check FAILED — the following tables are empty: {failed}"
        )

    print("[OK] All quality checks passed.")


if __name__ == "__main__":
    run()
