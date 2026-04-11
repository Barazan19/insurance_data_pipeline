SELECT 'raw_policies_count' AS check_name, COUNT(*) AS check_value
FROM raw.policies_raw

UNION ALL

SELECT 'raw_claims_count' AS check_name, COUNT(*) AS check_value
FROM raw.claims_raw

UNION ALL

SELECT 'raw_customers_count' AS check_name, COUNT(*) AS check_value
FROM raw.customers_raw

UNION ALL

SELECT 'clean_policies_count' AS check_name, COUNT(*) AS check_value
FROM clean.policies

UNION ALL

SELECT 'clean_claims_count' AS check_name, COUNT(*) AS check_value
FROM clean.claims

UNION ALL

SELECT 'clean_customers_count' AS check_name, COUNT(*) AS check_value
FROM clean.customers

UNION ALL

SELECT 'mart_policy_summary_count' AS check_name, COUNT(*) AS check_value
FROM mart.policy_summary;