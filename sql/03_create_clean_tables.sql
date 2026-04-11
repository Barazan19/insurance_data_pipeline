DROP TABLE IF EXISTS clean.policies;
CREATE TABLE clean.policies AS
SELECT
    TRIM(policy_id) AS policy_id,
    TRIM(customer_id) AS customer_id,
    INITCAP(TRIM(policy_type)) AS policy_type,
    premium_amount,
    start_date,
    end_date,
    CASE
        WHEN UPPER(TRIM(status)) = 'ACTIVE' THEN 'Active'
        WHEN UPPER(TRIM(status)) = 'LAPSED' THEN 'Lapsed'
        ELSE 'Unknown'
    END AS policy_status,
    INITCAP(TRIM(channel)) AS channel
FROM raw.policies_raw;

DROP TABLE IF EXISTS clean.claims;
CREATE TABLE clean.claims AS
SELECT
    TRIM(claim_id) AS claim_id,
    TRIM(policy_id) AS policy_id,
    claim_date,
    claim_amount,
    CASE
        WHEN UPPER(TRIM(claim_status)) = 'APPROVED' THEN 'Approved'
        WHEN UPPER(TRIM(claim_status)) = 'REJECTED' THEN 'Rejected'
        WHEN UPPER(TRIM(claim_status)) = 'PENDING' THEN 'Pending'
        ELSE 'Unknown'
    END AS claim_status,
    TRIM(diagnosis_code) AS diagnosis_code
FROM raw.claims_raw;

DROP TABLE IF EXISTS clean.customers;
CREATE TABLE clean.customers AS
SELECT
    TRIM(customer_id) AS customer_id,
    INITCAP(TRIM(customer_name)) AS customer_name,
    CASE
        WHEN UPPER(TRIM(gender)) = 'M' THEN 'Male'
        WHEN UPPER(TRIM(gender)) = 'F' THEN 'Female'
        ELSE 'Unknown'
    END AS gender,
    birth_date,
    INITCAP(TRIM(city)) AS city
FROM raw.customers_raw;