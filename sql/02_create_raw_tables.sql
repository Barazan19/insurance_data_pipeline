CREATE TABLE IF NOT EXISTS raw.policies_raw (
    policy_id VARCHAR(50),
    customer_id VARCHAR(50),
    policy_type VARCHAR(50),
    premium_amount NUMERIC(18,2),
    start_date DATE,
    end_date DATE,
    status VARCHAR(20),
    channel VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS raw.claims_raw (
    claim_id VARCHAR(50),
    policy_id VARCHAR(50),
    claim_date DATE,
    claim_amount NUMERIC(18,2),
    claim_status VARCHAR(30),
    diagnosis_code VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS raw.customers_raw (
    customer_id VARCHAR(50),
    customer_name VARCHAR(100),
    gender VARCHAR(10),
    birth_date DATE,
    city VARCHAR(50)
);