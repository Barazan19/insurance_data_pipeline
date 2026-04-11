DROP TABLE IF EXISTS mart.channel_performance;

CREATE TABLE mart.channel_performance AS
SELECT
    p.channel,
    COUNT(DISTINCT p.policy_id) AS total_policies,
    SUM(p.premium_amount) AS total_premium,
    COALESCE(SUM(c.claim_amount),0) AS total_claim,
    ROUND(
        COALESCE(SUM(c.claim_amount),0) / NULLIF(SUM(p.premium_amount),0),
        4
    ) AS loss_ratio
FROM clean.policies p
LEFT JOIN clean.claims c
    ON p.policy_id = c.policy_id
GROUP BY p.channel;

DROP TABLE IF EXISTS mart.customer_claim_summary;

CREATE TABLE mart.customer_claim_summary AS
SELECT
    cu.customer_id,
    cu.customer_name,
    cu.gender,
    cu.city,
    COUNT(DISTINCT p.policy_id) AS total_policies,
    COUNT(c.claim_id) AS total_claims,
    COALESCE(SUM(c.claim_amount),0) AS total_claim_amount,
    COALESCE(SUM(p.premium_amount),0) AS total_premium_amount,
    ROUND(
        COALESCE(SUM(c.claim_amount),0) /
        NULLIF(COALESCE(SUM(p.premium_amount),0),0),
        4
    ) AS loss_ratio
FROM clean.customers cu
LEFT JOIN clean.policies p
    ON cu.customer_id = p.customer_id
LEFT JOIN clean.claims c
    ON p.policy_id = c.policy_id
GROUP BY
    cu.customer_id,
    cu.customer_name,
    cu.gender,
    cu.city;

DROP TABLE IF EXISTS mart.claim_status_summary;

CREATE TABLE mart.claim_status_summary AS
SELECT
    claim_status,
    COUNT(*) AS total_claims,
    SUM(claim_amount) AS total_claim_amount
FROM clean.claims
GROUP BY claim_status;

DROP TABLE IF EXISTS mart.loss_ratio_by_policy_type;

CREATE TABLE mart.loss_ratio_by_policy_type AS
SELECT
    p.policy_type,
    SUM(p.premium_amount) AS total_premium,
    COALESCE(SUM(c.claim_amount), 0) AS total_claim,
    ROUND(
        COALESCE(SUM(c.claim_amount), 0) / NULLIF(SUM(p.premium_amount), 0),
        4
    ) AS loss_ratio
FROM clean.policies p
LEFT JOIN clean.claims c
    ON p.policy_id = c.policy_id
GROUP BY p.policy_type;
