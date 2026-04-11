DROP TABLE IF EXISTS mart.policy_summary;

CREATE TABLE mart.policy_summary AS
WITH claim_per_policy AS (
    SELECT
        policy_id,
        SUM(claim_amount) AS total_claim
    FROM clean.claims
    GROUP BY policy_id
)
SELECT
    p.policy_type,
    SUM(p.premium_amount) AS total_premium,
    COALESCE(SUM(cpp.total_claim), 0) AS total_claim,
    ROUND(
        COALESCE(SUM(cpp.total_claim), 0) / NULLIF(SUM(p.premium_amount), 0),
        4
    ) AS claim_ratio
FROM clean.policies p
LEFT JOIN claim_per_policy cpp
    ON p.policy_id = cpp.policy_id
GROUP BY p.policy_type;