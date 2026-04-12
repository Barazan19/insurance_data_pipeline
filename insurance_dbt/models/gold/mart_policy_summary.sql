WITH claim_per_policy AS (
    SELECT
        policy_id,
        SUM(claim_amount) AS total_claim
    FROM {{ ref('clean_claims') }}
    GROUP BY policy_id
)
SELECT
    p.policy_type,
    SUM(p.premium_amount)                                           AS total_premium,
    COALESCE(SUM(cpp.total_claim), 0)                               AS total_claim,
    ROUND(
        COALESCE(SUM(cpp.total_claim), 0) / NULLIF(SUM(p.premium_amount), 0),
        4
    )                                                               AS loss_ratio
FROM {{ ref('clean_policies') }} p
LEFT JOIN claim_per_policy cpp ON p.policy_id = cpp.policy_id
GROUP BY p.policy_type
