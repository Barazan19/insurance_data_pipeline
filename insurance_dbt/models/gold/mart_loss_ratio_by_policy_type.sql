SELECT
    p.policy_type,
    SUM(p.premium_amount)                                           AS total_premium,
    COALESCE(SUM(c.claim_amount), 0)                                AS total_claim,
    ROUND(
        COALESCE(SUM(c.claim_amount), 0) / NULLIF(SUM(p.premium_amount), 0),
        4
    )                                                               AS loss_ratio
FROM {{ ref('clean_policies') }} p
LEFT JOIN {{ ref('clean_claims') }} c ON p.policy_id = c.policy_id
GROUP BY p.policy_type
