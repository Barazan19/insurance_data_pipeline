SELECT
    cu.customer_id,
    cu.customer_name,
    cu.gender,
    cu.city,
    COUNT(DISTINCT p.policy_id)                                     AS total_policies,
    COUNT(c.claim_id)                                               AS total_claims,
    COALESCE(SUM(c.claim_amount), 0)                                AS total_claim_amount,
    COALESCE(SUM(p.premium_amount), 0)                              AS total_premium_amount,
    ROUND(
        COALESCE(SUM(c.claim_amount), 0) /
        NULLIF(COALESCE(SUM(p.premium_amount), 0), 0),
        4
    )                                                               AS loss_ratio
FROM {{ ref('clean_customers') }} cu
LEFT JOIN {{ ref('clean_policies') }} p  ON cu.customer_id = p.customer_id
LEFT JOIN {{ ref('clean_claims') }} c    ON p.policy_id = c.policy_id
GROUP BY cu.customer_id, cu.customer_name, cu.gender, cu.city
