SELECT
    claim_status,
    COUNT(*)                AS total_claims,
    SUM(claim_amount)       AS total_claim_amount
FROM {{ ref('clean_claims') }}
GROUP BY claim_status
