SELECT
    TRIM(claim_id)                      AS claim_id,
    TRIM(policy_id)                     AS policy_id,
    claim_date,
    claim_amount,
    CASE
        WHEN UPPER(TRIM(claim_status)) = 'APPROVED' THEN 'Approved'
        WHEN UPPER(TRIM(claim_status)) = 'REJECTED' THEN 'Rejected'
        WHEN UPPER(TRIM(claim_status)) = 'PENDING'  THEN 'Pending'
        ELSE 'Unknown'
    END                                 AS claim_status,
    TRIM(diagnosis_code)                AS diagnosis_code
FROM {{ source('raw', 'claims_raw') }}
