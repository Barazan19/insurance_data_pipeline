SELECT
    TRIM(policy_id)                     AS policy_id,
    TRIM(customer_id)                   AS customer_id,
    INITCAP(TRIM(policy_type))          AS policy_type,
    premium_amount,
    start_date,
    end_date,
    CASE
        WHEN UPPER(TRIM(status)) = 'ACTIVE'  THEN 'Active'
        WHEN UPPER(TRIM(status)) = 'LAPSED'  THEN 'Lapsed'
        WHEN UPPER(TRIM(status)) = 'EXPIRED' THEN 'Expired'
        ELSE 'Unknown'
    END                                 AS policy_status,
    INITCAP(TRIM(channel))              AS channel
FROM {{ source('raw', 'policies_raw') }}
