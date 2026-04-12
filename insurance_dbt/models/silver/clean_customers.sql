SELECT
    TRIM(customer_id)                   AS customer_id,
    INITCAP(TRIM(customer_name))        AS customer_name,
    CASE
        WHEN UPPER(TRIM(gender)) = 'M' THEN 'Male'
        WHEN UPPER(TRIM(gender)) = 'F' THEN 'Female'
        ELSE 'Unknown'
    END                                 AS gender,
    birth_date,
    INITCAP(TRIM(city))                 AS city
FROM {{ source('raw', 'customers_raw') }}
