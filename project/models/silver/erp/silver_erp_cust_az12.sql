

{{ config
        (
        materialized='table', 
        schema='silver'
        ) 
}}


WITH CUST_AZ12 AS (
    SELECT * FROM {{ ref('bronze_erp_cust_az12') }}
)

SELECT
  CASE
    WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' prefix if present
    ELSE cid
  END AS cid, 
  CASE
    WHEN bdate > GETDATE() THEN NULL
    ELSE bdate
  END AS bdate, -- Set future birthdates to NULL
  CASE
    WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
    WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
    ELSE 'n/a'
  END AS gen -- Normalize gender values and handle unknown cases
  COALESCE(dwh_create_date, CURRENT_TIMESTAMP()) AS dwh_create_date  -- add this column
FROM CUST_AZ12;