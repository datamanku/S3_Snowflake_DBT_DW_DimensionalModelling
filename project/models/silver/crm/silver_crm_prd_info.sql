

{{ config
        (
        materialized='table', 
        schema='silver'
        ) 
}}


WITH prd_info AS (
    SELECT * FROM {{ ref('bronze_crm_prd_info') }}
)

SELECT
  prd_id,
  REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract category ID -- new added column
  SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,        -- Extract product key
  prd_nm,
  COALESCE(prd_cost, 0) AS prd_cost,
  CASE 
    WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
    WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
    WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
    WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
    ELSE 'n/a'
  END AS prd_line, -- Map product line codes to descriptive values
  CAST(prd_start_dt AS DATE) AS prd_start_dt,
  CAST(
    DATEADD(day, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) 
    AS DATE
  ) AS prd_end_dt, -- Calculate end date as one day before the next start date
  COALESCE(dwh_create_date, CURRENT_TIMESTAMP()) AS dwh_create_date  -- add this column
FROM prd_info;