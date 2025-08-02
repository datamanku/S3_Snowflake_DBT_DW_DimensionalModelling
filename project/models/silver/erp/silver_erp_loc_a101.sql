

{{ config
        (
        materialized='table', 
        schema='silver'
        ) 
}}


WITH LOC_A101 AS (
    SELECT * FROM {{ ref('bronze_erp_loc_a101') }}
)

SELECT 
  REPLACE(cid, '-', '') AS cid, 
  CASE
    WHEN TRIM(cntry) = 'DE' THEN 'Germany'
    WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
    WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
    ELSE TRIM(cntry)
  END AS cntry -- Normalize and Handle missing or blank country codes
  COALESCE(dwh_create_date, CURRENT_TIMESTAMP()) AS dwh_create_date  -- add this column
FROM LOC_A101;