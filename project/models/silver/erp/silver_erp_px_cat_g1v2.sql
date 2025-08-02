

{{ config(
    materialized='table',
    schema='silver'
) }}

WITH PX_CAT_G1V2 AS (
    SELECT * FROM {{ ref('bronze_erp_px_cat_g1v2') }}
)

SELECT 
  *,
  COALESCE(dwh_create_date, CURRENT_TIMESTAMP()) AS dwh_create_date  -- add this column  
FROM PX_CAT_G1V2;




