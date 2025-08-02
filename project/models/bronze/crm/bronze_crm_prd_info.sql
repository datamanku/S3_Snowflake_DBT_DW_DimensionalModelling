
{{ config
        (
        materialized='table', 
        schema='bronze'
        ) 
}}


WITH raw_prd_info AS (
    SELECT * FROM {{ source('raw_for_bronze', 'p_info') }}
)
SELECT 
  prd_id as prd_id,
  prd_key as prd_key,
  prd_nm as prd_nm,
  prd_cost as prd_cost,
  prd_line as prd_line,
  prd_start_dt as prd_start_dt,
  prd_end_dt as prd_end_dt,
FROM raw_prd_info;