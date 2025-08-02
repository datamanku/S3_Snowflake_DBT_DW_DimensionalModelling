
{{ config
        (
        materialized='table', 
        schema='bronze'
        ) 
}}


WITH raw_LOC_A101 AS (
    SELECT * FROM {{ source('raw_for_bronze', 'L_A101') }}
)

SELECT 
  CID as cid,
  CNTRY as cntry
FROM raw_LOC_A101;