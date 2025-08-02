
{{ config
        (
        materialized='table', 
        schema='bronze'
        ) 
}}


WITH raw_PX_CAT_G1V2 AS (
    SELECT * FROM {{ source('raw_for_bronze', 'P_G1V2') }}
)

SELECT 
  ID as id,
  CAT as cat,
  SUBCAT as subcat,
  MAINTENANCE as maintenance
FROM raw_PX_CAT_G1V2;