
{{ config
        (
        materialized='table', 
        schema='bronze'
        ) 
}}


WITH raw_CUST_AZ12 AS (
    SELECT * FROM {{ source('raw_for_bronze', 'C_AZ12') }}
)

SELECT 
  CID as cid,
  BDATE as bdate,
  GEN as gen
FROM raw_CUST_AZ12;