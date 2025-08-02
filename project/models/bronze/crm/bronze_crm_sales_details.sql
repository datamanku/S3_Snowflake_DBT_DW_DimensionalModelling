
{{ config
        (
        materialized='table', 
        schema='bronze'
        ) 
}}


WITH raw_sales_details AS (
    SELECT * FROM {{ source('raw_for_bronze', 's_details') }}
)

SELECT 
  sls_ord_num as sls_ord_num,
  sls_prd_key as sls_prd_key,
  sls_cust_id as sls_cust_id,
  sls_order_dt as sls_order_dt,
  sls_ship_dt as sls_ship_dt,
  sls_due_dt as sls_due_dt,
  sls_sales as sls_sales,
  sls_quantity as sls_quantity,
  sls_price as sls_price
FROM raw_sales_details;