
{{ config
        (
        materialized='table', 
        schema='bronze',
        alias='bronze_crm_cust_info'
        ) 
}}


WITH raw_cust_info AS (
    SELECT * FROM {{ source('raw_for_bronze', 'c_info') }}
)
SELECT 
  cst_id as cst_id,
  cst_key as cst_key,
  cst_firstname as cst_firstname,
  cst_lastname as cst_lastname,
  cst_marital_status as cst_marital_status,
  cst_gndr as cst_gndr,           
  cst_create_date as cst_create_date,  
FROM raw_cust_info;



