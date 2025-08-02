

{{ config
        (
        materialized='table', 
        schema='silver'
        ) 
}}


WITH ranked_cust AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM {{ ref('bronze_crm_cust_info') }}
    WHERE cst_id IS NOT NULL
)

SELECT
  cst_id,
  cst_key,
  TRIM(cst_firstname) AS cst_firstname,
  TRIM(cst_lastname) AS cst_lastname,
  CASE 
    WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
    WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
    ELSE 'n/a'
  END AS cst_marital_status, -- Normalize marital status values to readable format
  CASE 
    WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
    WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
    ELSE 'n/a'
  END AS cst_gndr, -- Normalize gender values to readable format
  cst_create_date,
  COALESCE(dwh_create_date, CURRENT_TIMESTAMP()) AS dwh_create_date  -- add this column
FROM ranked_cust
WHERE flag_last = 1; 


