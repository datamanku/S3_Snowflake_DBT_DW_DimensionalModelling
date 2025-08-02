

-- ====================================================================
-- Checking 'silver_crm_cust_info'
-- ====================================================================

-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results

/*
SELECT 
    cst_id,
    COUNT(*) 
FROM {{ ref('silver_crm_cust_info') }}
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;
*/

-- Check for Unwanted Spaces
-- Expectation: No Results

/*
SELECT 
    cst_key 
FROM {{ ref('silver_crm_cust_info') }}
WHERE cst_key != TRIM(cst_key);
*/

