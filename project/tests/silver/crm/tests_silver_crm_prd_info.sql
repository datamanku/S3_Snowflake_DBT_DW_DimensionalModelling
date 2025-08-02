


-- ====================================================================
-- Checking 'silver_crm_prd_info'    
-- ====================================================================

-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT 
    prd_id,
    COUNT(*) 
FROM {{ ref('silver_crm_prd_info') }}
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT 
    prd_nm 
FROM {{ ref('silver_crm_prd_info') }}
WHERE prd_nm != TRIM(prd_nm);

-- Check for NULLs or Negative Values in Cost
-- Expectation: No Results
SELECT 
    prd_cost 
FROM {{ ref('silver_crm_prd_info') }}
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Check for Invalid Date Orders (Start Date > End Date)
-- Expectation: No Results
SELECT 
    * 
FROM {{ ref('silver_crm_prd_info') }}
WHERE prd_end_dt < prd_start_dt;