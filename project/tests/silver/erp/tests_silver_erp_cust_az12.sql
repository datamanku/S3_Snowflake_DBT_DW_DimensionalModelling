


-- ====================================================================
-- Checking 'silver.erp_cust_az12'     
-- ====================================================================

-- Identify Out-of-Range Dates
-- Expectation: Birthdates between 1924-01-01 and Today

SELECT DISTINCT 
    bdate 
FROM {{ ref('silver_erp_cust_az12') }}
WHERE bdate < '1924-01-01' 
   OR bdate > GETDATE();


