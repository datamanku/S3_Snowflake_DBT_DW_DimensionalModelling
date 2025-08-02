

-- ====================================================================
-- Checking 'silver_erp_loc_a101'      
-- ====================================================================

-- Check for Unwanted Spaces
-- Expectation: No Results

SELECT cntry
FROM {{ ref('silver_erp_loc_a101') }}
WHERE cntry IS NULL;