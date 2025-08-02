


-- ====================================================================
-- Checking 'silver.erp_px_cat_g1v2'    
-- ====================================================================

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT 
    * 
FROM {{ ref('silver_erp_px_cat_g1v2') }}
WHERE cat != TRIM(cat) 
   OR subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance);
   

