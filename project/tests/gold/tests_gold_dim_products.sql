

-- ====================================================================
-- Checking 'gold_product_key'   
-- ====================================================================

-- Check for Uniqueness of Product Key in gold_dim_products
-- Expectation: No results 

/*
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM {{ ref('gold_dim_products') }}
GROUP BY product_key
HAVING COUNT(*) > 1;
*/