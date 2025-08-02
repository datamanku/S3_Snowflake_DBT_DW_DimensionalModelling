

-- ====================================================================
-- Checking 'gold_dim_customers'    
-- ====================================================================

-- Check for Uniqueness of Customer Key in gold_dim_customers
-- Expectation: No results 

/*
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM {{ ref('gold_dim_customers') }}
GROUP BY customer_key
HAVING COUNT(*) > 1;
*/