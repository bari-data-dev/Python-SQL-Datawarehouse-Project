-- Check For Null Values in Primary Key
-- Expectation : No Result

SELECT 
cid,
COUNT(*)
FROM bronze_client1.erp_cust_az12
GROUP BY cid
HAVING COUNT(*) > 1 OR cid IS NULL


-- Check Unwanted Spaces
-- Exprecttion : No Result
-- To all string datatype

SELECT cid
FROM bronze_client1.erp_cust_az12
WHERE cid != TRIM(cid)

SELECT gen
FROM bronze_client1.erp_cust_az12
WHERE gen != TRIM(gen)


-- Check for Null or Negative on Numbers type
-- Expectation : No Result
-- Change Null to 0 if allowed if Negative?
-



-- Data Standardization & Consistency on low Cardinality Cols

SELECT DISTINCT gen
FROM bronze_client1.erp_cust_az12

-- Check for Invalid Date Orders
-- Expectations : No out of range date

SELECT DISTINCT
bdate
FROM bronze_client1.erp_cust_az12
WHERE bdate < '1900-01-01' OR bdate > CURRENT_DATE
ORDER BY bdate


-- Add Column cat_id and change DATETIME to DATE on silver_client1.crm_prd_info








