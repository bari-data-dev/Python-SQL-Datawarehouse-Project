-- Check For Null Values in Primary Key
-- Expectation : No Result

SELECT 
cid,
COUNT(*)
FROM bronze_client1.erp_loc_a101
GROUP BY cid
HAVING COUNT(*) > 1 OR cid IS NULL


-- Check Unwanted Spaces
-- Exprecttion : No Result
-- To all string datatype

SELECT cid
FROM bronze_client1.erp_loc_a101
WHERE cid != TRIM(cid)

SELECT cntry
FROM bronze_client1.erp_loc_a101
WHERE cntry != TRIM(cntry)


-- Check for Null or Negative on Numbers type
-- Expectation : No Result
-- Change Null to 0 if allowed if Negative?
-



-- Data Standardization & Consistency on low Cardinality Cols

SELECT DISTINCT cntry
FROM bronze_client1.erp_loc_a101
ORDER BY cntry

-- Check for Invalid Date Orders
-- Expectations : No out of range date
-

-- Add Column cat_id and change DATETIME to DATE on silver_client1.crm_prd_info
-







