-- Check For Null Values in Primary Key
-- Expectation : No Result

SELECT 
id,
COUNT(*)
FROM bronze_client1.erp_px_cat_g1v2
GROUP BY id
HAVING COUNT(*) > 1 OR id IS NULL


-- Check Unwanted Spaces
-- Exprecttion : No Result
-- To all string datatype

SELECT cat
FROM bronze_client1.erp_px_cat_g1v2
WHERE cat != TRIM(cat)

SELECT subcat
FROM bronze_client1.erp_px_cat_g1v2
WHERE subcat != TRIM(subcat)

SELECT maintenance
FROM bronze_client1.erp_px_cat_g1v2
WHERE maintenance != TRIM(maintenance)


-- Check for Null or Negative on Numbers type
-- Expectation : No Result
-- Change Null to 0 if allowed if Negative?
-



-- Data Standardization & Consistency on low Cardinality Cols

SELECT DISTINCT maintenance
FROM bronze_client1.erp_px_cat_g1v2
ORDER BY maintenance

-- Check for Invalid Date Orders
-- Expectations : No out of range date
-

-- Add Column cat_id and change DATETIME to DATE on silver_client1.crm_prd_info
-







