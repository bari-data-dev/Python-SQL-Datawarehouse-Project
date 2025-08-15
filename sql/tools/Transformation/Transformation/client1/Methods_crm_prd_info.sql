-- Check For Null Values in Primary Key
-- Expectation : No Result

SELECT 
prd_id,
COUNT(*)
FROM bronze_client1.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL


-- Check Unwanted Spaces
-- Exprecttion : No Result
-- To all string datatype

SELECT prd_nm
FROM bronze_client1.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

SELECT prd_line
FROM bronze_client1.crm_prd_info
WHERE prd_line != TRIM(prd_line)


-- Check for Null or Negative on Numbers type
-- Expectation : No Result
-- Change Null to 0 if allowed if Negative?
SELECT prd_cost
FROM bronze_client1.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL



-- Data Standardization & Consistency on low Cardinality Cols

SELECT DISTINCT prd_line
FROM bronze_client1.crm_prd_info

-- Check for Invalid Date Orders
-- Expectations : No Overlapping, End Date > Start Date
SELECT *
FROM bronze_client1.crm_prd_info
WHERE prd_end_dt < prd_start_dt

SELECT prd_start_dt
FROM bronze_client1.crm_prd_info
WHERE prd_start_dt IS NULL

-- Add Column cat_id and change DATETIME to DATE on silver_client1.crm_prd_info








