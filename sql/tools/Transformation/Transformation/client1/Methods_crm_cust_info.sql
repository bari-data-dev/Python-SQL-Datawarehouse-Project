-- Check For Null Values in Primary Key
-- Expectation : No Result

SELECT 
cst_id,
COUNT(*)
FROM bronze_client1.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL


-- Check Unwanted Spaces
-- Exprecttion : No Result
-- To all string datatype

SELECT cst_firstname
FROM bronze_client1.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)


SELECT cst_lastname
FROM bronze_client1.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

SELECT cst_marital_status
FROM bronze_client1.crm_cust_info
WHERE cst_marital_status != TRIM(cst_marital_status)

SELECT cst_gndr
FROM bronze_client1.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr)


-- Data Standardization & Consistency on low Cardinality Cols

SELECT DISTINCT cst_gndr
FROM bronze_client1.crm_cust_info

SELECT DISTINCT cst_marital_status
FROM bronze_client1.crm_cust_info











