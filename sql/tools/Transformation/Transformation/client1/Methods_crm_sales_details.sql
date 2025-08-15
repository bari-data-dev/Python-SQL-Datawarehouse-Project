-- Check For Null Values in Primary Key
-- Expectation : No Result

SELECT 
sls_prd_key,
COUNT(*)
FROM bronze_client1.crm_sales_details
GROUP BY sls_prd_key
HAVING sls_prd_key IS NULL

SELECT 
sls_cust_id,
COUNT(*)
FROM bronze_client1.crm_sales_details
GROUP BY sls_cust_id
HAVING sls_cust_id IS NULL


-- Check Unwanted Spaces
-- Exprecttion : No Result
-- To all string datatype

SELECT sls_ord_num
FROM bronze_client1.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num)


-- Check for Data Integrity Later
-- Setelah Tabel lain di load ke DB

WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver_client1.crm_cust_info)
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver_client1.crm_prd_info)


-- Check Consistency : Sales, Quantity, and Price
-- Sales : Quantity * Price != NULL, 0, Negative

SELECT DISTINCT
  sls_sales,
  sls_quantity,
  sls_price
FROM
  bronze_client1.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

-- Result
SELECT
  DISTINCT sls_sales AS sales_old,
  sls_quantity,
  sls_price AS price_old,
  CASE
    WHEN sls_sales IS NULL
    OR sls_sales <= 0
    OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
  END AS sls_sales,
  CASE
    WHEN sls_price IS NULL
    OR sls_price <= 0
    THEN ABS(sls_sales) / NULLIF(sls_quantity, 0)
    ELSE sls_price
  END AS sls_price
FROM
  bronze_client1.crm_sales_details
WHERE
  sls_sales != sls_quantity * sls_price
  OR sls_sales IS NULL
  OR sls_quantity IS NULL
  OR sls_price IS NULL
  OR sls_sales <= 0
  OR sls_quantity <= 0
  OR sls_price <= 0
ORDER BY
  sls_sales,
  sls_quantity,
  sls_price

-- Data Standardization & Consistency on low Cardinality Cols
-


-- Check for Invalid Date Orders
-- Expectations : No Overlapping, End Date > Start Date
-- Add change INT to DATE on silver_client1.crm_sales_details
SELECT
  NULLIF(sls_order_dt, 0) sls_order_dt
FROM
  bronze_client1.crm_sales_details
WHERE
  sls_order_dt <= 0
  OR LENGTH(sls_order_dt :: text) != 8
  OR sls_order_dt > 20300101
  OR sls_order_dt < 19000101
  
SELECT
  NULLIF(sls_ship_dt, 0) sls_ship_dt
FROM
  bronze_client1.crm_sales_details
WHERE
  sls_ship_dt <= 0
  OR LENGTH(sls_ship_dt :: text) != 8
  OR sls_ship_dt > 20300101
  OR sls_ship_dt < 19000101
  
SELECT
  NULLIF(sls_due_dt, 0) sls_due_dt
FROM
  bronze_client1.crm_sales_details
WHERE
  sls_due_dt <= 0
  OR LENGTH(sls_due_dt :: text) != 8
  OR sls_due_dt > 20300101
  OR sls_due_dt < 19000101
  
  
SELECT 
*
FROM bronze_client1.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt










