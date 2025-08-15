SELECT
  TRIM(sls_ord_num) AS sls_ord_num,
  TRIM(sls_prd_key) AS sls_prd_key,
  sls_cust_id,
  CASE
    WHEN sls_order_dt = 0
    OR LENGTH(sls_order_dt :: text) != 8 THEN NULL
    ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
  END AS sls_order_dt,
  CASE
    WHEN sls_ship_dt = 0
    OR LENGTH(sls_ship_dt :: text) != 8 THEN NULL
    ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
  END AS sls_ship_dt,
  CASE
    WHEN sls_due_dt = 0
    OR LENGTH(sls_due_dt :: text) != 8 THEN NULL
    ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
  END AS sls_due_dt,
  CASE
    WHEN sls_sales IS NULL
    OR sls_sales <= 0
    OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
  END AS sls_sales,
  sls_quantity,
  CASE
    WHEN sls_price IS NULL
    OR sls_price <= 0 THEN ABS(sls_sales) / NULLIF(sls_quantity, 0)
    ELSE sls_price
  END AS sls_price,
  dwh_batch_id
FROM
  bronze_client1.crm_sales_details