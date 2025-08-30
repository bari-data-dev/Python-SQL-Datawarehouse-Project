SELECT
  ROW_NUMBER() OVER (
    ORDER BY
      ci.cst_id
  ) AS customer_key,
  ci.cst_id AS customer_id,
  ci.cst_key AS customer_number,
  ci.cst_firstname AS customer_firstname,
  ci.cst_lastname AS customer_lastname,
  CASE
    WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr
    ELSE COALESCE(ca.gen, 'Unknown') :: varchar
  END AS gender,
  ci.cst_marital_status AS marital_status,
  la.cntry AS country,
  ca.bdate AS birth_date,
  ci.cst_create_date AS create_date,
  ci.dwh_batch_id
FROM
  silver_client1.crm_cust_info ci
  LEFT JOIN silver_client1.erp_cust_az12 ca 
      ON ci.cst_key = ca.cid
      AND ci.dwh_batch_id = ca.dwh_batch_id
  LEFT JOIN silver_client1.erp_loc_a101 la 
      ON ci.cst_key = la.cid;
      AND ci.dwh_batch_id = la.dwh_batch_id
