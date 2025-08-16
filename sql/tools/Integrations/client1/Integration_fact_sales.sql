SELECT
  sd.sls_ord_num AS order_number,
  pr.product_number,
  cs.customer_id,
  sd.sls_order_dt AS order_date,
  sd.sls_ship_dt AS shipping_date,
  sd.sls_due_dt AS due_date,
  sd.sls_sales AS sales,
  sd.sls_quantity AS quantity,
  sd.sls_price AS price sd.dwh_batch_id
FROM
  silver_client1.crm_sales_details sd
  LEFT JOIN gold_client1.dim_products pr ON sd.sls_prd_key = pr.product_number
  LEFT JOIN gold_client1.dim_customers cs ON sd.sls_cust_id = cs.customer_id