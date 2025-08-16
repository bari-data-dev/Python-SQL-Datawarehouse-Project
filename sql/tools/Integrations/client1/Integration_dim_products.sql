SELECT
  ROW_NUMBER() OVER (
    ORDER BY
      po.prd_key,
      po.prd_start_dt
  ) AS product_key,
  po.prd_id AS product_id,
  po.prd_key AS product_number,
  po.prd_nm AS product_name,
  po.prd_line AS product_line,
  po.cat_id AS category_id,
  pc.cat AS category,
  pc.subcat AS sub_category,
  pc.maintenance,
  po.prd_cost AS product_cost,
  po.prd_start_dt AS start_date po.dwh_batch_id
FROM
  silver_client1.crm_prd_info po
  LEFT JOIN silver_client1.erp_px_cat_g1v2 pc ON po.cat_id = pc.id
WHERE
  po.prd_end_dt IS NULL