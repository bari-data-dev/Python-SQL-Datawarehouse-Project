-- Duplication Checks on PK
SELECT prd_key, COUNT(*)FROM(
SELECT
po.prd_id,
po.cat_id,
po.prd_key,
po.prd_nm,
po.prd_cost,
po.prd_line,
po.prd_start_dt,
pc.cat,
pc.subcat,
pc.maintenance
FROM silver_client1.crm_prd_info po
LEFT JOIN silver_client1.erp_px_cat_g1v2 pc
ON po.cat_id = pc.id
WHERE po.prd_end_dt IS NULL
)t GROUP BY prd_key
HAVING COUNT(*) > 1