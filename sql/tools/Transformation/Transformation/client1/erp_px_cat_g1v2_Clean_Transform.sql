SELECT
id,
TRIM(cat) AS cat,
TRIM(subcat) AS subcat,
TRIM(maintenance) AS maintenance
FROM bronze_client1.erp_px_cat_g1v2
