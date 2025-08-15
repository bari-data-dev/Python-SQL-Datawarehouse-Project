SELECT
TRIM(REPLACE(cid, '-', '')) AS cid,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'Unknown'
ELSE TRIM(cntry)
END AS cntry
FROM bronze_client1.erp_loc_a101