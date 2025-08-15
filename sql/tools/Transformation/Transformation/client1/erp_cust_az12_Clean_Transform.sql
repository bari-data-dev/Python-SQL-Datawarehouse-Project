SELECT
  CASE
    WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
    ELSE cid
  END AS cid,
  CASE
    WHEN bdate > CURRENT_DATE THEN NULL
    ELSE bdate
  END AS bdate,
  CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
   WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
   ELSE 'Unknown'
   END AS gen
FROM
  bronze_client1.erp_cust_az12