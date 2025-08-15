SELECT
  prd_id,
  TRIM(REPLACE(substring(prd_key, 1, 5), '-', '_')) AS cat_id,
  TRIM(substring(prd_key, 7, LENGTH(prd_key))) AS prd_key,
  TRIM(prd_nm) AS prd_nm,
  COALESCE(prd_cost, 0) AS prd_cost,
  CASE
    WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
    WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Sport'
    WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
    WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
    ELSE 'Unknown'
  END AS prd_line,
  CAST(prd_start_dt AS DATE) AS prd_start_dt,
  CAST(
    LEAD(prd_start_dt) OVER (
      PARTITION BY prd_key
      ORDER BY
        prd_start_dt
    ) - INTERVAL '1 day' AS DATE
  ) AS prd_end_dt,
  dwh_batch_id
FROM
  bronze_client1.crm_prd_info