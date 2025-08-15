SELECT
  cst_id,
  TRIM(cst_key) AS cst_key,
  TRIM(cst_firstname) AS cst_firstname,
  TRIM(cst_lastname) AS cst_lastname,
  CASE
    WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
    WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
    ELSE 'Unknown'
  END AS cst_marital_status,
  CASE
    WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
    WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
    ELSE 'Unknown'
  END AS cst_gndr,
  CASE
    WHEN EXTRACT(
      YEAR
      FROM
        cst_create_date
    ) > EXTRACT(
      YEAR
      FROM
        CURRENT_DATE
    ) THEN make_date(
      EXTRACT(
        YEAR
        FROM
          CURRENT_DATE
      ) :: int,
      EXTRACT(
        MONTH
        FROM
          cst_create_date
      ) :: int,
      EXTRACT(
        DAY
        FROM
          cst_create_date
      ) :: int
    )
    ELSE cst_create_date
  END AS cst_create_date,
  % L
FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY cst_id
        ORDER BY
          cst_create_date DESC
      ) as flag_last
    FROM
      bronze_client1.crm_cust_info
    WHERE
      cst_id IS NOT NULL
      AND dwh_batch_id = % L
  ) t
WHERE
  flag_last = 1;