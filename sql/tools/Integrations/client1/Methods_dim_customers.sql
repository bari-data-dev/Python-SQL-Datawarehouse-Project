-- Duplication Checks on PK
SELECT
  cst_id,
  COUNT(*)
FROM
  (
    SELECT
      ci.cst_id,
      ci.cst_key,
      ci.cst_firstname,
      ci.cst_lastname,
      ci.cst_marital_status,
      CASE
        WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr
        ELSE ca.gen
      END AS integrated_gender,
      ci.cst_create_date,
      ca.bdate,
      la.cntry
    FROM
      silver_client1.crm_cust_info ci
      LEFT JOIN silver_client1.erp_cust_az12 ca ON ci.cst_key = ca.cid
      LEFT JOIN silver_client1.erp_loc_a101 la ON ci.cst_key = la.cid
  ) t
GROUP BY
  cst_id
HAVING
  COUNT(*) > 1 
  


-- Data Integration


SELECT
  DISTINCT ci.cst_gndr,
  ca.gen,
  CASE
    WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr
    ELSE COALESCE(ca.gen, 'Unknown')
  END AS integrated_gender
FROM
  silver_client1.crm_cust_info ci
  LEFT JOIN silver_client1.erp_cust_az12 ca ON ci.cst_key = ca.cid
ORDER BY
  ci.cst_gndr,
  ca.gen
  
  
-- Add Surogate Keys if ther is none 
ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key
  
  
  
  
  
  
  
  