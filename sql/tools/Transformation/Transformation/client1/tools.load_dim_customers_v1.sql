CREATE PROCEDURE tools.load_dim_customers_v1(
  IN p_client_schema VARCHAR,
  IN p_batch_id VARCHAR,
  OUT is_success BOOLEAN,
  OUT error_message TEXT
) LANGUAGE plpgsql AS $$ DECLARE v_sql TEXT;
v_count INT;
v_client_id INT;
BEGIN -- Resolve client_id
SELECT
  client_id INTO v_client_id
FROM
  tools.client_reference
WHERE
  client_schema = p_client_schema;
IF v_client_id IS NULL THEN RAISE EXCEPTION 'Client schema % tidak ditemukan di client_reference',
p_client_schema;
END IF;
-- Validate batch_id
IF p_batch_id IS NULL
OR trim(p_batch_id) = '' THEN RAISE EXCEPTION 'Batch ID tidak boleh kosong';
END IF;
-- Check source data
EXECUTE format(
  'SELECT COUNT(*) FROM silver_client1.crm_cust_info WHERE dwh_batch_id = %L',
  p_batch_id
) INTO v_count;
IF v_count = 0 THEN is_success := true;
error_message := NULL;
RETURN;
END IF;
-- Clear existing batch in gold
EXECUTE format(
  'DELETE FROM gold_client1.dim_customers WHERE dwh_batch_id = %L',
  p_batch_id
);
-- Insert into gold
EXECUTE format(
  $sql$
  INSERT INTO
    gold_client1.dim_customers (
      customer_id,
      customer_number,
      customer_firstname,
      customer_lastname,
      gender,
      marital_status,
      country,
      birth_date,
      create_date,
      dwh_batch_id
    )
  SELECT
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname,
    ci.cst_lastname,
    CASE
      WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr
      ELSE COALESCE(ca.gen, 'Unknown')
    END,
    ci.cst_marital_status,
    la.cntry,
    ca.bdate,
    ci.cst_create_date,
    % L
  FROM
    silver_client1.crm_cust_info ci
    LEFT JOIN silver_client1.erp_cust_az12 ca ON ci.cst_key = ca.cid
    LEFT JOIN silver_client1.erp_loc_a101 la ON ci.cst_key = la.cid
  WHERE
    ci.dwh_batch_id = % L;
$sql$,
p_batch_id,
p_batch_id
);
GET DIAGNOSTICS v_count = ROW_COUNT;
-- Log success
INSERT INTO
  tools.transformation_log(
    client_id,
    source_table,
    target_table,
    record_count,
    status,
    message,
    batch_id
  )
VALUES
(
    v_client_id,
    'silver_client1.crm_cust_info',
    'gold_client1.dim_customers',
    v_count,
    'SUCCESS',
    'Transformation completed',
    p_batch_id
  );
is_success := true;
error_message := NULL;
EXCEPTION
WHEN OTHERS THEN
INSERT INTO
  tools.transformation_log(
    client_id,
    source_table,
    target_table,
    record_count,
    status,
    message,
    batch_id
  )
VALUES
(
    v_client_id,
    'silver_client1.crm_cust_info',
    'gold_client1.dim_customers',
    0,
    'FAILED',
    SQLERRM,
    p_batch_id
  );
is_success := false;
error_message := SQLERRM;
END;
$$;