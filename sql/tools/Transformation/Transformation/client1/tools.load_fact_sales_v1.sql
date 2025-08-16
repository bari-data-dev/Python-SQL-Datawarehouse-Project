CREATE PROCEDURE tools.load_fact_sales_v1(
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
  'SELECT COUNT(*) FROM silver_client1.crm_sales_details WHERE dwh_batch_id = %L',
  p_batch_id
) INTO v_count;
IF v_count = 0 THEN is_success := true;
error_message := NULL;
RETURN;
END IF;
-- Clear existing batch
EXECUTE format(
  'DELETE FROM gold_client1.fact_sales WHERE dwh_batch_id = %L',
  p_batch_id
);
-- Insert into fact with surrogate key lookup
EXECUTE format(
  $sql$
  INSERT INTO
    gold_client1.fact_sales (
      order_number,
      customer_key,
      product_key,
      customer_id,
      product_number,
      order_date,
      shipping_date,
      due_date,
      sales,
      quantity,
      price,
      dwh_batch_id
    )
  SELECT
    sd.sls_ord_num,
    cs.customer_key,
    pr.product_key,
    cs.customer_id,
    pr.product_number,
    sd.sls_order_dt,
    sd.sls_ship_dt,
    sd.sls_due_dt,
    sd.sls_sales,
    sd.sls_quantity,
    sd.sls_price,
    % L
  FROM
    silver_client1.crm_sales_details sd
    LEFT JOIN gold_client1.dim_products pr ON sd.sls_prd_key = pr.product_number
    LEFT JOIN gold_client1.dim_customers cs ON sd.sls_cust_id = cs.customer_id
  WHERE
    sd.dwh_batch_id = % L;
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
    'silver_client1.crm_sales_details',
    'gold_client1.fact_sales',
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
    'silver_client1.crm_sales_details',
    'gold_client1.fact_sales',
    0,
    'FAILED',
    SQLERRM,
    p_batch_id
  );
is_success := false;
error_message := SQLERRM;
END;
$$;