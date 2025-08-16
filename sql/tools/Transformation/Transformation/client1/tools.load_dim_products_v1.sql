CREATE PROCEDURE tools.load_dim_products_v1(
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
  'SELECT COUNT(*) FROM silver_client1.crm_prd_info WHERE dwh_batch_id = %L',
  p_batch_id
) INTO v_count;
IF v_count = 0 THEN is_success := true;
error_message := NULL;
RETURN;
END IF;
-- Clear existing batch
EXECUTE format(
  'DELETE FROM gold_client1.dim_products WHERE dwh_batch_id = %L',
  p_batch_id
);
-- Insert into gold
EXECUTE format(
  $sql$
  INSERT INTO
    gold_client1.dim_products (
      product_id,
      product_number,
      product_name,
      product_line,
      category_id,
      category,
      sub_category,
      maintenance,
      product_cost,
      start_date,
      dwh_batch_id
    )
  SELECT
    po.prd_id,
    po.prd_key,
    po.prd_nm,
    po.prd_line,
    po.cat_id,
    pc.cat,
    pc.subcat,
    pc.maintenance,
    po.prd_cost,
    po.prd_start_dt,
    % L
  FROM
    silver_client1.crm_prd_info po
    LEFT JOIN silver_client1.erp_px_cat_g1v2 pc ON po.cat_id = pc.id
  WHERE
    po.prd_end_dt IS NULL
    AND po.dwh_batch_id = % L;
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
    'silver_client1.crm_prd_info',
    'gold_client1.dim_products',
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
    'silver_client1.crm_prd_info',
    'gold_client1.dim_products',
    0,
    'FAILED',
    SQLERRM,
    p_batch_id
  );
is_success := false;
error_message := SQLERRM;
END;
$$;