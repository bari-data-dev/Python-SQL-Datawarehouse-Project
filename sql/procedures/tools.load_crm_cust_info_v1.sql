-- PROCEDURE: tools.load_crm_cust_info_v1(character varying, character varying)

-- DROP PROCEDURE IF EXISTS tools.load_crm_cust_info_v1(character varying, character varying);

CREATE OR REPLACE PROCEDURE tools.load_crm_prd_info_v1(
	IN p_client_schema character varying,
	IN p_batch_id character varying,
	OUT is_success boolean,
	OUT error_message text)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    v_sql text;
    v_count int;
BEGIN
    -- Validasi batch_id
    IF p_batch_id IS NULL OR trim(p_batch_id) = '' THEN
        RAISE EXCEPTION 'Batch ID tidak boleh kosong';
    END IF;

    -- 1. Cek apakah ada data di bronze
    EXECUTE format(
        'SELECT COUNT(*) FROM bronze_client1.crm_cust_info WHERE dwh_batch_id = %L',
        p_batch_id
    )
    INTO v_count;

    IF v_count = 0 THEN
        is_success := true;
        error_message := NULL;
        RETURN;
    END IF;

    -- 2. Cek & tambah kolom dwh_batch_id di silver jika belum ada
    PERFORM 1
    FROM information_schema.columns
    WHERE table_schema = 'silver_client1'
      AND table_name = 'crm_cust_info'
      AND column_name = 'dwh_batch_id';

    IF NOT FOUND THEN
        v_sql := format(
            'ALTER TABLE %I.%I ADD COLUMN dwh_batch_id varchar(30)',
            'silver_client1', 'crm_cust_info'
        );
        EXECUTE v_sql;
    END IF;

    -- 3. Hapus data existing di silver untuk batch ini
    EXECUTE format(
        'DELETE FROM silver_client1.crm_cust_info WHERE dwh_batch_id = %L',
        p_batch_id
    );

    -- 4. Insert data hasil transformasi
    EXECUTE format($sql$
        INSERT INTO silver_client1.crm_cust_info (
            cst_id, 
            cst_key, 
            cst_firstname, 
            cst_lastname, 
            cst_marital_status, 
            cst_gndr,
            cst_create_date,
            dwh_batch_id
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname),
            TRIM(cst_lastname),
            CASE 
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                ELSE 'n/a'
            END,
            CASE 
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
            END,
            cst_create_date,
            %L
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze_client1.crm_cust_info
            WHERE cst_id IS NOT NULL
              AND dwh_batch_id = %L
        ) t
        WHERE flag_last = 1
    $sql$, p_batch_id, p_batch_id);

    -- 5. Hitung jumlah record insert
    GET DIAGNOSTICS v_count = ROW_COUNT;

    -- 6. Insert log sukses
    INSERT INTO tools.transformation_log (
        client_schema, source_table, target_table, record_count, status, message, batch_id
    ) VALUES (
        p_client_schema,
        'bronze_client1.crm_cust_info',
        'silver_client1.crm_cust_info',
        v_count,
        'SUCCESS',
        'Transformation completed successfully',
        p_batch_id
    );

    is_success := true;
    error_message := NULL;

EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO tools.transformation_log (
            client_schema, source_table, target_table, record_count, status, message, batch_id
        ) VALUES (
            p_client_schema,
            'bronze_client1.crm_cust_info',
            'silver_client1.crm_cust_info',
            0,
            'FAILED',
            SQLERRM,
            p_batch_id
        );
        is_success := false;
        error_message := SQLERRM;
END;
$BODY$;
ALTER PROCEDURE tools.load_crm_cust_info_v1(character varying, character varying)
    OWNER TO postgres;
