CREATE OR REPLACE PROCEDURE tools.load_{{table_name}}_v1(
    IN p_client_schema VARCHAR,      -- SCHEMA NAMA CLIENT (contoh: client1)
    IN p_batch_id VARCHAR,           -- BATCH ID YANG SEDANG DIPROSES
    OUT is_success BOOLEAN,          -- OUTPUT STATUS BERHASIL / TIDAK
    OUT error_message TEXT           -- OUTPUT PESAN ERROR JIKA GAGAL
)
LANGUAGE plpgsql
AS
$$
DECLARE
    v_sql TEXT;
    v_count INT;
    v_client_id INT;
BEGIN
    --------------------------------------------------------------------
    -- 1. AMBIL CLIENT_ID DARI TABEL REFERENSI
    --------------------------------------------------------------------
    SELECT client_id
    INTO v_client_id
    FROM tools.client_reference
    WHERE client_schema = p_client_schema;

    IF v_client_id IS NULL THEN
        RAISE EXCEPTION 'Client schema % tidak ditemukan di tools.client_reference', p_client_schema;
    END IF;

    --------------------------------------------------------------------
    -- 2. VALIDASI BATCH ID
    --------------------------------------------------------------------
    IF p_batch_id IS NULL OR trim(p_batch_id) = '' THEN
        RAISE EXCEPTION 'Batch ID tidak boleh kosong';
    END IF;

    --------------------------------------------------------------------
    -- 3. CEK APAKAH ADA DATA DI BRONZE UNTUK BATCH INI
    --------------------------------------------------------------------
    EXECUTE format(
        'SELECT COUNT(*) FROM %I.%I WHERE dwh_batch_id = %L',
        '{{bronze_schema}}', '{{table_name}}', p_batch_id
    )
    INTO v_count;

    -- JIKA TIDAK ADA DATA, ANGGAP BERHASIL DAN KELUAR
    IF v_count = 0 THEN
        is_success := true;
        error_message := NULL;
        RETURN;
    END IF;

    --------------------------------------------------------------------
    -- 4. CEK & TAMBAHKAN KOLOM dwh_batch_id DI SILVER JIKA BELUM ADA
    --------------------------------------------------------------------
    PERFORM 1
    FROM information_schema.columns
    WHERE table_schema = '{{silver_schema}}'
      AND table_name = '{{table_name}}'
      AND column_name = 'dwh_batch_id';

    IF NOT FOUND THEN
        v_sql := format(
            'ALTER TABLE %I.%I ADD COLUMN dwh_batch_id VARCHAR(30)',
            '{{silver_schema}}', '{{table_name}}'
        );
        EXECUTE v_sql;
    END IF;

    --------------------------------------------------------------------
    -- 5. HAPUS DATA EXISTING DI SILVER UNTUK BATCH INI
    --------------------------------------------------------------------
    EXECUTE format(
        'DELETE FROM %I.%I WHERE dwh_batch_id = %L',
        '{{silver_schema}}', '{{table_name}}', p_batch_id
    );

    --------------------------------------------------------------------
    -- 6. LAKUKAN INSERT DATA HASIL TRANSFORMASI
    --    GANTI BAGIAN SELECT DI BAWAH SESUAI LOGIC TRANSFORMASI
    --------------------------------------------------------------------
    EXECUTE format($sql$
        INSERT INTO {{silver_schema}}.{{table_name}} (
            -- LIST KOLOM TARGET DI SILVER
            col1,
            col2,
            col3,
            dwh_batch_id
        )
        SELECT
            -- LIST KOLOM HASIL TRANSFORMASI
            col1,
            UPPER(TRIM(col2)) AS col2,
            COALESCE(col3, 0) AS col3,
            %L
        FROM {{bronze_schema}}.{{table_name}}
        WHERE dwh_batch_id = %L
    $sql$, p_batch_id, p_batch_id);

    --------------------------------------------------------------------
    -- 7. HITUNG JUMLAH RECORD INSERT
    --------------------------------------------------------------------
    GET DIAGNOSTICS v_count = ROW_COUNT;

    --------------------------------------------------------------------
    -- 8. CATAT LOG KE tools.transformation_log (STATUS SUCCESS)
    --------------------------------------------------------------------
    INSERT INTO tools.transformation_log (
        client_id, source_table, target_table, record_count, status, message, batch_id
    ) VALUES (
        v_client_id,
        '{{bronze_schema}}.{{table_name}}',
        '{{silver_schema}}.{{table_name}}',
        v_count,
        'SUCCESS',
        'Transformation completed successfully',
        p_batch_id
    );

    -- SET OUTPUT
    is_success := true;
    error_message := NULL;

EXCEPTION
    --------------------------------------------------------------------
    -- 9. JIKA ERROR, CATAT LOG STATUS FAILED
    --------------------------------------------------------------------
    WHEN OTHERS THEN
        INSERT INTO tools.transformation_log (
            client_id, source_table, target_table, record_count, status, message, batch_id
        ) VALUES (
            v_client_id,
            '{{bronze_schema}}.{{table_name}}',
            '{{silver_schema}}.{{table_name}}',
            0,
            'FAILED',
            SQLERRM,
            p_batch_id
        );
        is_success := false;
        error_message := SQLERRM;
END;
$$;
