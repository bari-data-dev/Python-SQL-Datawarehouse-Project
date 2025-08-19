-- ============================================
-- CLIENT REFERENCE & CONFIGURATION
-- ============================================
CREATE TABLE IF NOT EXISTS tools.client_reference (
    client_id              INTEGER NOT NULL DEFAULT nextval('tools.client_reference_client_id_seq'::regclass),
    client_schema          VARCHAR(50) NOT NULL,
    client_name            VARCHAR(100),
    mapping_version        VARCHAR(10),
    required_column_version VARCHAR(10),
    config_version         VARCHAR(10),
    last_batch_id          VARCHAR(30),
    CONSTRAINT client_reference_pkey PRIMARY KEY (client_id),
    CONSTRAINT client_reference_client_schema_key UNIQUE (client_schema)
);

CREATE TABLE IF NOT EXISTS tools.client_config (
    config_id            INTEGER NOT NULL DEFAULT nextval('tools.client_config_config_id_seq'::regclass),
    client_schema        VARCHAR(50) NOT NULL,
    source_type          VARCHAR(20) NOT NULL,
    target_schema        VARCHAR(50) NOT NULL,
    target_table         VARCHAR(50) NOT NULL,
    source_config        JSONB,
    is_active            BOOLEAN DEFAULT FALSE,
    config_version       VARCHAR(10),
    logical_source_file  VARCHAR(100),
    CONSTRAINT client_config_pkey PRIMARY KEY (config_id)
);

CREATE TABLE IF NOT EXISTS tools.column_mapping (
    mapping_id           INTEGER NOT NULL DEFAULT nextval('tools.column_mapping_mapping_id_seq'::regclass),
    client_schema        VARCHAR(50) NOT NULL,
    source_column        VARCHAR(100) NOT NULL,
    target_column        VARCHAR(100) NOT NULL,
    mapping_version      VARCHAR(10),
    logical_source_file  VARCHAR(100),
    CONSTRAINT column_mapping_pkey PRIMARY KEY (mapping_id)
);

CREATE TABLE IF NOT EXISTS tools.required_columns (
    id                   INTEGER NOT NULL DEFAULT nextval('tools.required_columns_id_seq'::regclass),
    client_schema        VARCHAR(50) NOT NULL,
    column_name          VARCHAR(100) NOT NULL,
    required_column_version VARCHAR(10),
    logical_source_file  VARCHAR(100),
    CONSTRAINT required_columns_pkey PRIMARY KEY (id)
);

-- ============================================
-- AUDIT & LOGGING TABLES
-- ============================================
CREATE TABLE IF NOT EXISTS tools.file_audit_log (
    file_audit_id        INTEGER NOT NULL DEFAULT nextval('tools.file_audit_log_file_audit_id_seq'::regclass),
    client_schema        VARCHAR(50),
    logical_source_file  VARCHAR(100),
    physical_file_name   VARCHAR(200),
    json_file_name       VARCHAR(200),
    file_received_time   TIMESTAMP,
    json_converted_time  TIMESTAMP,
    mapping_validation_status VARCHAR(20),
    row_validation_status VARCHAR(20),
    load_status          VARCHAR(20),
    total_rows           INTEGER,
    valid_rows           INTEGER,
    invalid_rows         INTEGER,
    processed_by         VARCHAR(100),
    convert_status       VARCHAR(20),
    batch_id             VARCHAR(30),
    CONSTRAINT file_audit_log_pkey PRIMARY KEY (file_audit_id)
);

CREATE TABLE IF NOT EXISTS tools.job_execution_log (
    job_id               INTEGER NOT NULL DEFAULT nextval('tools.job_execution_log_job_id_seq'::regclass),
    job_name             VARCHAR(100),
    client_schema        VARCHAR(50),
    status               VARCHAR(20),
    start_time           TIMESTAMP,
    end_time             TIMESTAMP,
    error_message        TEXT,
    file_name            VARCHAR(200),
    batch_id             VARCHAR(30),
    CONSTRAINT job_execution_log_pkey PRIMARY KEY (job_id)
);

CREATE TABLE IF NOT EXISTS tools.mapping_validation_log (
    log_id               INTEGER NOT NULL DEFAULT nextval('tools.mapping_validation_log_log_id_seq'::regclass),
    client_schema        VARCHAR(50),
    expected_columns     TEXT,
    received_columns     TEXT,
    missing_columns      TEXT,
    extra_columns        TEXT,
    file_name            VARCHAR(200),
    "timestamp"          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id             VARCHAR(30),
    CONSTRAINT mapping_validation_log_pkey PRIMARY KEY (log_id)
);

CREATE TABLE IF NOT EXISTS tools.row_validation_log (
    error_id             INTEGER NOT NULL DEFAULT nextval('tools.row_validation_log_error_id_seq'::regclass),
    client_schema        VARCHAR(50),
    file_name            VARCHAR(200),
    row_number           INTEGER,
    column_name          VARCHAR(100),
    error_type           VARCHAR(100),
    error_detail         TEXT,
    "timestamp"          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id             VARCHAR(30),
    CONSTRAINT row_validation_log_pkey PRIMARY KEY (error_id)
);

CREATE TABLE IF NOT EXISTS tools.load_error_log (
    error_id             INTEGER NOT NULL DEFAULT nextval('tools.load_error_log_error_id_seq'::regclass),
    client_schema        VARCHAR(50),
    file_name            VARCHAR(200),
    error_detail         TEXT,
    stage                VARCHAR(100),
    "timestamp"          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id             VARCHAR(30),
    CONSTRAINT load_error_log_pkey PRIMARY KEY (error_id)
);

CREATE TABLE IF NOT EXISTS tools.transformation_log (
    log_id               INTEGER NOT NULL DEFAULT nextval('tools.transformation_log_log_id_seq'::regclass),
    client_schema        VARCHAR(50),
    source_table         VARCHAR(100),
    target_table         VARCHAR(100),
    record_count         INTEGER,
    status               VARCHAR(50),
    message              TEXT,
    "timestamp"          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id             VARCHAR(30),
    CONSTRAINT transformation_log_pkey PRIMARY KEY (log_id)
);

-- ============================================
-- TRANSFORMATION CONFIG
-- ============================================
CREATE TABLE IF NOT EXISTS tools.transformation_config (
    id              BIGSERIAL PRIMARY KEY,
    client_schema   TEXT NOT NULL,          -- contoh: 'client1'
    transform_version INT NOT NULL,         -- versi konfigurasi
    proc_name       TEXT NOT NULL,          -- ex: 'tools.transform_orders_client1'
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT NOW(),
    created_by      TEXT DEFAULT current_user
);

CREATE INDEX idx_transformation_config_schema_version
    ON tools.transformation_config (client_schema, transform_version);

-- Tambah kolom batch_id ke semua tabel bronze
DO $$
DECLARE
    tbl RECORD;
    col_name TEXT := 'dwh_batch_id';
    col_type TEXT := 'VARCHAR(255)';
BEGIN
    FOR tbl IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'bronze'
          AND table_type = 'BASE TABLE'
    LOOP
        EXECUTE format(
            'ALTER TABLE bronze.%I ADD COLUMN IF NOT EXISTS %I %s;',
            tbl.table_name, col_name, col_type
        );
    END LOOP;
END $$;

-- ============================================
-- INTEGRATION CONFIG & LOGS
-- ============================================
CREATE TABLE IF NOT EXISTS tools.integration_config (
    integration_id    SERIAL PRIMARY KEY,
    client_id         INTEGER NOT NULL,
    proc_name         VARCHAR(255) NOT NULL,
    integration_version VARCHAR(50) NOT NULL DEFAULT 'v1',
    is_active         BOOLEAN NOT NULL DEFAULT TRUE,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by        VARCHAR(100) DEFAULT 'system',
    table_type        VARCHAR(20),
    run_order         INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS tools.integration_dependencies (
    dependency_id     SERIAL PRIMARY KEY,
    client_id         INTEGER NOT NULL,
    fact_proc_name    VARCHAR(200) NOT NULL,
    dim_proc_name     VARCHAR(200) NOT NULL,
    CONSTRAINT fk_integration_dep_client FOREIGN KEY (client_id)
        REFERENCES tools.client_reference (client_id)
);

CREATE TABLE IF NOT EXISTS tools.integration_log (
    integration_log_id SERIAL PRIMARY KEY,
    client_id         INTEGER NOT NULL,
    status            VARCHAR(50),
    record_count      INTEGER,
    proc_name         VARCHAR(200) NOT NULL,
    table_type        VARCHAR(50),
    batch_id          VARCHAR(30),
    message           TEXT,
    start_time        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time          TIMESTAMP,
    CONSTRAINT fk_integration_log_client FOREIGN KEY (client_id)
        REFERENCES tools.client_reference (client_id)
);

-- ============================================
-- MATERIALIZED VIEW REFRESH CONFIG & LOGS
-- ============================================
CREATE TABLE IF NOT EXISTS tools.mv_refresh_config (
    mv_id            SERIAL PRIMARY KEY,
    client_id        INTEGER NOT NULL,
    mv_proc_name     VARCHAR(255) NOT NULL,
    is_active        BOOLEAN NOT NULL DEFAULT TRUE,
    refresh_mode     VARCHAR(20) DEFAULT 'manual',
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by       VARCHAR(100) DEFAULT 'system'
);

CREATE TABLE IF NOT EXISTS tools.mv_refresh_log (
    mv_log_id        SERIAL PRIMARY KEY,
    client_id        INTEGER NOT NULL,
    status           VARCHAR(50),
    proc_mv_name     VARCHAR(200) NOT NULL,
    batch_id         VARCHAR(30),
    message          TEXT,
    start_time       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time         TIMESTAMP,
    CONSTRAINT fk_mv_refresh_log_client FOREIGN KEY (client_id)
        REFERENCES tools.client_reference (client_id)
);
