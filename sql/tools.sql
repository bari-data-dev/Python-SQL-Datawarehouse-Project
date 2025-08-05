CREATE TABLE IF NOT EXISTS tools.client_reference
(
    client_id integer NOT NULL DEFAULT nextval('tools.client_reference_client_id_seq'::regclass),
    client_schema character varying(50) COLLATE pg_catalog."default" NOT NULL,
    client_name character varying(100) COLLATE pg_catalog."default",
    mapping_version character varying(10) COLLATE pg_catalog."default",
    required_column_version character varying(10) COLLATE pg_catalog."default",
    config_version character varying(10) COLLATE pg_catalog."default",
    last_batch_id character varying(30) COLLATE pg_catalog."default",
    CONSTRAINT client_reference_pkey PRIMARY KEY (client_id),
    CONSTRAINT client_reference_client_schema_key UNIQUE (client_schema)
)

CREATE TABLE IF NOT EXISTS tools.client_config
(
    config_id integer NOT NULL DEFAULT nextval('tools.client_config_config_id_seq'::regclass),
    client_schema character varying(50) COLLATE pg_catalog."default" NOT NULL,
    source_type character varying(20) COLLATE pg_catalog."default" NOT NULL,
    target_schema character varying(50) COLLATE pg_catalog."default" NOT NULL,
    target_table character varying(50) COLLATE pg_catalog."default" NOT NULL,
    source_config jsonb,
    is_active boolean DEFAULT false,
    config_version character varying(10) COLLATE pg_catalog."default",
    logical_source_file character varying(100) COLLATE pg_catalog."default",
    CONSTRAINT client_config_pkey PRIMARY KEY (config_id)
)

CREATE TABLE IF NOT EXISTS tools.column_mapping
(
    mapping_id integer NOT NULL DEFAULT nextval('tools.column_mapping_mapping_id_seq'::regclass),
    client_schema character varying(50) COLLATE pg_catalog."default" NOT NULL,
    source_column character varying(100) COLLATE pg_catalog."default" NOT NULL,
    target_column character varying(100) COLLATE pg_catalog."default" NOT NULL,
    mapping_version character varying(10) COLLATE pg_catalog."default",
    logical_source_file character varying(100) COLLATE pg_catalog."default",
    CONSTRAINT column_mapping_pkey PRIMARY KEY (mapping_id)
)

CREATE TABLE IF NOT EXISTS tools.required_columns
(
    id integer NOT NULL DEFAULT nextval('tools.required_columns_id_seq'::regclass),
    client_schema character varying(50) COLLATE pg_catalog."default" NOT NULL,
    column_name character varying(100) COLLATE pg_catalog."default" NOT NULL,
    required_column_version character varying(10) COLLATE pg_catalog."default",
    logical_source_file character varying(100) COLLATE pg_catalog."default",
    CONSTRAINT required_columns_pkey PRIMARY KEY (id)
)

CREATE TABLE IF NOT EXISTS tools.file_audit_log
(
    file_audit_id integer NOT NULL DEFAULT nextval('tools.file_audit_log_file_audit_id_seq'::regclass),
    client_schema character varying(50) COLLATE pg_catalog."default",
    logical_source_file character varying(100) COLLATE pg_catalog."default",
    physical_file_name character varying(200) COLLATE pg_catalog."default",
    json_file_name character varying(200) COLLATE pg_catalog."default",
    file_received_time timestamp without time zone,
    json_converted_time timestamp without time zone,
    mapping_validation_status character varying(20) COLLATE pg_catalog."default",
    row_validation_status character varying(20) COLLATE pg_catalog."default",
    load_status character varying(20) COLLATE pg_catalog."default",
    total_rows integer,
    valid_rows integer,
    invalid_rows integer,
    processed_by character varying(100) COLLATE pg_catalog."default",
    convert_status character varying(20) COLLATE pg_catalog."default",
    batch_id character varying(30) COLLATE pg_catalog."default",
    CONSTRAINT file_audit_log_pkey PRIMARY KEY (file_audit_id)
)

CREATE TABLE IF NOT EXISTS tools.job_execution_log
(
    job_id integer NOT NULL DEFAULT nextval('tools.job_execution_log_job_id_seq'::regclass),
    job_name character varying(100) COLLATE pg_catalog."default",
    client_schema character varying(50) COLLATE pg_catalog."default",
    status character varying(20) COLLATE pg_catalog."default",
    start_time timestamp without time zone,
    end_time timestamp without time zone,
    error_message text COLLATE pg_catalog."default",
    file_name character varying(200) COLLATE pg_catalog."default",
    batch_id character varying(30) COLLATE pg_catalog."default",
    CONSTRAINT job_execution_log_pkey PRIMARY KEY (job_id)
)

CREATE TABLE IF NOT EXISTS tools.mapping_validation_log
(
    log_id integer NOT NULL DEFAULT nextval('tools.mapping_validation_log_log_id_seq'::regclass),
    client_schema character varying(50) COLLATE pg_catalog."default",
    expected_columns text COLLATE pg_catalog."default",
    received_columns text COLLATE pg_catalog."default",
    missing_columns text COLLATE pg_catalog."default",
    extra_columns text COLLATE pg_catalog."default",
    file_name character varying(200) COLLATE pg_catalog."default",
    "timestamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    batch_id character varying(30) COLLATE pg_catalog."default",
    CONSTRAINT mapping_validation_log_pkey PRIMARY KEY (log_id)
)

CREATE TABLE IF NOT EXISTS tools.row_validation_log
(
    error_id integer NOT NULL DEFAULT nextval('tools.row_validation_log_error_id_seq'::regclass),
    client_schema character varying(50) COLLATE pg_catalog."default",
    file_name character varying(200) COLLATE pg_catalog."default",
    row_number integer,
    column_name character varying(100) COLLATE pg_catalog."default",
    error_type character varying(100) COLLATE pg_catalog."default",
    error_detail text COLLATE pg_catalog."default",
    "timestamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    batch_id character varying(30) COLLATE pg_catalog."default",
    CONSTRAINT row_validation_log_pkey PRIMARY KEY (error_id)
)

CREATE TABLE IF NOT EXISTS tools.load_error_log
(
    error_id integer NOT NULL DEFAULT nextval('tools.load_error_log_error_id_seq'::regclass),
    client_schema character varying(50) COLLATE pg_catalog."default",
    file_name character varying(200) COLLATE pg_catalog."default",
    error_detail text COLLATE pg_catalog."default",
    stage character varying(100) COLLATE pg_catalog."default",
    "timestamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    batch_id character varying(30) COLLATE pg_catalog."default",
    CONSTRAINT load_error_log_pkey PRIMARY KEY (error_id)
)

CREATE TABLE IF NOT EXISTS tools.transformation_log
(
    log_id integer NOT NULL DEFAULT nextval('tools.transformation_log_log_id_seq'::regclass),
    client_schema character varying(50) COLLATE pg_catalog."default",
    source_table character varying(100) COLLATE pg_catalog."default",
    target_table character varying(100) COLLATE pg_catalog."default",
    record_count integer,
    status character varying(50) COLLATE pg_catalog."default",
    message text COLLATE pg_catalog."default",
    "timestamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    batch_id character varying(30) COLLATE pg_catalog."default",
    CONSTRAINT transformation_log_pkey PRIMARY KEY (log_id)
)
















