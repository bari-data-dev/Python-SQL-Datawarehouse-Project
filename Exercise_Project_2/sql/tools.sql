-- Schema tools
CREATE SCHEMA IF NOT EXISTS tools;

-- CLIENT_REFERENCE
CREATE TABLE tools.client_reference (
    client_id SERIAL PRIMARY KEY,
    client_schema VARCHAR(50) NOT NULL UNIQUE,
    client_name VARCHAR(100)
);

ALTER TABLE tools.client_reference
ADD COLUMN active_schema_version VARCHAR(20);

-- CLIENT_CONFIG
CREATE TABLE tools.client_config (
    config_id SERIAL PRIMARY KEY,
    client_schema VARCHAR(50) NOT NULL,
    schema_version VARCHAR(50) NOT NULL,
    source_type VARCHAR(20) NOT NULL, -- csv, excel, api, db
    target_schema VARCHAR(50) NOT NULL,
    target_table VARCHAR(50) NOT NULL,
    source_config JSONB, -- Optional: for API/DB
    is_active BOOLEAN DEFAULT FALSE,
    UNIQUE (client_schema, schema_version)
);

-- COLUMN_MAPPING
CREATE TABLE tools.column_mapping (
    mapping_id SERIAL PRIMARY KEY,
    client_schema VARCHAR(50) NOT NULL,
    schema_version VARCHAR(50) NOT NULL,
    source_column VARCHAR(100) NOT NULL,
    target_column VARCHAR(100) NOT NULL
);

-- REQUIRED_COLUMNS (optional)
CREATE TABLE tools.required_columns (
    id SERIAL PRIMARY KEY,
    client_schema VARCHAR(50) NOT NULL,
    schema_version VARCHAR(50) NOT NULL,
    column_name VARCHAR(100) NOT NULL
);

-- ERROR_LOG
CREATE TABLE tools.error_log (
    error_id SERIAL PRIMARY KEY,
    client_schema VARCHAR(50),
    error_type VARCHAR(50),
    error_detail TEXT,
    file_name VARCHAR(200),
    row_number INTEGER,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- TRANSFORMATION_LOG (optional)
CREATE TABLE tools.transformation_log (
    log_id SERIAL PRIMARY KEY,
    client_schema VARCHAR(50),
    source_table VARCHAR(100),
    target_table VARCHAR(100),
    record_count INT,
    status VARCHAR(50), -- success, partial, failed
    message TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

SELECT * FROM tools.client_reference;
SELECT * FROM tools.client_config;
SELECT * FROM tools.column_mapping;
SELECT * FROM tools.required_columns;
SELECT * FROM tools.error_log;

INSERT INTO tools.client_reference (client_schema, client_name, active_schema_version)
VALUES ('client1', 'Retail1', 'v1');

INSERT INTO tools.client_config (client_schema, schema_version, source_type, target_schema, target_table, is_active)
VALUES 
('client1', 'v1', 'csv', 'bronze_client1', 'crm_cust_info', TRUE),
('client1', 'v1', 'csv', 'bronze_client1', 'crm_prd_info', TRUE),
('client1', 'v1', 'csv', 'bronze_client1', 'crm_sales_details', TRUE),
('client1', 'v1', 'csv', 'bronze_client1', 'erp_cust_az12', TRUE),
('client1', 'v1', 'csv', 'bronze_client1', 'erp_loc_a101', TRUE),
('client1', 'v1', 'csv', 'bronze_client1', 'erp_px_cat_g1v2', TRUE);


INSERT INTO tools.column_mapping (client_schema, schema_version, source_column, target_column)
VALUES
('client1', 'v1', 'cst_id', 'cst_id'),
('client1', 'v1', 'cst_key', 'cst_key'),
('client1', 'v1', 'cst_firstname', 'cst_firstname'),
('client1', 'v1', 'cst_lastname', 'cst_lastname'),
('client1', 'v1', 'cst_marital_status', 'cst_marital_status'),
('client1', 'v1', 'cst_gndr', 'cst_gndr'),
('client1', 'v1', 'cst_create_date', 'cst_create_date');



INSERT INTO tools.required_columns (client_schema, schema_version, column_name)
VALUES
('client1', 'v1', 'cst_id'),
('client1', 'v1', 'cst_key'),
('client1', 'v1', 'cst_firstname'),
('client1', 'v1', 'cst_create_date');



ALTER TABLE tools.column_mapping
ADD COLUMN source_file VARCHAR(100);

UPDATE tools.column_mapping
SET source_file = 'cust_info';











