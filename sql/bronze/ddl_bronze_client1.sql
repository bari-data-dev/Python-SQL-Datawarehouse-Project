-- Buat schema jika belum ada
CREATE SCHEMA IF NOT EXISTS bronze_client1;

-- Tabel CRM: Customer Info
DROP TABLE IF EXISTS bronze_client1.crm_cust_info;
CREATE TABLE bronze_client1.crm_cust_info (
    cst_id              INTEGER,
    cst_key             VARCHAR(50),
    cst_firstname       VARCHAR(50),
    cst_lastname        VARCHAR(50),
    cst_marital_status  VARCHAR(50),
    cst_gndr            VARCHAR(50),
    cst_create_date     DATE
);

-- Tabel CRM: Product Info
DROP TABLE IF EXISTS bronze_client1.crm_prd_info;
CREATE TABLE bronze_client1.crm_prd_info (
    prd_id       INTEGER,
    prd_key      VARCHAR(50),
    prd_nm       VARCHAR(50),
    prd_cost     NUMERIC(10,5),
    prd_line     VARCHAR(50),
    prd_start_dt TIMESTAMP,
    prd_end_dt   TIMESTAMP
);

-- Tabel CRM: Sales Details
DROP TABLE IF EXISTS bronze_client1.crm_sales_details;
CREATE TABLE bronze_client1.crm_sales_details (
    sls_ord_num   VARCHAR(50),
    sls_prd_key   VARCHAR(50),
    sls_cust_id   INTEGER,
    sls_order_dt  INTEGER,   -- Jika bentuknya integer timestamp (epoch), bisa diubah nanti
    sls_ship_dt   INTEGER,
    sls_due_dt    INTEGER,
    sls_sales     NUMERIC(10,5),
    sls_quantity  INTEGER,
    sls_price     NUMERIC(10,5)
);

-- Tabel ERP: Lokasi
DROP TABLE IF EXISTS bronze_client1.erp_loc_a101;
CREATE TABLE bronze_client1.erp_loc_a101 (
    cid    VARCHAR(50),
    cntry  VARCHAR(50)
);

-- Tabel ERP: Customer
DROP TABLE IF EXISTS bronze_client1.erp_cust_az12;
CREATE TABLE bronze_client1.erp_cust_az12 (
    cid    VARCHAR(50),
    bdate  DATE,
    gen    VARCHAR(50)
);

-- Tabel ERP: Produk Kategori
DROP TABLE IF EXISTS bronze_client1.erp_px_cat_g1v2;
CREATE TABLE bronze_client1.erp_px_cat_g1v2 (
    id           VARCHAR(50),
    cat          VARCHAR(50),
    subcat       VARCHAR(50),
    maintenance  VARCHAR(50)
);
