CREATE TABLE gold_client1.dim_customers (
    customer_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id VARCHAR,          -- natural key
    customer_number VARCHAR,
    customer_firstname VARCHAR,
    customer_lastname VARCHAR,
    gender VARCHAR,
    marital_status VARCHAR,
    country VARCHAR,
    birth_date DATE,
    create_date TIMESTAMP,
    dwh_batch_id VARCHAR
);


CREATE TABLE gold_client1.dim_products (
    product_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id VARCHAR,           -- natural key
    product_number VARCHAR,
    product_name VARCHAR,
    product_line VARCHAR,
    category_id VARCHAR,
    category VARCHAR,
    sub_category VARCHAR,
    maintenance VARCHAR,
    product_cost NUMERIC,
    start_date DATE,
    dwh_batch_id VARCHAR
);


CREATE TABLE gold_client1.fact_sales (
    sales_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_number VARCHAR,
    customer_key BIGINT REFERENCES gold_client1.dim_customers(customer_key),
    product_key BIGINT REFERENCES gold_client1.dim_products(product_key),
    customer_id VARCHAR,        -- keep natural key for lineage/debug
    product_number VARCHAR,     -- keep natural key for lineage/debug
    order_date DATE,
    shipping_date DATE,
    due_date DATE,
    sales NUMERIC,
    quantity NUMERIC,
    price NUMERIC,
    dwh_batch_id VARCHAR
);
