-- 1. Total sales per bulan, breakdown per product line
CREATE MATERIALIZED VIEW gold_client1.mv_sales_monthly_productline AS
SELECT 
    DATE_TRUNC('month', fs.order_date)::DATE AS month,
    dp.product_name,
    dp.category,
    dp.sub_category,
    SUM(fs.sales) AS total_sales
FROM gold_client1.fact_sales fs
JOIN gold_client1.dim_products dp 
  ON fs.product_key = dp.product_key
GROUP BY 1, 2, 3, 4;


-- 2. Total sales per customer, per country
CREATE MATERIALIZED VIEW gold_client1.mv_sales_customer_country AS
SELECT 
    dc.country,
    dc.customer_id,
    dc.customer_firstname || ' ' || dc.customer_lastname AS customer_name,
    SUM(fs.sales) AS total_sales
FROM gold_client1.fact_sales fs
JOIN gold_client1.dim_customers dc 
  ON fs.customer_key = dc.customer_key
GROUP BY 1,2,3;


-- 3. Customer Lifetime Value (CLV)
CREATE MATERIALIZED VIEW gold_client1.mv_customer_lifetime_value AS
SELECT 
    dc.customer_id,
    dc.customer_firstname || ' ' || dc.customer_lastname AS customer_name,
    SUM(fs.sales) AS lifetime_value,
    COUNT(DISTINCT fs.order_number) AS order_count
FROM gold_client1.fact_sales fs
JOIN gold_client1.dim_customers dc 
  ON fs.customer_key = dc.customer_key
GROUP BY 1,2;


-- 4. Running cumulative sales per customer
CREATE MATERIALIZED VIEW gold_client1.mv_running_sales_customer AS
SELECT
    dc.customer_id,
    fs.order_date,
    SUM(fs.sales) OVER (
        PARTITION BY dc.customer_id
        ORDER BY fs.order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_sales
FROM gold_client1.fact_sales fs
JOIN gold_client1.dim_customers dc 
  ON fs.customer_key = dc.customer_key;


-- 5. Top 3 produk terlaris tiap bulan, per negara
CREATE MATERIALIZED VIEW gold_client1.mv_top3_products_month_country AS
WITH sales_per_product AS (
    SELECT
        DATE_TRUNC('month', fs.order_date)::DATE AS month,
        dc.country,
        dp.product_name,
        SUM(fs.sales) AS total_sales
    FROM gold_client1.fact_sales fs
    JOIN gold_client1.dim_customers dc ON fs.customer_key = dc.customer_key
    JOIN gold_client1.dim_products dp  ON fs.product_key = dp.product_key
    GROUP BY 1,2,3
)
SELECT *
FROM (
    SELECT 
        s.*,
        RANK() OVER (PARTITION BY month, country ORDER BY total_sales DESC) AS rank_sales
    FROM sales_per_product s
) ranked
WHERE rank_sales <= 3;


-- 6. Customer churn (6 bulan tidak beli)
CREATE MATERIALIZED VIEW gold_client1.mv_customer_churn AS
SELECT
    dc.customer_id,
    MIN(fs.order_date) AS first_purchase,
    MAX(fs.order_date) AS last_purchase,
    CASE 
        WHEN MAX(fs.order_date) < CURRENT_DATE - INTERVAL '6 months'
        THEN TRUE ELSE FALSE
    END AS is_churn
FROM gold_client1.fact_sales fs
JOIN gold_client1.dim_customers dc ON fs.customer_key = dc.customer_key
GROUP BY dc.customer_id;



-- 7. Rata-rata jarak antar order tiap customer
CREATE MATERIALIZED VIEW gold_client1.mv_customer_order_gap AS
SELECT 
    customer_id,
    AVG(order_gap) AS avg_gap_days
FROM (
    SELECT
        dc.customer_id,
        fs.order_date,
        LAG(fs.order_date) OVER (PARTITION BY dc.customer_id ORDER BY fs.order_date) AS prev_date,
        (fs.order_date - LAG(fs.order_date) OVER (PARTITION BY dc.customer_id ORDER BY fs.order_date))::INT AS order_gap
    FROM gold_client1.fact_sales fs
    JOIN gold_client1.dim_customers dc 
      ON fs.customer_key = dc.customer_key
) g
WHERE prev_date IS NOT NULL
GROUP BY customer_id;

-- 8. Total sales dengan ROLLUP (product_line, category, sub_category)
CREATE MATERIALIZED VIEW gold_client1.mv_sales_rollup_product AS
SELECT
    dp.product_line,
    dp.category,
    dp.sub_category,
    SUM(fs.sales) AS total_sales
FROM gold_client1.fact_sales fs
JOIN gold_client1.dim_products dp ON fs.product_key = dp.product_key
GROUP BY ROLLUP (dp.product_line, dp.category, dp.sub_category);



-- 9. Recursive: rantai order delay
CREATE MATERIALIZED VIEW gold_client1.mv_delayed_orders_chain AS
WITH RECURSIVE delayed_orders AS (
    SELECT
        fs.order_number,
        fs.customer_key,
        fs.shipping_date,
        fs.due_date,
        1 AS level
    FROM gold_client1.fact_sales fs
    WHERE fs.shipping_date > fs.due_date
    UNION ALL
    SELECT
        fs.order_number,
        fs.customer_key,
        fs.shipping_date,
        fs.due_date,
        d.level + 1
    FROM gold_client1.fact_sales fs
    JOIN delayed_orders d 
      ON fs.customer_key = d.customer_key
     AND fs.order_date > d.shipping_date
)
SELECT * FROM delayed_orders;


INSERT INTO tools.mv_refresh_config (
    client_id,
    mv_proc_name,
    is_active,
    refresh_mode,
    created_at,
    created_by
)
VALUES
    (2, 'mv_customer_churn', TRUE, 'FULL', NOW(), 'system'),
    (2, 'mv_customer_lifetime_value',    TRUE, 'FULL', NOW(), 'system'),
    (2, 'mv_customer_order_gap',   TRUE, 'FULL', NOW(), 'system'),
    (2, 'mv_delayed_orders_chain',    TRUE, 'FULL', NOW(), 'system'),
    (2, 'mv_running_sales_customer', TRUE, 'FULL', NOW(), 'system'),
    (2, 'mv_sales_customer_country',            TRUE, 'FULL', NOW(), 'system'),
    (2, 'mv_sales_monthly_productline',        TRUE, 'FULL', NOW(), 'system'),
    (2, 'mv_sales_rollup_product',      TRUE, 'FULL', NOW(), 'system'),
    (2, 'mv_top3_products_month_country',      TRUE, 'FULL', NOW(), 'system');

















