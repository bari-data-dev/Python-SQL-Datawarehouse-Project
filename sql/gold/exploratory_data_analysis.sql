-- 1Client: “Tolong buat report total sales per bulan, dibreakdown per product line.”

SELECT 
    DATE_TRUNC('month', fs.order_date)::DATE AS month,
    dp.product_name,
    dp.category,
    dp.sub_category,
    SUM(fs.sales) AS total_sales
FROM gold_client1.fact_sales fs
JOIN gold_client1.dim_products dp 
  ON fs.product_key = dp.product_key
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2;

-- 2Client: “Berapa total sales per customer, di-group per country?”

SELECT 
    dc.country,
    dc.customer_id,
    dc.customer_firstname || ' ' || dc.customer_lastname AS customer_name,
    SUM(fs.sales) AS total_sales
FROM gold_client1.fact_sales fs
JOIN gold_client1.dim_customers dc 
  ON fs.customer_key = dc.customer_key
GROUP BY 1,2,3
ORDER BY total_sales DESC;


-- 3Customer Lifetime Value (CLV)

--- Client: “Berapa CLV per customer?”


SELECT 
    dc.customer_id,
    dc.customer_firstname || ' ' || dc.customer_lastname AS customer_name,
    SUM(fs.sales) AS lifetime_value,
    COUNT(DISTINCT fs.order_number) AS order_count
FROM gold_client1.fact_sales fs
JOIN gold_client1.dim_customers dc 
  ON fs.customer_key = dc.customer_key
GROUP BY 1,2
ORDER BY lifetime_value DESC;


-- 4Client: “Saya mau lihat perkembangan sales kumulatif setiap customer dari waktu ke waktu.”

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
  ON fs.customer_key = dc.customer_key
ORDER BY dc.customer_id, fs.order_date;


-- 5Client: “Saya mau tahu 3 produk terlaris tiap bulan, per negara customer.”

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
WHERE rank_sales <= 3
ORDER BY month, country, rank_sales;

-- 6Client: “Kapan customer pertama kali beli, kapan terakhir, dan apakah sudah 6 bulan tidak beli (churn)?”

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

-- 7Client: “Hitung rata-rata jarak antar order tiap customer.”

SELECT 
    customer_id,
    AVG(order_gap) AS avg_gap_days
FROM (
    SELECT
        dc.customer_id,
        fs.order_date,
        LAG(fs.order_date) OVER (PARTITION BY dc.customer_id ORDER BY fs.order_date) AS prev_date,
        EXTRACT(DAY FROM fs.order_date - LAG(fs.order_date) OVER (PARTITION BY dc.customer_id ORDER BY fs.order_date)) AS order_gap
    FROM gold_client1.fact_sales fs
    JOIN gold_client1.dim_customers dc ON fs.customer_key = dc.customer_key
) g
WHERE prev_date IS NOT NULL
GROUP BY customer_id;


SELECT 
    customer_id,
    AVG(order_gap) AS avg_gap_days
FROM (
    SELECT
        dc.customer_id,
        fs.order_date,
        LAG(fs.order_date) OVER (PARTITION BY dc.customer_id ORDER BY fs.order_date) AS prev_date,
        (fs.order_date - LAG(fs.order_date) OVER (PARTITION BY dc.customer_id ORDER BY fs.order_date))::INT AS order_gap -- revisi
    FROM gold_client1.fact_sales fs
    JOIN gold_client1.dim_customers dc 
      ON fs.customer_key = dc.customer_key
) g
WHERE prev_date IS NOT NULL
GROUP BY customer_id;

-- Client: “Kasih saya report total sales dengan kombinasi grouping per kategori, sub-kategori, dan product line.”

SELECT
    dp.product_line,
    dp.category,
    dp.sub_category,
    SUM(fs.sales) AS total_sales
FROM gold_client1.fact_sales fs
JOIN gold_client1.dim_products dp ON fs.product_key = dp.product_key
GROUP BY ROLLUP (dp.product_line, dp.category, dp.sub_category)
ORDER BY dp.product_line, dp.category, dp.sub_category;


-- Client: “Saya mau tahu rantai order yang delay, misalnya order yang shipping_date melewati due_date dan order berikutnya yang terkait customer sama apakah juga kena delay.”

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



