-- Sample analytical queries for Power BI / BI tools

-- Sales by Product Category
SELECT 
    dp.category,
    dp.subcategory,
    COUNT(DISTINCT fs.transaction_id) as transaction_count,
    SUM(fs.quantity) as total_quantity,
    SUM(fs.total_amount) as total_revenue,
    AVG(fs.total_amount) as avg_transaction_value
FROM fact_sales fs
JOIN dim_product dp ON fs.product_key = dp.product_key
WHERE dp.is_current = TRUE
GROUP BY dp.category, dp.subcategory
ORDER BY total_revenue DESC;

-- Sales by Time Period
SELECT 
    dd.year,
    dd.quarter,
    dd.month_name,
    COUNT(DISTINCT fs.transaction_id) as transaction_count,
    SUM(fs.total_amount) as total_revenue,
    SUM(fs.tax_amount) as total_tax
FROM fact_sales fs
JOIN dim_date dd ON fs.date_key = dd.date_key
GROUP BY dd.year, dd.quarter, dd.month_name, dd.month
ORDER BY dd.year, dd.quarter, dd.month;

-- Top Customers by Revenue
SELECT 
    dc.customer_id,
    dc.first_name || ' ' || dc.last_name as customer_name,
    dc.city,
    dc.state,
    COUNT(DISTINCT fs.transaction_id) as purchase_count,
    SUM(fs.total_amount) as total_spent,
    AVG(fs.total_amount) as avg_transaction
FROM fact_sales fs
JOIN dim_customer dc ON fs.customer_key = dc.customer_key
WHERE dc.is_current = TRUE
GROUP BY dc.customer_id, dc.first_name, dc.last_name, dc.city, dc.state
ORDER BY total_spent DESC
LIMIT 100;

-- Sales by Region
SELECT 
    dl.region,
    dl.state,
    COUNT(DISTINCT fs.transaction_id) as transaction_count,
    SUM(fs.quantity) as total_units_sold,
    SUM(fs.total_amount) as total_revenue
FROM fact_sales fs
JOIN dim_location dl ON fs.location_key = dl.location_key
GROUP BY dl.region, dl.state
ORDER BY total_revenue DESC;

-- Product Performance Analysis
SELECT 
    dp.product_name,
    dp.category,
    dp.brand,
    COUNT(DISTINCT fs.transaction_id) as times_sold,
    SUM(fs.quantity) as total_quantity,
    SUM(fs.total_amount) as total_revenue,
    AVG(fs.discount) as avg_discount,
    (dp.unit_price - dp.unit_cost) * SUM(fs.quantity) as estimated_profit
FROM fact_sales fs
JOIN dim_product dp ON fs.product_key = dp.product_key
WHERE dp.is_current = TRUE
GROUP BY dp.product_name, dp.category, dp.brand, dp.unit_price, dp.unit_cost
ORDER BY total_revenue DESC
LIMIT 50;

-- Monthly Sales Trend
SELECT 
    dd.year,
    dd.month,
    dd.month_name,
    SUM(fs.total_amount) as monthly_revenue,
    COUNT(DISTINCT fs.customer_key) as unique_customers,
    COUNT(DISTINCT fs.transaction_id) as transaction_count
FROM fact_sales fs
JOIN dim_date dd ON fs.date_key = dd.date_key
GROUP BY dd.year, dd.month, dd.month_name
ORDER BY dd.year, dd.month;

-- Customer Segmentation by Purchase Behavior
SELECT 
    CASE 
        WHEN total_spent > 10000 THEN 'High Value'
        WHEN total_spent > 5000 THEN 'Medium Value'
        ELSE 'Low Value'
    END as customer_segment,
    COUNT(*) as customer_count,
    AVG(total_spent) as avg_spent,
    AVG(purchase_count) as avg_purchases
FROM (
    SELECT 
        dc.customer_key,
        COUNT(DISTINCT fs.transaction_id) as purchase_count,
        SUM(fs.total_amount) as total_spent
    FROM fact_sales fs
    JOIN dim_customer dc ON fs.customer_key = dc.customer_key
    WHERE dc.is_current = TRUE
    GROUP BY dc.customer_key
) customer_stats
GROUP BY customer_segment;
