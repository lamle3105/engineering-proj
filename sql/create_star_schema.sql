-- Create Customer Dimension Table
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key BIGINT PRIMARY KEY,
    customer_id VARCHAR(50) UNIQUE NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(20),
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(10),
    country VARCHAR(50),
    effective_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_customer_id ON dim_customer(customer_id);
CREATE INDEX idx_dim_customer_current ON dim_customer(is_current);

-- Create Product Dimension Table
CREATE TABLE IF NOT EXISTS dim_product (
    product_key BIGINT PRIMARY KEY,
    product_id VARCHAR(50) UNIQUE NOT NULL,
    product_name VARCHAR(255),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    unit_price DECIMAL(10, 2),
    unit_cost DECIMAL(10, 2),
    effective_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_product_id ON dim_product(product_id);
CREATE INDEX idx_dim_product_category ON dim_product(category);
CREATE INDEX idx_dim_product_current ON dim_product(is_current);

-- Create Date Dimension Table
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year INT,
    quarter INT,
    month INT,
    day INT,
    day_of_week INT,
    week_of_year INT,
    month_name VARCHAR(20),
    day_name VARCHAR(20),
    is_weekend BOOLEAN,
    is_holiday BOOLEAN DEFAULT FALSE
);

CREATE INDEX idx_dim_date_full ON dim_date(full_date);
CREATE INDEX idx_dim_date_year_month ON dim_date(year, month);

-- Create Location Dimension Table
CREATE TABLE IF NOT EXISTS dim_location (
    location_key BIGINT PRIMARY KEY,
    location_id VARCHAR(50) UNIQUE NOT NULL,
    store_name VARCHAR(255),
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(10),
    country VARCHAR(50),
    region VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_location_id ON dim_location(location_id);
CREATE INDEX idx_dim_location_region ON dim_location(region);

-- Create Sales Fact Table
CREATE TABLE IF NOT EXISTS fact_sales (
    sales_key BIGINT PRIMARY KEY,
    customer_key BIGINT,
    product_key BIGINT,
    date_key INT,
    location_key BIGINT,
    transaction_id VARCHAR(100) UNIQUE NOT NULL,
    quantity INT,
    unit_price DECIMAL(10, 2),
    discount DECIMAL(5, 4),
    total_amount DECIMAL(12, 2),
    tax_amount DECIMAL(10, 2),
    transaction_date DATE,
    transaction_timestamp TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (location_key) REFERENCES dim_location(location_key)
);

CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_key);
CREATE INDEX idx_fact_sales_product ON fact_sales(product_key);
CREATE INDEX idx_fact_sales_date ON fact_sales(date_key);
CREATE INDEX idx_fact_sales_location ON fact_sales(location_key);
CREATE INDEX idx_fact_sales_transaction_date ON fact_sales(transaction_date);
CREATE INDEX idx_fact_sales_transaction_id ON fact_sales(transaction_id);

-- Create view for easy reporting
CREATE OR REPLACE VIEW vw_sales_analysis AS
SELECT 
    fs.sales_key,
    fs.transaction_id,
    fs.transaction_date,
    dc.customer_id,
    dc.first_name,
    dc.last_name,
    dc.email,
    dc.city as customer_city,
    dc.state as customer_state,
    dp.product_id,
    dp.product_name,
    dp.category,
    dp.subcategory,
    dp.brand,
    dl.store_name,
    dl.city as store_city,
    dl.region,
    dd.year,
    dd.quarter,
    dd.month,
    dd.month_name,
    fs.quantity,
    fs.unit_price,
    fs.discount,
    fs.total_amount,
    fs.tax_amount
FROM fact_sales fs
LEFT JOIN dim_customer dc ON fs.customer_key = dc.customer_key
LEFT JOIN dim_product dp ON fs.product_key = dp.product_key
LEFT JOIN dim_date dd ON fs.date_key = dd.date_key
LEFT JOIN dim_location dl ON fs.location_key = dl.location_key
WHERE dc.is_current = TRUE AND dp.is_current = TRUE;
