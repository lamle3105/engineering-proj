# Power BI Integration Guide

## Overview

This guide explains how to connect Power BI to the data warehouse created by the data engineering pipeline and build effective reports and dashboards.

## Prerequisites

- Power BI Desktop installed
- Access to the data warehouse (PostgreSQL/MySQL)
- Completed data pipeline execution with curated data

## Connection Setup

### Option 1: Direct Query (Recommended for Real-time Data)

1. **Open Power BI Desktop**

2. **Get Data**
   - Click "Get Data" → "Database" → "PostgreSQL database" (or MySQL)
   - Enter server details:
     - Server: `your-database-host:5432`
     - Database: `datawarehouse`

3. **Authentication**
   - Choose "Database" authentication
   - Enter username and password
   - Click "Connect"

4. **Select Tables**
   - Choose the following tables:
     - `fact_sales`
     - `dim_customer`
     - `dim_product`
     - `dim_date`
     - `dim_location`
   - Or use the view: `vw_sales_analysis`

### Option 2: Import Mode (Better Performance)

Follow the same steps but select "Import" instead of "DirectQuery" mode when prompted.

## Data Model Setup

### Automatic Relationships

Power BI should automatically detect relationships based on key columns:

```
fact_sales (customer_key) → dim_customer (customer_key)
fact_sales (product_key) → dim_product (product_key)
fact_sales (date_key) → dim_date (date_key)
fact_sales (location_key) → dim_location (location_key)
```

### Manual Relationship Configuration

If relationships are not detected:

1. Click "Model" view (left sidebar)
2. Drag and drop to create relationships:
   - `fact_sales[customer_key]` → `dim_customer[customer_key]`
   - `fact_sales[product_key]` → `dim_product[product_key]`
   - `fact_sales[date_key]` → `dim_date[date_key]`
   - `fact_sales[location_key]` → `dim_location[location_key]`

3. Set relationship properties:
   - Cardinality: Many to One (*:1)
   - Cross filter direction: Single
   - Make active: Yes

## Creating Measures

### Sales Metrics

```dax
Total Revenue = SUM(fact_sales[total_amount])

Total Quantity = SUM(fact_sales[quantity])

Average Transaction Value = AVERAGE(fact_sales[total_amount])

Transaction Count = COUNT(fact_sales[transaction_id])

Total Tax = SUM(fact_sales[tax_amount])
```

### Time Intelligence

```dax
Revenue YTD = 
CALCULATE(
    [Total Revenue],
    DATESYTD(dim_date[full_date])
)

Revenue LY = 
CALCULATE(
    [Total Revenue],
    SAMEPERIODLASTYEAR(dim_date[full_date])
)

Revenue Growth = 
DIVIDE(
    [Total Revenue] - [Revenue LY],
    [Revenue LY],
    0
)

Revenue MoM = 
VAR PrevMonth = 
    CALCULATE(
        [Total Revenue],
        DATEADD(dim_date[full_date], -1, MONTH)
    )
RETURN
    DIVIDE([Total Revenue] - PrevMonth, PrevMonth, 0)
```

### Customer Metrics

```dax
Unique Customers = DISTINCTCOUNT(fact_sales[customer_key])

Average Revenue per Customer = 
DIVIDE([Total Revenue], [Unique Customers])

Customer Count = COUNTROWS(dim_customer)
```

### Product Metrics

```dax
Products Sold = DISTINCTCOUNT(fact_sales[product_key])

Average Units per Transaction = 
DIVIDE([Total Quantity], [Transaction Count])
```

## Sample Reports

### 1. Executive Dashboard

**Visualizations:**
- KPI Cards:
  - Total Revenue
  - Total Transactions
  - Unique Customers
  - Average Transaction Value
  
- Line Chart: Revenue by Month
- Bar Chart: Top 10 Products by Revenue
- Map: Revenue by Location
- Pie Chart: Revenue by Category

### 2. Sales Performance Report

**Visualizations:**
- Matrix: Sales by Product Category and Month
- Waterfall Chart: Revenue Growth MoM
- Clustered Bar Chart: Top 10 Customers
- Line and Clustered Column Chart: Revenue and Quantity by Month

### 3. Customer Analysis

**Visualizations:**
- Scatter Chart: Customer Value vs. Purchase Frequency
- Table: Customer Details with Revenue
- Funnel Chart: Customer Segmentation
- Tree Map: Revenue by Customer City

### 4. Product Analysis

**Visualizations:**
- Matrix: Product Performance by Category
- Ribbon Chart: Top Products over Time
- Stacked Bar Chart: Revenue by Brand
- Table: Product Profitability Analysis

## Filters and Slicers

Add slicers for:
- Date Range (from dim_date)
- Product Category (from dim_product)
- Region/State (from dim_location)
- Customer City (from dim_customer)

## Row-Level Security (RLS)

### Setup RLS for Regional Managers

1. **Go to Modeling → Manage Roles**

2. **Create Role: "Regional Manager"**
   ```dax
   [region] = USERNAME()
   ```

3. **Test the Role**
   - Modeling → View as Roles
   - Select "Regional Manager"

## Performance Optimization

### Best Practices

1. **Use Import Mode** when possible for better performance
2. **Remove unnecessary columns** from the model
3. **Disable auto date/time** if using dim_date
4. **Create aggregation tables** for large datasets
5. **Use query folding** with DirectQuery

### Indexes

Ensure these indexes exist in the database:
```sql
CREATE INDEX idx_fact_sales_date ON fact_sales(date_key);
CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_key);
CREATE INDEX idx_fact_sales_product ON fact_sales(product_key);
```

## Scheduled Refresh (Power BI Service)

### Setup

1. **Publish to Power BI Service**
   - File → Publish → Select Workspace

2. **Configure Gateway**
   - Install On-premises Data Gateway
   - Configure connection to database

3. **Schedule Refresh**
   - Dataset settings → Scheduled refresh
   - Set frequency (e.g., daily at 3 AM)

## Common DAX Patterns

### Running Total

```dax
Running Total Revenue = 
CALCULATE(
    [Total Revenue],
    FILTER(
        ALLSELECTED(dim_date),
        dim_date[date_key] <= MAX(dim_date[date_key])
    )
)
```

### Ranking

```dax
Product Revenue Rank = 
RANKX(
    ALLSELECTED(dim_product[product_name]),
    [Total Revenue],
    ,
    DESC,
    DENSE
)
```

### Percentage of Total

```dax
Revenue % of Total = 
DIVIDE(
    [Total Revenue],
    CALCULATE([Total Revenue], ALL(dim_product))
)
```

## Troubleshooting

### Connection Issues

- Check database credentials
- Verify network access to database
- Ensure database service is running
- Check firewall rules

### Performance Issues

- Reduce number of visuals per page
- Use aggregated tables
- Optimize DAX measures
- Consider DirectQuery vs Import trade-offs

### Data Refresh Failures

- Check gateway connectivity
- Verify credentials haven't expired
- Review refresh history logs
- Ensure database is accessible

## Resources

- [Power BI Documentation](https://docs.microsoft.com/en-us/power-bi/)
- [DAX Guide](https://dax.guide/)
- [Power BI Community](https://community.powerbi.com/)

## Support

For issues with the data pipeline or data quality:
- Contact the Data Engineering Team
- Review the pipeline logs in Airflow
- Check the data warehouse for data freshness
