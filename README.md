# 📦 GourmetFast Analytics — dbt Project

## Overview
This dbt project transforms raw e-commerce CSVs into a clean, analytics-ready data model for **GourmetFast**, an online specialty food delivery company.

The goal is to support key business questions:

- **Who are our most valuable customers?**
- **Which products are selling the most?**
- **How are our sales trending over time?**

The project follows modern dbt best practices: sources, staging, dimensional models, and fact tables, and data quality tests.

## Core Layer (dim_*, fct_*)
### Dimensions
- `dim_customers` and `dim_products` are incremental tables
- Update/Insert if upstream attributes change
- Stored as tables for fast downstream querying

### Fact Tables
- `fct_orders` is an incremental fact model at the order level
- Includes normalized order status, quantity, pricing, and computed revenue
- Uses an incremental filter on order_date to load only new data
- Joins product pricing to compute line-item revenue

The core layer enables analysis of customers, products, and revenue over time.

## Answering the Business Questions
1. Most valuable customers
```
SELECT customer_id, SUM(revenue) AS total_spend
FROM fct_orders
GROUP BY 1
ORDER BY total_spend DESC;
```

2. Top-selling products
```
SELECT product_id, SUM(quantity) AS total_qty
FROM fct_orders
GROUP BY 1
ORDER BY total_qty DESC;
```

3. Sales trending over time
```
SELECT order_date, SUM(revenue) AS daily_revenue
FROM fct_orders
GROUP BY order_date
ORDER BY order_date;   
``` 
