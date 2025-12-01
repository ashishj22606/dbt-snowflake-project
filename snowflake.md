# Snowflake Configuration and Concepts

This document outlines the Snowflake-specific configurations, setup, and concepts used in this dbt project.

## Table of Contents
- [Initial Setup](#initial-setup)
- [Database Objects](#database-objects)
- [Warehouse Configuration](#warehouse-configuration)
- [Data Loading](#data-loading)
- [Security and Access Control](#security-and-access-control)
- [Performance Optimization](#performance-optimization)
- [Monitoring and Maintenance](#monitoring-and-maintenance)

## Initial Setup

Run the following SQL scripts in Snowflake to set up the initial environment:

```sql
-- Create a warehouse for transformation workloads
CREATE WAREHOUSE transforming
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 600  -- 10 minutes
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- Create databases
CREATE DATABASE raw;
CREATE DATABASE analytics;

-- Create schemas for raw data
CREATE SCHEMA raw.jaffle_shop;
CREATE SCHEMA raw.stripe;
```

## Database Objects

### Raw Data Structure

#### Jaffle Shop Data
```sql
-- Customers table
CREATE TABLE raw.jaffle_shop.customers (
  id INTEGER,
  first_name VARCHAR,
  last_name VARCHAR
);

-- Orders table
CREATE TABLE raw.jaffle_shop.orders (
  id INTEGER,
  user_id INTEGER,
  order_date DATE,
  status VARCHAR,
  _etl_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### Stripe Payment Data
```sql
CREATE TABLE raw.stripe.payment (
  id INTEGER,
  orderid INTEGER,
  paymentmethod VARCHAR,
  status VARCHAR,
  amount INTEGER,
  created DATE,
  _batched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Data Loading

### Loading Data from S3

#### Jaffle Shop Customers
```sql
COPY INTO raw.jaffle_shop.customers (id, first_name, last_name)
FROM 's3://dbt-tutorial-public/jaffle_shop_customers.csv'
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
);
```

#### Jaffle Shop Orders
```sql
COPY INTO raw.jaffle_shop.orders (id, user_id, order_date, status)
FROM 's3://dbt-tutorial-public/jaffle_shop_orders.csv'
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
);
```

#### Stripe Payments
```sql
COPY INTO raw.stripe.payment (id, orderid, paymentmethod, status, amount, created)
FROM 's3://dbt-tutorial-public/stripe_payments.csv'
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
);
```

## Warehouse Configuration

### Warehouse Sizing
- **Transforming Warehouse**: XSMALL for development and transformation workloads
- **Scaling**: Configured to auto-suspend after 10 minutes of inactivity

### Query Optimization
- Enable query acceleration for complex analytical queries
- Use result caching for frequently accessed data
- Consider time travel for data recovery

## Security and Access Control

### Best Practices
- Use role-based access control (RBAC)
- Implement least privilege principle
- Secure sensitive data with masking policies
- Enable network policies for IP whitelisting

### Recommended Roles
1. `TRANSFORMER`: For dbt transformations
2. `ANALYST`: For read access to analytics
3. `DEVELOPER`: For development and testing

## Performance Optimization

### Clustering Keys
Consider adding clustering keys to large tables:
```sql
-- Example clustering key for orders
ALTER TABLE analytics.dim_customers CLUSTER BY (customer_id);
```

### Materialized Views
For frequently queried aggregations:
```sql
CREATE MATERIALIZED VIEW analytics.customer_metrics AS
SELECT 
    customer_id,
    COUNT(DISTINCT order_id) as total_orders,
    SUM(amount) as total_spent
FROM analytics.fct_orders
GROUP BY customer_id;
```

## Monitoring and Maintenance

### Query History
```sql
-- View recent queries
SELECT * 
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
ORDER BY start_time DESC
LIMIT 100;
```

### Storage Usage
```sql
-- Check database storage
SELECT * 
FROM INFORMATION_SCHEMA.DATABASE_STORAGE_USAGE
ORDER BY USAGE_DATE DESC;
```

### Warehouse Credit Usage
```sql
-- Monitor warehouse credit consumption
SELECT * 
FROM INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY
ORDER BY START_TIME DESC;
```

## Integration with dbt

### dbt Profile Configuration
```yaml
dbt_fundamentals:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <your-account-identifier>
      user: <your-username>
      password: <your-password>
      role: TRANSFORMER
      database: analytics
      warehouse: transforming
      schema: dbt_<your_username>
      threads: 4
```

### Recommended dbt Materializations
- Staging models: Views
- Marts: Tables
- Large fact tables: Incremental models

## Troubleshooting

### Common Issues
1. **Permission Errors**: Verify role assignments and object privileges
2. **Query Timeouts**: Increase warehouse size or optimize queries
3. **Data Loading Issues**: Check file formats and staging locations
4. **Connectivity Problems**: Verify network policies and credentials

## Resources
- [Snowflake Documentation](https://docs.snowflake.com/)
- [dbt + Snowflake Best Practices](https://docs.getdbt.com/guides/migrating/legacy-databases/snowflake)
- [Snowflake Community](https://community.snowflake.com/)
