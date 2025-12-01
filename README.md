# dbt Fundamentals with Snowflake

This project serves as a learning exercise for dbt (data build tool) and Snowflake, focusing on core concepts and best practices for modern data transformation.

## Documentation

Included is comprehensive documentation to help understand both the technical implementation and conceptual foundations:

### Conceptual Guides
- **[dbt Concepts & Best Practices](./dbt.md)**: Detailed explanations of dbt concepts including:
  - Data modeling approaches
  - Testing strategies
  - Documentation practices
  - Materialization options
  - Project structure and best practices

- **[Snowflake Configuration & Concepts](./snowflake.md)**: Snowflake-specific setup and optimizations including:
  - Database and warehouse setup
  - Data loading from S3
  - Performance optimization
  - Security and access control
  - Monitoring and maintenance

### Project-Specific Documentation

In addition to the conceptual guides, this README provides project-specific implementation details below.

## Project Structure

```
dbt_fundamentals/
├── models/
│   ├── marts/               # Final data models for analytics
│   │   └── dim_customers.sql
│   └── staging/             # Source data transformations
│       ├── jaffle_shop/     # Example e-commerce data
│       └── stripe/          # Payment processing data
├── tests/                   # Data quality tests
│   └── assert_stg_stripe__payment_total_positive.sql
├── dbt_project.yml          # Project configuration
└── README.md
```

## Key Concepts Demonstrated

### 1. Data Modeling
- **Staging Layer**: Raw data transformations and type casting
- **Marts Layer**: Business-ready dimensional models
- **Materialization Strategies**:
  - Views for staging models (faster development)
  - Tables for marts (better query performance)

### 2. Testing
- **Data Quality Tests**:
  - Asserting payment totals are positive
  - (Add more tests as you implement them)

### 3. dbt Commands
```bash
# Run models
dbt run

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve

# Run specific models
dbt run --models staging.*
dbt run --models marts.*
```

### 4. Snowflake Integration
- Snowflake as the data warehouse
- Role-based access control
- Warehouse configuration
- Query optimization techniques

## Getting Started

1. **Prerequisites**:
   - dbt Core installed
   - Snowflake account with proper credentials
   - Python 3.7+

2. **Setup**:
   ```bash
   # Install dependencies
   pip install -r requirements.txt
   
   # Install dbt packages
   dbt deps
   
   # Configure your profiles.yml with Snowflake credentials
   ```

3. **Running the Project**:
   ```bash
   # Run all models
   dbt run
   
   # Run tests
   dbt test
   ```

## Learning Resources

### dbt
- [dbt Documentation](https://docs.getdbt.com/docs/introduction)
- [dbt Community](https://getdbt.com/community)
- [dbt Learn](https://courses.getdbt.com/)

### Snowflake
- [Snowflake Documentation](https://docs.snowflake.com/)
- [Snowflake Community](https://community.snowflake.com/)
- [Snowflake University](https://learn.snowflake.com/)

## Next Steps

1. Add more data sources to the staging layer
2. Implement incremental models for large datasets
3. Set up CI/CD for automated testing and deployment
4. Add more comprehensive documentation
5. Implement data quality monitoring
