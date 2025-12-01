# dbt Concepts and Best Practices

This document explains the core concepts of dbt (data build tool) and how they're applied in this project.

## Table of Contents
- [Data Modeling in dbt](#data-modeling-in-dbt)
- [Testing Strategies](#testing-strategies)
- [Documentation](#documentation)
- [Materializations](#materializations)
- [Macros and Jinja](#macros-and-jinja)
- [dbt Project Structure](#dbt-project-structure)
- [Best Practices](#best-practices)

## Data Modeling in dbt

dbt uses a modular approach to data modeling with two main layers:

### 1. Staging Layer
- First transformation of raw data
- Performs type casting and basic cleaning
- Naming convention: `stg_<source>_<entity>`
- Example: `stg_jaffle_shop_orders`, `stg_stripe_payments`

### 2. Marts Layer
- Business-ready dimensional models
- Combines data from multiple staging models
- Follows star schema patterns (facts and dimensions)
- Naming convention: `dim_<entity>` for dimensions, `fct_<process>` for facts
- Example: `dim_customers`, `fct_orders`

## Testing Strategies

dbt provides several ways to ensure data quality:

### 1. Built-in Tests
- `not_null`: Ensures no null values
- `unique`: Verifies column values are unique
- `accepted_values`: Validates against a list
- `relationships`: Checks referential integrity

### 2. Custom Tests
- SQL files in the `tests` directory
- Can test complex business logic
- Example: `assert_stg_stripe__payment_total_positive.sql`

### 3. Schema Tests
- Defined in YAML files
- Run using `dbt test`
- Can be generic or model-specific

## Documentation

dbt makes it easy to document your data models:

```yaml
# In schema.yml
version: 2

models:
  - name: dim_customers
    description: "Customer dimension table with enriched data"
    columns:
      - name: customer_id
        description: "Primary key for customers"
        tests:
          - not_null
          - unique
```

Generate and view documentation with:
```bash
dbt docs generate
dbt docs serve
```

## Materializations

dbt supports different materialization strategies:

| Type     | Description                          | When to Use                      |
|----------|--------------------------------------|----------------------------------|
| View     | Creates a view in the database       | Development, frequently changing |
| Table    | Creates a physical table             | Production, performance-critical |
| Incremental | Updates only new/changed data     | Large tables, frequent updates   |
| Ephemeral | In-memory CTE, no database object | Intermediate transformations     |

## Macros and Jinja

dbt uses Jinja templating for dynamic SQL:

```sql
-- Example macro
{% macro cents_to_dollars(column_name, precision=2) %}
  ({{ column_name }} / 100)::numeric(16, {{ precision }})
{% endmacro %}

-- Usage in a model
SELECT
  id,
  {{ cents_to_dollars('amount_cents') }} as amount
FROM {{ ref('stg_payments') }}
```

## dbt Project Structure

```
project/
├── dbt_project.yml   # Project configuration
├── models/           # SQL models
│   ├── staging/     # Raw data transformations
│   └── marts/       # Business-ready models
├── tests/           # Custom data tests
├── macros/          # Reusable SQL components
├── seeds/           # Reference data
└── snapshots/       # Type 2 slowly changing dimensions
```

## Best Practices

1. **Version Control**
   - Track all .sql and .yml files
   - Use descriptive commit messages
   - Document breaking changes

2. **Model Design**
   - Keep models small and focused
   - Use `ref()` for model dependencies
   - Document all models and columns

3. **Performance**
   - Use incremental models for large tables
   - Add appropriate indexes
   - Monitor query performance

4. **Testing**
   - Test critical business logic
   - Add tests for edge cases
   - Run tests in CI/CD pipeline

5. **Documentation**
   - Document all models and columns
   - Keep documentation up-to-date
   - Add examples for complex transformations

## Learning Resources

- [dbt Documentation](https://docs.getdbt.com/)
- [dbt Learn](https://courses.getdbt.com/)
- [dbt Community](https://community.getdbt.com/)
- [dbt Style Guide](https://github.com/dbt-labs/corp/blob/main/dbt_style_guide.md)
