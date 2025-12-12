# Source Dependency Tracking

## Overview
The `SOURCE_OBJ` column in `PROCESS_EXECUTION_LOG` captures **all dbt-managed dependencies** for each model. This includes sources, models, seeds, snapshots, and other dbt node types.

---

## What Gets Captured

### ✅ Automatically Captured (dbt-managed)

All dependencies declared using dbt's `source()` and `ref()` functions:

#### 1. **Sources** (type: `source`)
```sql
-- In your model:
SELECT * FROM {{ source('jaffle_shop', 'customers') }}

-- Captured as:
{
  "source_1": {
    "type": "source",
    "name": "jaffle_shop.customers",
    "node_id": "source.my_project.jaffle_shop.customers"
  }
}
```

#### 2. **Models** (type: `ref`)
```sql
-- In your model:
SELECT * FROM {{ ref('stg_customers') }}

-- Captured as:
{
  "source_1": {
    "type": "ref",
    "name": "stg_customers",
    "node_id": "model.my_project.stg_customers"
  }
}
```

#### 3. **Seeds** (type: `seed`)
```sql
-- In your model:
SELECT * FROM {{ ref('country_codes') }}  -- where country_codes is a seed

-- Captured as:
{
  "source_1": {
    "type": "seed",
    "name": "country_codes",
    "node_id": "seed.my_project.country_codes"
  }
}
```

#### 4. **Snapshots** (type: `snapshot`)
```sql
-- In your model:
SELECT * FROM {{ ref('customers_snapshot') }}  -- where customers_snapshot is a snapshot

-- Captured as:
{
  "source_1": {
    "type": "snapshot",
    "name": "customers_snapshot",
    "node_id": "snapshot.my_project.customers_snapshot"
  }
}
```

#### 5. **Other dbt Node Types**
- **Tests** (type: `test`) - rare, but captured if referenced
- **Analyses** (type: `analysis`) - rare, but captured if referenced
- **Exposures** (type: `exposure`) - rare, but captured if referenced
- **Unknown types** (type: `unknown_*`) - any future dbt node types

---

## What Does NOT Get Captured

### ❌ NOT Automatically Captured

#### 1. **Hardcoded Table References**
```sql
-- These are NOT captured:
SELECT * FROM raw.jaffle_shop.customers
SELECT * FROM analytics.dim_calendar
SELECT * FROM external_database.public.vendors
```

**Solution:** Convert to dbt sources:
```yaml
# In _src_jaffle_shop.yml
sources:
  - name: jaffle_shop
    database: raw
    schema: jaffle_shop
    tables:
      - name: customers
```

Then use: `{{ source('jaffle_shop', 'customers') }}`

#### 2. **External Tables (not in dbt)**
```sql
-- Direct reference NOT captured:
SELECT * FROM external_db.public.third_party_data
```

**Solution:** Add as dbt source (even if you don't manage it):
```yaml
sources:
  - name: external
    database: external_db
    schema: public
    tables:
      - name: third_party_data
```

#### 3. **Dynamic Table Names**
```sql
-- Variable table names NOT captured:
{% set table_name = 'customers_' ~ var('date') %}
SELECT * FROM raw.{{ table_name }}
```

**Solution:** Use dbt sources with variables if possible

#### 4. **System Tables**
```sql
-- Information schema queries NOT captured:
SELECT * FROM information_schema.tables
SELECT * FROM snowflake.account_usage.query_history
```

**Workaround:** These can optionally be declared as sources if tracking is needed

---

## Example: Complete Dependency Capture

### Model SQL (stg_orders.sql)
```sql
WITH 

-- From a dbt source
raw_orders AS (
    SELECT * FROM {{ source('jaffle_shop', 'orders') }}
),

-- From another model
customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

-- From a seed
status_mapping AS (
    SELECT * FROM {{ ref('order_status_mapping') }}
),

-- From a snapshot
customer_history AS (
    SELECT * FROM {{ ref('customers_snapshot') }}
)

SELECT 
    o.*,
    c.customer_name,
    s.status_display_name,
    h.historical_tier
FROM raw_orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
LEFT JOIN status_mapping s ON o.status = s.status_code
LEFT JOIN customer_history h ON o.customer_id = h.customer_id
```

### Resulting SOURCE_OBJ
```json
{
  "source_1": {
    "type": "source",
    "name": "jaffle_shop.orders",
    "node_id": "source.my_project.jaffle_shop.orders"
  },
  "source_2": {
    "type": "ref",
    "name": "stg_customers",
    "node_id": "model.my_project.stg_customers"
  },
  "source_3": {
    "type": "seed",
    "name": "order_status_mapping",
    "node_id": "seed.my_project.order_status_mapping"
  },
  "source_4": {
    "type": "snapshot",
    "name": "customers_snapshot",
    "node_id": "snapshot.my_project.customers_snapshot"
  }
}
```

---

## Query Examples

### Get all source types for a model
```sql
SELECT 
    MODEL_NAME,
    f.key as source_key,
    f.value:type::string as source_type,
    f.value:name::string as source_name,
    f.value:node_id::string as node_id
FROM PROCESS_EXECUTION_LOG,
     LATERAL FLATTEN(input => SOURCE_OBJ) f
WHERE PROCESS_STEP_ID = 'JOB_abc123...'
  AND RECORD_TYPE = 'MODEL'
  AND MODEL_NAME = 'stg_orders';
```

### Count dependencies by type across all models
```sql
SELECT 
    f.value:type::string as dependency_type,
    COUNT(*) as count
FROM PROCESS_EXECUTION_LOG,
     LATERAL FLATTEN(input => SOURCE_OBJ) f
WHERE PROCESS_STEP_ID = 'JOB_abc123...'
  AND RECORD_TYPE = 'MODEL'
GROUP BY dependency_type
ORDER BY count DESC;
```

### Find models that depend on seeds
```sql
SELECT 
    MODEL_NAME,
    f.value:name::string as seed_name
FROM PROCESS_EXECUTION_LOG,
     LATERAL FLATTEN(input => SOURCE_OBJ) f
WHERE PROCESS_STEP_ID = 'JOB_abc123...'
  AND RECORD_TYPE = 'MODEL'
  AND f.value:type::string = 'seed';
```

### Find models that depend on snapshots
```sql
SELECT 
    MODEL_NAME,
    f.value:name::string as snapshot_name
FROM PROCESS_EXECUTION_LOG,
     LATERAL FLATTEN(input => SOURCE_OBJ) f
WHERE PROCESS_STEP_ID = 'JOB_abc123...'
  AND RECORD_TYPE = 'MODEL'
  AND f.value:type::string = 'snapshot';
```

---

## Best Practices

### ✅ DO:
1. **Use dbt sources** for all external tables
2. **Use ref()** for all model dependencies
3. **Declare seeds** in `seeds/` folder
4. **Use snapshots** for SCD tracking

### ❌ AVOID:
1. Hardcoded table names (use sources instead)
2. Dynamic table names (makes dependency tracking impossible)
3. Direct database references without source definitions

---

## Troubleshooting

### "My source is not showing up in SOURCE_OBJ"

Check:
1. Is it referenced using `{{ source() }}` or `{{ ref() }}`?
2. Is the source declared in a `_src_*.yml` file?
3. Is the model actually being used (not commented out)?
4. Run `dbt ls --select +your_model` to see what dbt thinks the dependencies are

### "I see 'unknown_*' type in SOURCE_OBJ"

This means dbt has a new node type we haven't explicitly handled. The dependency is still captured - just check what the `node_id` prefix is and we can add explicit support for it.

---

## Summary

**What gets captured:**
- ✅ All `source()` references
- ✅ All `ref()` references (models, seeds, snapshots)
- ✅ Test dependencies (if any)
- ✅ Analysis dependencies (if any)
- ✅ Any other dbt-managed node types

**What doesn't get captured:**
- ❌ Hardcoded table names
- ❌ External tables not declared as sources
- ❌ Dynamic table references
- ❌ System/metadata tables

**Solution:** Convert everything to dbt sources and refs for complete lineage tracking! 🎯
