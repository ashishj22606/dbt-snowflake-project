# SOURCE_OBJ Structure

## Overview
The `SOURCE_OBJ` column now captures all source dependencies for each model in the dbt run, organized by model name.

## Structure

```json
{
  "model_name_1": {
    "source_1": {
      "type": "source",
      "name": "jaffle_shop.customers",
      "node_id": "source.dbt_fundamentals.jaffle_shop.customers"
    },
    "source_2": {
      "type": "source",
      "name": "jaffle_shop.orders",
      "node_id": "source.dbt_fundamentals.jaffle_shop.orders"
    }
  },
  "model_name_2": {
    "source_1": {
      "type": "ref",
      "name": "stg_customers",
      "node_id": "model.dbt_fundamentals.stg_customers"
    },
    "source_2": {
      "type": "ref",
      "name": "stg_orders",
      "node_id": "model.dbt_fundamentals.stg_orders"
    },
    "source_3": {
      "type": "source",
      "name": "stripe.payments",
      "node_id": "source.dbt_fundamentals.stripe.payments"
    }
  }
}
```

## Field Descriptions

### Top Level
- **Key**: Model name (e.g., `dim_customers`, `stg_orders`)
- **Value**: Object containing all sources for that model

### Source Entry
Each source is numbered sequentially (`source_1`, `source_2`, etc.) and contains:

- **type**: Either `"source"` or `"ref"`
  - `source`: References a dbt source (from `sources.yml`)
  - `ref`: References another dbt model
  
- **name**: Human-readable name
  - For sources: `"schema_name.table_name"` (e.g., `"jaffle_shop.customers"`)
  - For refs: `"model_name"` (e.g., `"stg_customers"`)
  
- **node_id**: Full dbt node identifier for detailed tracking
  - Format: `node_type.project.schema.name`

## Example Queries

### Get all sources for a specific model
```sql
SELECT 
    pel.PROCESS_STEP_ID,
    pel.SOURCE_OBJ:dim_customers as dim_customers_sources
FROM PROCESS_EXECUTION_LOG pel
WHERE pel.PROCESS_STEP_ID = 'JOB_abc123';
```

### Count sources per model
```sql
SELECT 
    model_key,
    OBJECT_KEYS(model_sources.value) as source_keys,
    ARRAY_SIZE(OBJECT_KEYS(model_sources.value)) as source_count
FROM PROCESS_EXECUTION_LOG pel,
LATERAL FLATTEN(input => pel.SOURCE_OBJ) model_sources
WHERE pel.PROCESS_STEP_ID = 'JOB_abc123';
```

### Get all source tables used across all models
```sql
SELECT DISTINCT
    source_entry.value:type::string as source_type,
    source_entry.value:name::string as source_name,
    source_entry.value:node_id::string as node_id
FROM PROCESS_EXECUTION_LOG pel,
LATERAL FLATTEN(input => pel.SOURCE_OBJ) model_sources,
LATERAL FLATTEN(input => model_sources.value) source_entry
WHERE pel.PROCESS_STEP_ID = 'JOB_abc123'
ORDER BY source_type, source_name;
```

### Get lineage: which models depend on a specific source
```sql
SELECT 
    model_sources.key as model_name,
    source_entry.key as source_key,
    source_entry.value:type::string as source_type,
    source_entry.value:name::string as source_name
FROM PROCESS_EXECUTION_LOG pel,
LATERAL FLATTEN(input => pel.SOURCE_OBJ) model_sources,
LATERAL FLATTEN(input => model_sources.value) source_entry
WHERE pel.PROCESS_STEP_ID = 'JOB_abc123'
  AND source_entry.value:name::string LIKE '%customers%'
ORDER BY model_name;
```

## How It's Populated

1. **Job Start** (`log_run_start`): Initializes `SOURCE_OBJ` as empty object `{}`
2. **Model Start** (`log_execution_start`): For each model that starts:
   - Reads `model.depends_on.nodes` to get all dependencies
   - Parses node IDs to extract source/ref information
   - Adds model entry with numbered sources to `SOURCE_OBJ`
3. **Job End** (`log_run_end`): No changes to `SOURCE_OBJ`

## Benefits

✅ **Easy to read**: Clear structure with numbered sources per model
✅ **Complete lineage**: Tracks both source tables and model references
✅ **Queryable**: Use Snowflake's FLATTEN to analyze dependencies
✅ **Unique identifiers**: Node IDs provide full traceability


ALTER TABLE IF EXISTS DEV_PROVIDERPDM.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG
ADD COLUMN IF NOT EXISTS ROWS_PRODUCED NUMBER,
ADD COLUMN IF NOT EXISTS ROWS_INSERTED NUMBER,
ADD COLUMN IF NOT EXISTS ROWS_UPDATED NUMBER,
ADD COLUMN IF NOT EXISTS ROWS_DELETED NUMBER,
ADD COLUMN IF NOT EXISTS ROWS_WRITTEN_TO_RESULT NUMBER;