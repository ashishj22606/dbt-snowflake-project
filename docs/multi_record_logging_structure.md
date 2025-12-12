# Multi-Record Logging Structure (Hierarchical)

## Overview
The logging system has been updated from a **single-record-per-job** approach to a **multi-record hierarchical** approach, where:
- **1 JOB record** per dbt run
- **N MODEL records** per dbt run (one for each model executed)

This makes it much easier to query and analyze individual model executions while maintaining job-level context.

---

## Table Schema Requirements

### New Columns Required
Add these columns to `PROCESS_EXECUTION_LOG` table:

```sql
ALTER TABLE PROCESS_EXECUTION_LOG 
ADD COLUMN PARENT_STEP_ID VARCHAR(500);

ALTER TABLE PROCESS_EXECUTION_LOG 
ADD COLUMN RECORD_TYPE VARCHAR(50);
```

### Column Definitions
- **PARENT_STEP_ID**: For MODEL records, contains the JOB's PROCESS_STEP_ID. For JOB records, is NULL.
- **RECORD_TYPE**: Either 'JOB' or 'MODEL'

---

## Record Structure

### JOB Record
**PROCESS_STEP_ID**: `JOB_<invocation_id>`  
**PARENT_STEP_ID**: `NULL`  
**RECORD_TYPE**: `JOB`

**Contains:**
- Job-level metadata (target, warehouse, threads, dbt version)
- Overall job status (SUCCESS/FAILED)
- Job start and end timestamps
- Summary counts (total models, successful, failed, skipped)
- Total row count across all models
- Job-level timeline (JOB_START, JOB_COMPLETE)
- Error summary

**Key Columns:**
- `PROCESS_CONFIG_OBJ`: Job configuration details
- `SOURCE_DATA_CNT`: Sum of all model row counts
- `DESTINATION_DATA_CNT_OBJ`: Summary object with counts
- `STEP_EXECUTION_OBJ`: Job timeline and summary
- `ERROR_MESSAGE_OBJ`: Aggregated error details

---

### MODEL Records
**PROCESS_STEP_ID**: `JOB_<invocation_id>_MODEL_<model_name>`  
**PARENT_STEP_ID**: `JOB_<invocation_id>`  
**RECORD_TYPE**: `MODEL`

**Contains (per model):**
- Model-specific source dependencies
- Model destination details (database, schema, table, materialization)
- Execution type (INCREMENTAL_LOAD, TRUNCATE_FULL_LOAD, etc.)
- Model start and end timestamps
- Model row count
- Model-specific timeline (MODEL_START, MODEL_COMPLETE)
- Query IDs (start and end)
- Model status (SUCCESS/FAILED)

**Key Columns:**
- `SOURCE_OBJ`: JSON with source dependencies for this model only
  ```json
  {
    "source_1": {"type": "source", "name": "jaffle_shop.customers", "node_id": "..."},
    "source_2": {"type": "ref", "name": "stg_orders", "node_id": "..."}
  }
  ```
- `DESTINATION_OBJ`: Destination details for this model
  ```json
  {
    "database": "DEV_DB",
    "schema": "STAGING",
    "table": "stg_customers",
    "materialization": "view",
    "execution_type": "VIEW_REFRESH",
    "full_name": "DEV_DB.STAGING.stg_customers"
  }
  ```
- `SOURCE_DATA_CNT`: Row count for this model
- `EXECUTION_TYPE_NAME`: INCREMENTAL_LOAD, TRUNCATE_FULL_LOAD, VIEW_REFRESH, etc.
- `STEP_EXECUTION_OBJ`: Model-specific timeline
  ```json
  {
    "model_name": "stg_customers",
    "current_step": "MODEL_COMPLETED",
    "query_id_start": "01abc123-...",
    "query_id_end": "01abc456-...",
    "execution_timeline": [
      {
        "step_number": 1,
        "timestamp": "2025-12-11 10:30:00.123",
        "level": "Info",
        "step_type": "MODEL_START",
        "title": "Model Started: stg_customers",
        "query_id": "01abc123-...",
        "content": {...}
      },
      {
        "step_number": 2,
        "timestamp": "2025-12-11 10:30:05.456",
        "level": "Info",
        "step_type": "MODEL_COMPLETE",
        "title": "Model Completed: stg_customers",
        "query_id": "01abc456-...",
        "query_result": {
          "rows_in_destination": 1000,
          "execution_status": "SUCCESS"
        },
        "content": {...}
      }
    ]
  }
  ```

---

## Macro Flow

### 1. `log_run_start` (on-run-start hook)
- **Action**: INSERT 1 JOB record
- **Sets**: RECORD_TYPE='JOB', PARENT_STEP_ID=NULL
- **Initializes**: Job-level config, empty timeline

### 2. `log_execution_start` (pre-hook on each model)
- **Action**: INSERT 1 MODEL record per model
- **Sets**: RECORD_TYPE='MODEL', PARENT_STEP_ID=JOB_ID
- **Captures**: Source dependencies, destination details, start time, query ID
- **Timeline**: Adds MODEL_START event

### 3. `log_execution_end` (post-hook on each model)
- **Action**: UPDATE the specific MODEL record
- **Updates**: End time, status=SUCCESS, row count, end query ID
- **Timeline**: Adds MODEL_COMPLETE event with query results

### 4. `log_run_end` (on-run-end hook)
- **Action**: UPDATE the JOB record
- **Aggregates**: Total counts from all MODEL records
- **Summarizes**: Success/failure counts, total rows
- **Timeline**: Adds JOB_COMPLETE event

---

## Query Examples

### Get all models for a specific job
```sql
SELECT 
    PROCESS_STEP_ID,
    EXECUTION_STATUS_NAME,
    EXECUTION_TYPE_NAME,
    EXECUTION_START_TMSTP,
    EXECUTION_END_TMSTP,
    SOURCE_DATA_CNT as row_count,
    STEP_EXECUTION_OBJ:model_name::string as model_name,
    DESTINATION_OBJ:database::string as database,
    DESTINATION_OBJ:schema::string as schema,
    DESTINATION_OBJ:table::string as table
FROM PROCESS_EXECUTION_LOG
WHERE PARENT_STEP_ID = 'JOB_abc123...'
  AND RECORD_TYPE = 'MODEL'
ORDER BY EXECUTION_START_TMSTP;
```

### Get job summary
```sql
SELECT 
    PROCESS_STEP_ID,
    EXECUTION_STATUS_NAME,
    EXECUTION_START_TMSTP,
    EXECUTION_END_TMSTP,
    SOURCE_DATA_CNT as total_rows,
    STEP_EXECUTION_OBJ:job_status::string as job_status,
    DESTINATION_DATA_CNT_OBJ:total_models::int as total_models,
    DESTINATION_DATA_CNT_OBJ:successful_models::int as successful,
    DESTINATION_DATA_CNT_OBJ:failed_models::int as failed
FROM PROCESS_EXECUTION_LOG
WHERE PROCESS_STEP_ID = 'JOB_abc123...'
  AND RECORD_TYPE = 'JOB';
```

### Get model lineage (sources)
```sql
SELECT 
    m.PROCESS_STEP_ID,
    m.STEP_EXECUTION_OBJ:model_name::string as model_name,
    f.value:type::string as source_type,
    f.value:name::string as source_name,
    f.value:node_id::string as node_id
FROM PROCESS_EXECUTION_LOG m,
     LATERAL FLATTEN(input => m.SOURCE_OBJ) f
WHERE m.PARENT_STEP_ID = 'JOB_abc123...'
  AND m.RECORD_TYPE = 'MODEL'
  AND m.STEP_EXECUTION_OBJ:model_name::string = 'stg_customers';
```

### Get execution timeline for a model
```sql
SELECT 
    PROCESS_STEP_ID,
    STEP_EXECUTION_OBJ:model_name::string as model_name,
    t.value:step_number::int as step_number,
    t.value:timestamp::string as timestamp,
    t.value:step_type::string as step_type,
    t.value:title::string as title,
    t.value:query_id::string as query_id,
    t.value:query_result as query_result
FROM PROCESS_EXECUTION_LOG m,
     LATERAL FLATTEN(input => m.STEP_EXECUTION_OBJ:execution_timeline) t
WHERE m.PARENT_STEP_ID = 'JOB_abc123...'
  AND m.RECORD_TYPE = 'MODEL'
  AND m.STEP_EXECUTION_OBJ:model_name::string = 'stg_customers'
ORDER BY t.value:step_number::int;
```

### Get all failed models across all jobs
```sql
SELECT 
    PARENT_STEP_ID as job_id,
    STEP_EXECUTION_OBJ:model_name::string as model_name,
    EXECUTION_START_TMSTP,
    EXECUTION_END_TMSTP,
    DESTINATION_OBJ:full_name::string as table_name,
    ERROR_MESSAGE_OBJ
FROM PROCESS_EXECUTION_LOG
WHERE RECORD_TYPE = 'MODEL'
  AND EXECUTION_STATUS_NAME = 'FAILED'
ORDER BY EXECUTION_START_TMSTP DESC;
```

### Get job and all its models (hierarchical view)
```sql
WITH job AS (
    SELECT * 
    FROM PROCESS_EXECUTION_LOG 
    WHERE PROCESS_STEP_ID = 'JOB_abc123...' 
      AND RECORD_TYPE = 'JOB'
),
models AS (
    SELECT * 
    FROM PROCESS_EXECUTION_LOG 
    WHERE PARENT_STEP_ID = 'JOB_abc123...' 
      AND RECORD_TYPE = 'MODEL'
)
SELECT 
    j.PROCESS_STEP_ID as job_id,
    j.EXECUTION_STATUS_NAME as job_status,
    j.EXECUTION_START_TMSTP as job_start,
    j.EXECUTION_END_TMSTP as job_end,
    m.PROCESS_STEP_ID as model_id,
    m.STEP_EXECUTION_OBJ:model_name::string as model_name,
    m.EXECUTION_STATUS_NAME as model_status,
    m.SOURCE_DATA_CNT as model_row_count,
    m.EXECUTION_TYPE_NAME as execution_type
FROM job j
LEFT JOIN models m ON 1=1
ORDER BY m.EXECUTION_START_TMSTP;
```

---

## Benefits

### ✅ Easier Querying
- No need to flatten complex JSON arrays for basic queries
- Direct column access for model-level data
- Simple WHERE clauses to filter by model or job

### ✅ Better Performance
- Indexes can be created on PARENT_STEP_ID for fast joins
- No JSON parsing required for common queries
- Smaller individual records

### ✅ Clearer Separation
- Job-level vs Model-level data clearly separated
- Each record has focused, relevant information
- Easier to understand table structure

### ✅ Maintained Lineage
- Full query ID tracking per model
- Complete timeline per model
- Source dependencies preserved per model
- Row counts tracked per model

### ✅ Flexible Analysis
- Can analyze at job level (summary)
- Can analyze at model level (details)
- Can join job and model records for full context
- Easy to aggregate across multiple jobs

---

## Migration Notes

If you have existing data in the old single-record format:
1. Add the new columns (PARENT_STEP_ID, RECORD_TYPE)
2. Update existing records: SET RECORD_TYPE='JOB', PARENT_STEP_ID=NULL
3. The new macros will create the new structure going forward
4. Old records remain queryable as JOB records (without MODEL detail records)


ALTER TABLE DEV_PROVIDERPDM.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG 
ADD COLUMN PARENT_STEP_ID VARCHAR(500);

ALTER TABLE DEV_PROVIDERPDM.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG 
ADD COLUMN RECORD_TYPE VARCHAR(50);