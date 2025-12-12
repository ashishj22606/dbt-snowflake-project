# Multi-Record Logging Structure (Simplified)

## Overview
The logging system has been updated from a **single-record-per-job** approach to a **multi-record simplified** approach, where:
- **1 JOB record** per dbt run
- **N MODEL records** per dbt run (one for each model executed)
- **All records share the same PROCESS_STEP_ID** (the job ID)

This makes it much easier to query and analyze individual model executions while maintaining job-level context.

---

## Table Schema Requirements

### New Columns Required
Add these columns to `PROCESS_EXECUTION_LOG` table:

```sql
ALTER TABLE PROCESS_EXECUTION_LOG 
ADD COLUMN RECORD_TYPE VARCHAR(50);

ALTER TABLE PROCESS_EXECUTION_LOG 
ADD COLUMN MODEL_NAME VARCHAR(500);
```

### Column Definitions
- **PROCESS_STEP_ID**: Same for all records in a job run (format: `JOB_<invocation_id>`)
- **RECORD_TYPE**: Either 'JOB' or 'MODEL'
- **MODEL_NAME**: NULL for JOB records, model name for MODEL records

### Primary Key
Composite key: `(PROCESS_STEP_ID, RECORD_TYPE, MODEL_NAME)`

---

## Record Structure

### JOB Record
**PROCESS_STEP_ID**: `JOB_<invocation_id>`  
**RECORD_TYPE**: `JOB`  
**MODEL_NAME**: `NULL`

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
**PROCESS_STEP_ID**: `JOB_<invocation_id>` (same as JOB record)  
**RECORD_TYPE**: `MODEL`  
**MODEL_NAME**: `<model_name>` (e.g., 'stg_customers')

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
- **Sets**: PROCESS_STEP_ID='JOB_xxx', RECORD_TYPE='JOB', MODEL_NAME=NULL
- **Initializes**: Job-level config, empty timeline

### 2. `log_execution_start` (pre-hook on each model)
- **Action**: INSERT 1 MODEL record per model
- **Sets**: PROCESS_STEP_ID='JOB_xxx' (same as job), RECORD_TYPE='MODEL', MODEL_NAME='model_name'
- **Captures**: Source dependencies, destination details, start time, query ID
- **Timeline**: Adds MODEL_START event

### 3. `log_execution_end` (post-hook on each model)
- **Action**: UPDATE the specific MODEL record
- **WHERE**: PROCESS_STEP_ID='JOB_xxx' AND RECORD_TYPE='MODEL' AND MODEL_NAME='model_name'
- **Updates**: End time, status=SUCCESS, row count, end query ID
- **Timeline**: Adds MODEL_COMPLETE event with query results

### 4. `log_run_end` (on-run-end hook)
- **Action**: UPDATE the JOB record
- **WHERE**: PROCESS_STEP_ID='JOB_xxx' AND RECORD_TYPE='JOB'
- **Aggregates**: Total counts from all MODEL records (WHERE PROCESS_STEP_ID='JOB_xxx' AND RECORD_TYPE='MODEL')
- **Summarizes**: Success/failure counts, total rows
- **Timeline**: Adds JOB_COMPLETE event

---

## Query Examples

### Get all records for a specific job (both JOB and MODEL records)
```sql
SELECT 
    PROCESS_STEP_ID,
    RECORD_TYPE,
    MODEL_NAME,
    EXECUTION_STATUS_NAME,
    EXECUTION_TYPE_NAME,
    EXECUTION_START_TMSTP,
    EXECUTION_END_TMSTP,
    SOURCE_DATA_CNT as row_count
FROM PROCESS_EXECUTION_LOG
WHERE PROCESS_STEP_ID = 'JOB_abc123...'
ORDER BY RECORD_TYPE DESC, EXECUTION_START_TMSTP;  -- JOB first, then MODELs
```

### Get all models for a specific job
```sql
SELECT 
    PROCESS_STEP_ID,
    MODEL_NAME,
    EXECUTION_STATUS_NAME,
    EXECUTION_TYPE_NAME,
    EXECUTION_START_TMSTP,
    EXECUTION_END_TMSTP,
    SOURCE_DATA_CNT as row_count,
    DESTINATION_OBJ:database::string as database,
    DESTINATION_OBJ:schema::string as schema,
    DESTINATION_OBJ:table::string as table
FROM PROCESS_EXECUTION_LOG
WHERE PROCESS_STEP_ID = 'JOB_abc123...'
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
    m.MODEL_NAME,
    f.value:type::string as source_type,
    f.value:name::string as source_name,
    f.value:node_id::string as node_id
FROM PROCESS_EXECUTION_LOG m,
     LATERAL FLATTEN(input => m.SOURCE_OBJ) f
WHERE m.PROCESS_STEP_ID = 'JOB_abc123...'
  AND m.RECORD_TYPE = 'MODEL'
  AND m.MODEL_NAME = 'stg_customers';
```

### Get execution timeline for a model
```sql
SELECT 
    PROCESS_STEP_ID,
    MODEL_NAME,
    t.value:step_number::int as step_number,
    t.value:timestamp::string as timestamp,
    t.value:step_type::string as step_type,
    t.value:title::string as title,
    t.value:query_id::string as query_id,
    t.value:query_result as query_result
FROM PROCESS_EXECUTION_LOG m,
     LATERAL FLATTEN(input => m.STEP_EXECUTION_OBJ:execution_timeline) t
WHERE m.PROCESS_STEP_ID = 'JOB_abc123...'
  AND m.RECORD_TYPE = 'MODEL'
  AND m.MODEL_NAME = 'stg_customers'
ORDER BY t.value:step_number::int;
```

### Get all failed models across all jobs
```sql
SELECT 
    PROCESS_STEP_ID as job_id,
    MODEL_NAME,
    EXECUTION_START_TMSTP,
    EXECUTION_END_TMSTP,
    DESTINATION_OBJ:full_name::string as table_name,
    ERROR_MESSAGE_OBJ
FROM PROCESS_EXECUTION_LOG
WHERE RECORD_TYPE = 'MODEL'
  AND EXECUTION_STATUS_NAME = 'FAILED'
ORDER BY EXECUTION_START_TMSTP DESC;
```

### Get job and all its models (unified view)
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
    WHERE PROCESS_STEP_ID = 'JOB_abc123...' 
      AND RECORD_TYPE = 'MODEL'
)
SELECT 
    j.PROCESS_STEP_ID as job_id,
    j.EXECUTION_STATUS_NAME as job_status,
    j.EXECUTION_START_TMSTP as job_start,
    j.EXECUTION_END_TMSTP as job_end,
    m.MODEL_NAME,
    m.EXECUTION_STATUS_NAME as model_status,
    m.SOURCE_DATA_CNT as model_row_count,
    m.EXECUTION_TYPE_NAME as execution_type
FROM job j
LEFT JOIN models m ON 1=1
ORDER BY m.EXECUTION_START_TMSTP;
```

### Simple query to get everything for a job
```sql
-- This is the beauty of same PROCESS_STEP_ID!
SELECT 
    PROCESS_STEP_ID,
    RECORD_TYPE,
    MODEL_NAME,
    EXECUTION_STATUS_NAME,
    EXECUTION_START_TMSTP,
    EXECUTION_END_TMSTP,
    SOURCE_DATA_CNT,
    EXECUTION_TYPE_NAME
FROM PROCESS_EXECUTION_LOG
WHERE PROCESS_STEP_ID = 'JOB_abc123...'
ORDER BY 
    CASE WHEN RECORD_TYPE = 'JOB' THEN 0 ELSE 1 END,  -- JOB first
    EXECUTION_START_TMSTP;
```

---

## Benefits

### ✅ Easier Querying
- **Single WHERE clause** to get all job data: `WHERE PROCESS_STEP_ID = 'JOB_xxx'`
- Direct column access for model-level data with MODEL_NAME
- Simple filtering: add `AND RECORD_TYPE = 'MODEL'` or `AND RECORD_TYPE = 'JOB'`
- No need to remember complex PROCESS_STEP_ID patterns

### ✅ Better Performance
- Natural grouping by PROCESS_STEP_ID for partition pruning
- Can create index on (PROCESS_STEP_ID, RECORD_TYPE, MODEL_NAME)
- No JSON parsing required for common queries
- Smaller individual records

### ✅ Clearer Semantics
- PROCESS_STEP_ID truly represents the "job run"
- RECORD_TYPE differentiates job summary vs model details
- MODEL_NAME is a direct column (not buried in JSON)
- Intuitive structure: all records with same ID belong together

### ✅ Maintained Lineage
- Full query ID tracking per model
- Complete timeline per model
- Source dependencies preserved per model
- Row counts tracked per model

### ✅ Flexible Analysis
- Get everything: `WHERE PROCESS_STEP_ID = 'JOB_xxx'`
- Get job summary: add `AND RECORD_TYPE = 'JOB'`
- Get all models: add `AND RECORD_TYPE = 'MODEL'`
- Get specific model: add `AND MODEL_NAME = 'xyz'`
- Easy to aggregate across multiple jobs

---

## Migration Notes

If you have existing data in the old single-record format:
1. Add the new columns (RECORD_TYPE, MODEL_NAME)
2. Update existing records: SET RECORD_TYPE='JOB', MODEL_NAME=NULL
3. The new macros will create the new structure going forward
4. Old records remain queryable as JOB records (without MODEL detail records)

### Required DDL
```sql
ALTER TABLE DEV_PROVIDERPDM.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG 
ADD COLUMN RECORD_TYPE VARCHAR(50);

ALTER TABLE DEV_PROVIDERPDM.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG 
ADD COLUMN MODEL_NAME VARCHAR(500);

-- Update existing records
UPDATE DEV_PROVIDERPDM.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG
SET RECORD_TYPE = 'JOB',
    MODEL_NAME = NULL
WHERE RECORD_TYPE IS NULL;
```

### Recommended Index
```sql
CREATE INDEX idx_process_execution_log_lookup 
ON DEV_PROVIDERPDM.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG 
(PROCESS_STEP_ID, RECORD_TYPE, MODEL_NAME);
```
 
ADD COLUMN PARENT_STEP_ID VARCHAR(500);

ALTER TABLE DEV_PROVIDERPDM.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG 
ADD COLUMN RECORD_TYPE VARCHAR(50);


-- Add new columns
ALTER TABLE DEV_PROVIDERPDM.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG 
ADD COLUMN RECORD_TYPE VARCHAR(50);

ALTER TABLE DEV_PROVIDERPDM.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG 
ADD COLUMN MODEL_NAME VARCHAR(500);

-- Recommended index
CREATE INDEX idx_process_execution_log_lookup 
ON DEV_PROVIDERPDM.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG 
(PROCESS_STEP_ID, RECORD_TYPE, MODEL_NAME);