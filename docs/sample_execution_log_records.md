# Process Execution Log - Sample Records

This document shows how records will appear in the `DEV_PROVIDERPDM.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG` table after running dbt.

---

## Key Design: 1 Record Per Job

**All models within a single dbt run are captured in ONE record.** The `STEP_EXECUTION_OBJ` column contains a JSON array with details for each model, and `DESTINATION_DATA_CNT_OBJ` contains summary counts.

---

## Scenario: Running 3 Models

Assume you run `dbt run` with 3 models:
- `stg_jaffle_shop__customers`
- `stg_jaffle_shop__orders`
- `dim_customers`

You will get **1 record** that continuously updates as each model executes.

---

## The Single Record (Showing Progression)

### Basic Columns

| Column | At Job Start | After All Models Complete |
|--------|--------------|---------------------------|
| PROCESS_EXECUTION_LOG_SK | 1 (auto) | 1 |
| PROCESS_CONFIG_SK | NULL | NULL |
| PROCESS_STEP_ID | `JOB_abc123-def456-ghi789` | `JOB_abc123-def456-ghi789` |
| EXECUTION_STATUS_NAME | `RUNNING` | `SUCCESS` |
| EXECUTION_COMPLETED_IND | `N` | `Y` |
| EXECUTION_START_TMSTP | `2025-12-10 10:00:00.000` | `2025-12-10 10:00:00.000` |
| EXECUTION_END_TMSTP | NULL | `2025-12-10 10:05:30.000` |
| EXECUTION_TYPE_NAME | `DBT_JOB_RUN` | `DBT_JOB_RUN` |
| EXTRACT_START_TMSTP | `2025-12-10 10:00:00.000` | `2025-12-10 10:00:00.000` |
| EXTRACT_END_TMSTP | NULL | `2025-12-10 10:05:30.000` |
| INSERT_TMSTP | `2025-12-10 10:00:00.000` | `2025-12-10 10:00:00.000` |
| UPDATE_TMSTP | `2025-12-10 10:00:00.000` | `2025-12-10 10:05:30.000` |
| DELETED_IND | `N` | `N` |

---

### SOURCE_OBJ (VARIANT)

```json
{
    "type": "DBT_JOB",
    "project_name": "dbt_fundamentals"
}
```

---

### DESTINATION_OBJ (VARIANT)

```json
{
    "target_name": "dev",
    "target_schema": "dbt_ajain"
}
```

---

### PROCESS_CONFIG_OBJ (VARIANT)

```json
{
    "invocation_id": "abc123-def456-ghi789",
    "project_name": "dbt_fundamentals",
    "target_name": "dev"
}
```

---

### DESTINATION_DATA_CNT_OBJ (VARIANT) - Summary Counts

| State | Value |
|-------|-------|
| At Job Start | `{"total_models": 0, "completed": 0, "failed": 0, "running": 0}` |
| Model 1 Started | `{"total_models": 1, "completed": 0, "failed": 0, "running": 1}` |
| Model 1 Completed | `{"total_models": 1, "completed": 1, "failed": 0, "running": 0}` |
| Model 2 Started | `{"total_models": 2, "completed": 1, "failed": 0, "running": 1}` |
| Model 2 Completed | `{"total_models": 2, "completed": 2, "failed": 0, "running": 0}` |
| Model 3 Started | `{"total_models": 3, "completed": 2, "failed": 0, "running": 1}` |
| Model 3 Completed | `{"total_models": 3, "completed": 3, "failed": 0, "running": 0}` |

---

### STEP_EXECUTION_OBJ (VARIANT) - All Model Details

This is where all model execution details are stored as a JSON object with a `models` array:

```json
{
    "current_step": "JOB_COMPLETED",
    "summary": {
        "total": 0,
        "success": 0,
        "error": 0,
        "running": 0
    },
    "models": [
        {
            "model_name": "stg_jaffle_shop__customers",
            "database": "DEV_DB",
            "schema": "dbt_ajain",
            "status": "SUCCESS",
            "start_time": "2025-12-10 10:00:01.000",
            "end_time": "2025-12-10 10:01:15.000",
            "duration_seconds": 74,
            "error": null
        },
        {
            "model_name": "stg_jaffle_shop__orders",
            "database": "DEV_DB",
            "schema": "dbt_ajain",
            "status": "SUCCESS",
            "start_time": "2025-12-10 10:01:16.000",
            "end_time": "2025-12-10 10:02:30.000",
            "duration_seconds": 74,
            "error": null
        },
        {
            "model_name": "dim_customers",
            "database": "DEV_DB",
            "schema": "dbt_ajain",
            "status": "SUCCESS",
            "start_time": "2025-12-10 10:02:31.000",
            "end_time": "2025-12-10 10:05:25.000",
            "duration_seconds": 174,
            "error": null
        }
    ]
}
```

---

### ERROR_MESSAGE_OBJ (VARIANT)

Empty array when no errors:
```json
[]
```

---

## Querying the Log

### Get all model details from a job:

```sql
SELECT 
    PROCESS_STEP_ID,
    f.value:model_name::STRING AS model_name,
    f.value:status::STRING AS status,
    f.value:start_time::TIMESTAMP_NTZ AS start_time,
    f.value:end_time::TIMESTAMP_NTZ AS end_time,
    f.value:duration_seconds::INT AS duration_seconds
FROM DEV_PROVIDERPDM.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG,
LATERAL FLATTEN(input => STEP_EXECUTION_OBJ:models) f
WHERE PROCESS_STEP_ID = 'JOB_abc123-def456-ghi789';
```

### Get job summary:

```sql
SELECT 
    PROCESS_STEP_ID,
    EXECUTION_STATUS_NAME,
    EXECUTION_START_TMSTP,
    EXECUTION_END_TMSTP,
    TIMESTAMPDIFF(SECOND, EXECUTION_START_TMSTP, EXECUTION_END_TMSTP) AS total_duration_seconds,
    DESTINATION_DATA_CNT_OBJ:total_models::INT AS total_models,
    DESTINATION_DATA_CNT_OBJ:completed::INT AS completed_models,
    DESTINATION_DATA_CNT_OBJ:failed::INT AS failed_models
FROM DEV_PROVIDERPDM.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG
WHERE EXECUTION_TYPE_NAME = 'DBT_JOB_RUN'
ORDER BY INSERT_TMSTP DESC;
```

---

## Key Benefits

1. **Single Record Per Job**: Easy to track and query
2. **Real-Time Updates**: Record updates as each model executes
3. **All Details in One Place**: `STEP_EXECUTION_OBJ.models` contains all model info
4. **Summary Counts**: `DESTINATION_DATA_CNT_OBJ` shows progress at a glance
5. **Queryable**: Use Snowflake's FLATTEN to extract individual model details
| `stg_jaffle_shop__customers_abc123...` | `DBT_MODEL_RUN` | SUCCESS | 1m 14s |
| `stg_jaffle_shop__orders_abc123...` | `DBT_MODEL_RUN` | SUCCESS | 1m 14s |
| `dim_customers_abc123...` | `DBT_MODEL_RUN` | SUCCESS | 2m 54s |

---

## Key Points

1. **Unique Identifier**: Each record uses `model_name + invocation_id` as `PROCESS_STEP_ID` to ensure uniqueness per run.

2. **Real-Time Updates**: Records are inserted at START with `EXECUTION_STATUS_NAME = 'RUNNING'` and updated at END with `SUCCESS`.

3. **Snowflake Server Time**: All timestamps use `CURRENT_TIMESTAMP()` from Snowflake.

4. **VARIANT Columns**: JSON objects are stored in VARIANT columns for flexible metadata storage.

5. **1 Record Per Model Per Run**: The `invocation_id` ensures that each dbt run creates new records, not duplicates.
