# Process Execution Log - Sample Records

This document shows how records will appear in the `DEV_PROVIDERPDM.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG` table after running dbt.

---

## Scenario: Running 3 Models

Assume you run `dbt run` with 3 models:
- `stg_jaffle_shop__customers`
- `stg_jaffle_shop__orders`
- `dim_customers`

You will get **4 records** in total (1 job + 3 models).

---

## Record 1: Job-Level Record (on-run-start / on-run-end)

| Column | Value (Start) | Value (End) |
|--------|---------------|-------------|
| PROCESS_EXECUTION_LOG_SK | 1 (auto) | 1 |
| PROCESS_CONFIG_SK | NULL | NULL |
| PROCESS_STEP_ID | `JOB_abc123-def456-ghi789` | `JOB_abc123-def456-ghi789` |
| EXECUTION_STATUS_NAME | `RUNNING` | `SUCCESS` |
| EXECUTION_COMPLETED_IND | `N` | `Y` |
| EXECUTION_START_TWSTP | `2025-12-10 10:00:00.000` | `2025-12-10 10:00:00.000` |
| EXECUTION_END_TWSTP | NULL | `2025-12-10 10:05:30.000` |
| SOURCE_OBJ | `{"type": "DBT_JOB", "project_name": "dbt_fundamentals"}` | (unchanged) |
| DESTINATION_OBJ | `{"target_name": "dev", "target_schema": "dbt_ajain"}` | (unchanged) |
| PROCESS_CONFIG_OBJ | `{"invocation_id": "abc123-def456-ghi789", "project_name": "dbt_fundamentals", "target_name": "dev"}` | (unchanged) |
| SOURCE_DATA_CNT | NULL | NULL |
| DESTINATION_DATA_CNT_OBJ | NULL | NULL |
| EXECUTION_TYPE_NAME | `DBT_JOB_RUN` | `DBT_JOB_RUN` |
| EXTRACT_START_TWSTP | `2025-12-10 10:00:00.000` | `2025-12-10 10:00:00.000` |
| EXTRACT_END_TWSTP | NULL | `2025-12-10 10:05:30.000` |
| ERROR_MESSAGE_OBJ | NULL | NULL |
| STEP_EXECUTION_OBJ | `{"step": "JOB_START", "type": "FULL_RUN"}` | `{"step": "JOB_END", "type": "FULL_RUN", "status": "SUCCESS"}` |
| INSERT_TWSTP | `2025-12-10 10:00:00.000` | `2025-12-10 10:00:00.000` |
| UPDATE_TWSTP | `2025-12-10 10:00:00.000` | `2025-12-10 10:05:30.000` |
| DELETED_IND | `N` | `N` |

---

## Record 2: Model-Level Record (stg_jaffle_shop__customers)

| Column | Value (Start) | Value (End) |
|--------|---------------|-------------|
| PROCESS_EXECUTION_LOG_SK | 2 (auto) | 2 |
| PROCESS_CONFIG_SK | NULL | NULL |
| PROCESS_STEP_ID | `stg_jaffle_shop__customers_abc123-def456-ghi789` | (same) |
| EXECUTION_STATUS_NAME | `RUNNING` | `SUCCESS` |
| EXECUTION_COMPLETED_IND | `N` | `Y` |
| EXECUTION_START_TWSTP | `2025-12-10 10:00:01.000` | `2025-12-10 10:00:01.000` |
| EXECUTION_END_TWSTP | NULL | `2025-12-10 10:01:15.000` |
| SOURCE_OBJ | `{"model_name": "stg_jaffle_shop__customers", "database": "DEV_DB", "schema": "dbt_ajain"}` | (unchanged) |
| DESTINATION_OBJ | `{"table": "stg_jaffle_shop__customers", "database": "DEV_DB", "schema": "dbt_ajain"}` | (unchanged) |
| PROCESS_CONFIG_OBJ | `{"invocation_id": "abc123-def456-ghi789", "project_name": "dbt_fundamentals"}` | (unchanged) |
| SOURCE_DATA_CNT | NULL | NULL |
| DESTINATION_DATA_CNT_OBJ | NULL | NULL |
| EXECUTION_TYPE_NAME | `DBT_MODEL_RUN` | `DBT_MODEL_RUN` |
| EXTRACT_START_TWSTP | `2025-12-10 10:00:01.000` | `2025-12-10 10:00:01.000` |
| EXTRACT_END_TWSTP | NULL | `2025-12-10 10:01:15.000` |
| ERROR_MESSAGE_OBJ | NULL | NULL |
| STEP_EXECUTION_OBJ | `{"step": "START", "model": "stg_jaffle_shop__customers"}` | `{"step": "END", "model": "stg_jaffle_shop__customers", "status": "SUCCESS"}` |
| INSERT_TWSTP | `2025-12-10 10:00:01.000` | `2025-12-10 10:00:01.000` |
| UPDATE_TWSTP | `2025-12-10 10:00:01.000` | `2025-12-10 10:01:15.000` |
| DELETED_IND | `N` | `N` |

---

## Record 3: Model-Level Record (stg_jaffle_shop__orders)

| Column | Value (Start) | Value (End) |
|--------|---------------|-------------|
| PROCESS_EXECUTION_LOG_SK | 3 (auto) | 3 |
| PROCESS_CONFIG_SK | NULL | NULL |
| PROCESS_STEP_ID | `stg_jaffle_shop__orders_abc123-def456-ghi789` | (same) |
| EXECUTION_STATUS_NAME | `RUNNING` | `SUCCESS` |
| EXECUTION_COMPLETED_IND | `N` | `Y` |
| EXECUTION_START_TWSTP | `2025-12-10 10:01:16.000` | `2025-12-10 10:01:16.000` |
| EXECUTION_END_TWSTP | NULL | `2025-12-10 10:02:30.000` |
| SOURCE_OBJ | `{"model_name": "stg_jaffle_shop__orders", "database": "DEV_DB", "schema": "dbt_ajain"}` | (unchanged) |
| DESTINATION_OBJ | `{"table": "stg_jaffle_shop__orders", "database": "DEV_DB", "schema": "dbt_ajain"}` | (unchanged) |
| PROCESS_CONFIG_OBJ | `{"invocation_id": "abc123-def456-ghi789", "project_name": "dbt_fundamentals"}` | (unchanged) |
| SOURCE_DATA_CNT | NULL | NULL |
| DESTINATION_DATA_CNT_OBJ | NULL | NULL |
| EXECUTION_TYPE_NAME | `DBT_MODEL_RUN` | `DBT_MODEL_RUN` |
| EXTRACT_START_TWSTP | `2025-12-10 10:01:16.000` | `2025-12-10 10:01:16.000` |
| EXTRACT_END_TWSTP | NULL | `2025-12-10 10:02:30.000` |
| ERROR_MESSAGE_OBJ | NULL | NULL |
| STEP_EXECUTION_OBJ | `{"step": "START", "model": "stg_jaffle_shop__orders"}` | `{"step": "END", "model": "stg_jaffle_shop__orders", "status": "SUCCESS"}` |
| INSERT_TWSTP | `2025-12-10 10:01:16.000` | `2025-12-10 10:01:16.000` |
| UPDATE_TWSTP | `2025-12-10 10:01:16.000` | `2025-12-10 10:02:30.000` |
| DELETED_IND | `N` | `N` |

---

## Record 4: Model-Level Record (dim_customers)

| Column | Value (Start) | Value (End) |
|--------|---------------|-------------|
| PROCESS_EXECUTION_LOG_SK | 4 (auto) | 4 |
| PROCESS_CONFIG_SK | NULL | NULL |
| PROCESS_STEP_ID | `dim_customers_abc123-def456-ghi789` | (same) |
| EXECUTION_STATUS_NAME | `RUNNING` | `SUCCESS` |
| EXECUTION_COMPLETED_IND | `N` | `Y` |
| EXECUTION_START_TWSTP | `2025-12-10 10:02:31.000` | `2025-12-10 10:02:31.000` |
| EXECUTION_END_TWSTP | NULL | `2025-12-10 10:05:25.000` |
| SOURCE_OBJ | `{"model_name": "dim_customers", "database": "DEV_DB", "schema": "dbt_ajain"}` | (unchanged) |
| DESTINATION_OBJ | `{"table": "dim_customers", "database": "DEV_DB", "schema": "dbt_ajain"}` | (unchanged) |
| PROCESS_CONFIG_OBJ | `{"invocation_id": "abc123-def456-ghi789", "project_name": "dbt_fundamentals"}` | (unchanged) |
| SOURCE_DATA_CNT | NULL | NULL |
| DESTINATION_DATA_CNT_OBJ | NULL | NULL |
| EXECUTION_TYPE_NAME | `DBT_MODEL_RUN` | `DBT_MODEL_RUN` |
| EXTRACT_START_TWSTP | `2025-12-10 10:02:31.000` | `2025-12-10 10:02:31.000` |
| EXTRACT_END_TWSTP | NULL | `2025-12-10 10:05:25.000` |
| ERROR_MESSAGE_OBJ | NULL | NULL |
| STEP_EXECUTION_OBJ | `{"step": "START", "model": "dim_customers"}` | `{"step": "END", "model": "dim_customers", "status": "SUCCESS"}` |
| INSERT_TWSTP | `2025-12-10 10:02:31.000` | `2025-12-10 10:02:31.000` |
| UPDATE_TWSTP | `2025-12-10 10:02:31.000` | `2025-12-10 10:05:25.000` |
| DELETED_IND | `N` | `N` |

---

## Summary

| PROCESS_STEP_ID | EXECUTION_TYPE_NAME | STATUS | Duration |
|-----------------|---------------------|--------|----------|
| `JOB_abc123...` | `DBT_JOB_RUN` | SUCCESS | 5m 30s |
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
