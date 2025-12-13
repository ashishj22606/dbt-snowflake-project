# Recommended Improvements for Process Execution Log

Based on your example JSON structure, here are the improvements we should make to the dbt logging solution:

---

## Current State vs. Your Requirements

### What We Have Now:
```json
{
  "models": [
    {
      "model_name": "stg_customers",
      "status": "SUCCESS",
      "execution_time_seconds": 1.25
    }
  ],
  "current_step": "RUNNING: model",
  "summary": {"total": 1, "success": 1, "error": 0}
}
```

### What You Need (Based on Example):
```json
{
  "source_details": {
    "schema": "XCELYS_CORE_RAW",
    "stream": "NDC_CODE_MASTER_BASE_raw_stream",
    "table": "NDC_CODE_MASTER_BASE",
    "warehouse": "XCELYS_APP1_WH"
  },
  "target_details": {
    "schema": "XCELYS_CORE_TARGET",
    "table": "NDC_CODE_MASTER",
    "work_schema": "XCELYS_CORE_WORK"
  },
  "config": {
    "merge_mode": "UPSERT",
    "natural_keys": ["NDC_CODE"],
    "sort_keys": ["INFA_LAST_REPLICATED", "INFA_OPERATION_TIME"]
  },
  "execution_log": [
    {
      "level": "Info",
      "timestamp": "2025-03-12 03:00:33.941",
      "title": "Process Config Details",
      "content": {...}
    },
    {
      "level": "Info", 
      "timestamp": "2025-03-12 03:00:35.782",
      "title": "Execute Query to Get max PROCESS_EXECUTION_LOG_SK Value",
      "content": {
        "query_result": "239786",
        "query_id": "01baf440-030c-a823-005d-7607c2cfb9aa"
      }
    }
  ],
  "data_counts": {
    "raw_count_before": 98,
    "work_count_after": 98,
    "target_inserted": 10,
    "target_updated": 5,
    "target_deleted": 2
  },
  "query_ids": [
    "01baf440-030c-a823-005d-7607c2cfb9aa",
    "01baf440-030c-a823-005d-7607c2cfb8fa"
  ]
}
```

---

## Recommended Improvements

### 1. **SOURCE_OBJ Enhancement**
Current:
```json
{"type": "DBT_JOB", "project_name": "dbt_fundamentals"}
```

**Improved:**
```json
{
  "source_type": "DBT_MODEL",
  "project_name": "dbt_fundamentals",
  "source_schema": "{{ model.schema }}",
  "source_table": "{{ model.alias }}",
  "source_database": "{{ model.database }}",
  "materialization": "{{ model.config.materialized }}",
  "depends_on": ["upstream_model_1", "upstream_model_2"]
}
```

### 2. **DESTINATION_OBJ Enhancement**
Current:
```json
{"target_name": "dev", "target_schema": "dbt_ajain"}
```

**Improved:**
```json
{
  "target_name": "dev",
  "target_schema": "dbt_ajain",
  "target_table": "{{ model.alias }}",
  "target_database": "{{ model.database }}",
  "full_refresh": false,
  "incremental_strategy": "merge"
}
```

### 3. **PROCESS_CONFIG_OBJ Enhancement**
Current:
```json
{
  "invocation_id": "abc123",
  "project_name": "dbt_fundamentals",
  "target_name": "dev"
}
```

**Improved:**
```json
{
  "invocation_id": "abc123",
  "project_name": "dbt_fundamentals",
  "target_name": "dev",
  "dbt_version": "{{ dbt_version }}",
  "run_started_at": "{{ run_started_at }}",
  "which": "{{ which }}",
  "threads": "{{ threads }}",
  "selector": "{{ selector }}"
}
```

### 4. **SOURCE_DATA_CNT Enhancement**
Current: `NULL`

**Improved:** Actual row counts
- Use `{{ this }}` to query source row counts
- Capture before/after counts for incremental models

### 5. **DESTINATION_DATA_CNT_OBJ Enhancement**
Current:
```json
{"total_models": 3, "success": 2, "failed": 1}
```

**Improved:**
```json
{
  "total_models": 3,
  "success": 2,
  "failed": 1,
  "skipped": 0,
  "rows_affected_per_model": {
    "stg_customers": {"inserted": 100, "updated": 50, "deleted": 10},
    "stg_orders": {"inserted": 200, "updated": 75, "deleted": 5}
  },
  "total_rows_affected": {
    "inserted": 300,
    "updated": 125,
    "deleted": 15
  }
}
```

### 6. **STEP_EXECUTION_OBJ Enhancement**
Current:
```json
{
  "models": [
    {"model_name": "stg_customers", "status": "SUCCESS"}
  ]
}
```

**Improved:**
```json
{
  "models": [
    {
      "model_name": "stg_customers",
      "status": "SUCCESS",
      "start_time": "2025-12-10 10:00:01",
      "end_time": "2025-12-10 10:01:15",
      "duration_seconds": 74,
      "query_id": "01baf440-030c-a823-005d-7607c2cfb9aa",
      "rows_affected": {"inserted": 100, "updated": 50, "deleted": 10},
      "warehouse": "COMPUTE_WH",
      "warehouse_size": "MEDIUM",
      "credits_used": 0.05
    }
  ],
  "execution_timeline": [
    {
      "timestamp": "2025-12-10 10:00:00",
      "level": "Info",
      "title": "Job Started",
      "content": {"invocation_id": "abc123"}
    },
    {
      "timestamp": "2025-12-10 10:00:01",
      "level": "Info",
      "title": "Model Started: stg_customers",
      "content": {"query_id": "01baf440..."}
    }
  ]
}
```

### 7. **ERROR_MESSAGE_OBJ Enhancement**
Current:
```json
{"error_count": 2, "message": "2 model(s) failed"}
```

**Improved:**
```json
{
  "error_count": 2,
  "errors": [
    {
      "timestamp": "2025-12-10 10:05:00",
      "model_name": "dim_customers",
      "error_type": "Database Error",
      "error_code": "002003",
      "error_message": "SQL compilation error: Object 'CUSTOMERS' does not exist",
      "query_id": "01baf440-030c-a823-005d-7607c2cfb9aa",
      "stack_trace": "..."
    }
  ]
}
```

---

## Key Missing Features to Add

### 1. **Query ID Tracking**
- Capture Snowflake's `LAST_QUERY_ID()` after each model runs
- Store in `STEP_EXECUTION_OBJ.models[].query_id`

### 2. **Row Count Tracking**
- Use `@@ROWCOUNT` or query information schema
- Track inserted/updated/deleted rows per model

### 3. **Warehouse Metrics**
- Warehouse name and size
- Credits consumed
- Query compilation time vs execution time

### 4. **Timeline/Event Log**
- Array of timestamped events
- Each major step logged with timestamp and details

### 5. **Configuration Details**
- Natural keys / primary keys
- Sort keys / partition keys
- Merge mode (if using incremental)

### 6. **Data Quality Checks**
- Pre-execution row counts
- Post-execution row counts
- Data validation results

---

## Implementation Priority

| Priority | Feature | Effort | Value |
|----------|---------|--------|-------|
| 🔴 HIGH | Query ID tracking | Low | High |
| 🔴 HIGH | Row counts (inserted/updated/deleted) | Medium | High |
| 🟡 MEDIUM | Execution timeline array | Medium | Medium |
| 🟡 MEDIUM | Enhanced error details | Low | Medium |
| 🟢 LOW | Warehouse metrics | Medium | Low |
| 🟢 LOW | Configuration details | Low | Low |

---

## Should We Implement These?

Would you like me to:

1. **Add Query ID tracking** (quick win, high value)
2. **Add row count tracking** (requires additional queries)
3. **Add execution timeline** (structured event log)
4. **Keep current simple version** (less overhead)

Let me know which features are most important for your use case!
