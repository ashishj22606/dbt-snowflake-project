{%- macro log_run_start() -%}

{#- 
    This macro inserts a job-level record into PROCESS_EXECUTION_LOG when a dbt run starts.
    It uses Snowflake's CURRENT_TIMESTAMP() to get server time.
    The PROCESS_STEP_ID is the unique identifier for the entire job run (JOB_ + invocation_id).
-#}

{% set log_table = 'DEV_PROVIDERPDM.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG' %}
{% set run_id = invocation_id %}
{% set process_step_id = 'JOB_' ~ run_id %}

insert into {{ log_table }} (
    PROCESS_CONFIG_SK,
    PROCESS_STEP_ID,
    EXECUTION_STATUS_NAME,
    EXECUTION_COMPLETED_IND,
    EXECUTION_START_TWSTP,
    EXECUTION_END_TWSTP,
    SOURCE_OBJ,
    DESTINATION_OBJ,
    PROCESS_CONFIG_OBJ,
    SOURCE_DATA_CNT,
    DESTINATION_DATA_CNT_OBJ,
    EXECUTION_TYPE_NAME,
    EXTRACT_START_TWSTP,
    EXTRACT_END_TWSTP,
    ERROR_MESSAGE_OBJ,
    STEP_EXECUTION_OBJ,
    INSERT_TWSTP,
    UPDATE_TWSTP,
    DELETED_IND
)
select
    null as PROCESS_CONFIG_SK,
    '{{ process_step_id }}' as PROCESS_STEP_ID,
    'RUNNING' as EXECUTION_STATUS_NAME,
    'N' as EXECUTION_COMPLETED_IND,
    CURRENT_TIMESTAMP() as EXECUTION_START_TWSTP,
    null as EXECUTION_END_TWSTP,
    parse_json('{"type": "DBT_JOB", "project_name": "{{ project_name }}"}') as SOURCE_OBJ,
    parse_json('{"target_name": "{{ target.name }}", "target_schema": "{{ target.schema }}"}') as DESTINATION_OBJ,
    parse_json('{"invocation_id": "{{ run_id }}", "project_name": "{{ project_name }}", "target_name": "{{ target.name }}"}') as PROCESS_CONFIG_OBJ,
    null as SOURCE_DATA_CNT,
    null as DESTINATION_DATA_CNT_OBJ,
    'DBT_JOB_RUN' as EXECUTION_TYPE_NAME,
    CURRENT_TIMESTAMP() as EXTRACT_START_TWSTP,
    null as EXTRACT_END_TWSTP,
    null as ERROR_MESSAGE_OBJ,
    parse_json('{"step": "JOB_START", "type": "FULL_RUN"}') as STEP_EXECUTION_OBJ,
    CURRENT_TIMESTAMP() as INSERT_TWSTP,
    CURRENT_TIMESTAMP() as UPDATE_TWSTP,
    'N' as DELETED_IND

{%- endmacro -%}
