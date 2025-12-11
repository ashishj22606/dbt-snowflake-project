{%- macro log_run_start() -%}

{#- 
    This macro inserts a SINGLE record into PROCESS_EXECUTION_LOG when a dbt run starts.
    Captures enhanced configuration and metadata for ETL tracking.
-#}

{% set log_table = 'DEV_PROVIDERPDM.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG' %}
{% set run_id = invocation_id %}
{% set process_step_id = 'JOB_' ~ run_id %}

insert into {{ log_table }} (
    PROCESS_CONFIG_SK,
    PROCESS_STEP_ID,
    EXECUTION_STATUS_NAME,
    EXECUTION_COMPLETED_IND,
    EXECUTION_START_TMSTP,
    EXECUTION_END_TMSTP,
    SOURCE_OBJ,
    DESTINATION_OBJ,
    PROCESS_CONFIG_OBJ,
    SOURCE_DATA_CNT,
    DESTINATION_DATA_CNT_OBJ,
    EXECUTION_TYPE_NAME,
    EXTRACT_START_TMSTP,
    EXTRACT_END_TMSTP,
    ERROR_MESSAGE_OBJ,
    STEP_EXECUTION_OBJ,
    INSERT_TMSTP,
    UPDATE_TMSTP,
    DELETED_IND
)
select
    null as PROCESS_CONFIG_SK,
    '{{ process_step_id }}' as PROCESS_STEP_ID,
    'RUNNING' as EXECUTION_STATUS_NAME,
    'N' as EXECUTION_COMPLETED_IND,
    CURRENT_TIMESTAMP() as EXECUTION_START_TMSTP,
    null::TIMESTAMP_NTZ as EXECUTION_END_TMSTP,
    parse_json('{}') as SOURCE_OBJ,
    parse_json('{}') as DESTINATION_OBJ,
    parse_json('{"invocation_id":"{{ run_id }}","project_name":"{{ project_name }}","target_name":"{{ target.name }}","dbt_version":"{{ dbt_version }}","run_started_at":"{{ run_started_at }}","which":"run","full_refresh":false,"target_database":"{{ target.database }}","target_schema":"{{ target.schema }}","warehouse":"{{ target.warehouse }}","threads":{{ target.threads }}}') as PROCESS_CONFIG_OBJ,
    0 as SOURCE_DATA_CNT,
    parse_json('{}') as DESTINATION_DATA_CNT_OBJ,
    'DBT_JOB_RUN' as EXECUTION_TYPE_NAME,
    CURRENT_TIMESTAMP() as EXTRACT_START_TMSTP,
    null::TIMESTAMP_NTZ as EXTRACT_END_TMSTP,
    parse_json('{"errors":[]}') as ERROR_MESSAGE_OBJ,
    parse_json('{"models":[],"current_step":"JOB_STARTED","execution_timeline":[]}') as STEP_EXECUTION_OBJ,
    CURRENT_TIMESTAMP() as INSERT_TMSTP,
    CURRENT_TIMESTAMP() as UPDATE_TMSTP,
    'N' as DELETED_IND

{%- endmacro -%}
