{%- macro log_run_start() -%}

{#- 
    This macro inserts a JOB-level record into PROCESS_EXECUTION_LOG when a dbt run starts.
    RECORD_TYPE = 'JOB' for job-level tracking.
    Individual models will create separate MODEL records with the same PROCESS_STEP_ID.
-#}

{% set snowflake_db = env_var('SNOWFLAKE_DATABASE', 'DEV_PROVIDERPDM') %}
{% set log_table = snowflake_db ~ '.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG' %}
{% set run_id = invocation_id %}
{% set process_step_id = 'JOB_' ~ run_id %}

insert into {{ log_table }} (
    PROCESS_STEP_ID,
    RECORD_TYPE,
    MODEL_NAME,
    EXECUTION_STATUS_NAME,
    EXECUTION_COMPLETED_IND,
    EXECUTION_START_TMSTP,
    EXECUTION_END_TMSTP,
    SOURCE_OBJ,
    DESTINATION_OBJ,
    PROCESS_CONFIG_OBJ,
    EXECUTION_TYPE_NAME,
    ERROR_MESSAGE_OBJ,
    STEP_EXECUTION_OBJ,
    INSERT_TMSTP,
    UPDATE_TMSTP,
    DELETED_IND
)
select
    '{{ process_step_id }}' as PROCESS_STEP_ID,
    'JOB' as RECORD_TYPE,
    null as MODEL_NAME,
    'RUNNING' as EXECUTION_STATUS_NAME,
    'N' as EXECUTION_COMPLETED_IND,
    CURRENT_TIMESTAMP() as EXECUTION_START_TMSTP,
    null::TIMESTAMP_NTZ as EXECUTION_END_TMSTP,
    parse_json('null') as SOURCE_OBJ,
    parse_json('null') as DESTINATION_OBJ,
    parse_json('{"invocation_id":"{{ run_id }}","project_name":"{{ project_name }}","target_name":"{{ target.name }}","dbt_version":"{{ dbt_version }}","run_started_at":"{{ run_started_at }}","which":"run","full_refresh":false,"target_database":"{{ target.database }}","target_schema":"{{ target.schema }}","warehouse":"{{ target.warehouse }}","threads":{{ target.threads }}}') as PROCESS_CONFIG_OBJ,
    'DBT_JOB_RUN' as EXECUTION_TYPE_NAME,
    parse_json('null') as ERROR_MESSAGE_OBJ,
    parse_json('{"current_step":"JOB_STARTED","execution_timeline":[{"step_number":1,"timestamp":"' || to_varchar(current_timestamp(), 'YYYY-MM-DD HH24:MI:SS.FF3') || '","level":"Info","step_type":"JOB_START","title":"Job Started","content":{"invocation_id":"{{ run_id }}","project":"{{ project_name }}","target":"{{ target.name }}"}}]}') as STEP_EXECUTION_OBJ,
    CURRENT_TIMESTAMP() as INSERT_TMSTP,
    CURRENT_TIMESTAMP() as UPDATE_TMSTP,
    'N' as DELETED_IND

{%- endmacro -%}
