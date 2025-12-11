{%- macro log_run_end() -%}

{#- 
    This macro updates the job-level record in PROCESS_EXECUTION_LOG when a dbt run ends.
    It uses Snowflake's CURRENT_TIMESTAMP() to get server time.
    The PROCESS_STEP_ID is the unique identifier for the entire job run (JOB_ + invocation_id).
-#}

{% set log_table = 'DEV_PROVIDERPDM.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG' %}
{% set run_id = invocation_id %}
{% set process_step_id = 'JOB_' ~ run_id %}

update {{ log_table }}
set
    EXECUTION_STATUS_NAME = 'SUCCESS',
    EXECUTION_COMPLETED_IND = 'Y',
    EXECUTION_END_TWSTP = CURRENT_TIMESTAMP(),
    EXTRACT_END_TWSTP = CURRENT_TIMESTAMP(),
    UPDATE_TWSTP = CURRENT_TIMESTAMP(),
    STEP_EXECUTION_OBJ = parse_json('{"step": "JOB_END", "type": "FULL_RUN", "status": "SUCCESS"}'),
    ERROR_MESSAGE_OBJ = null
where PROCESS_STEP_ID = '{{ process_step_id }}'

{%- endmacro -%}
