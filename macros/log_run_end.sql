{%- macro log_run_end() -%}

{#- 
    This macro updates the SINGLE record in PROCESS_EXECUTION_LOG when a dbt run ends.
    It updates the final status and summary counts.
-#}

{% set log_table = 'DEV_PROVIDERPDM.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG' %}
{% set run_id = invocation_id %}
{% set process_step_id = 'JOB_' ~ run_id %}

update {{ log_table }}
set
    EXECUTION_STATUS_NAME = 'SUCCESS',
    EXECUTION_COMPLETED_IND = 'Y',
    EXECUTION_END_TMSTP = CURRENT_TIMESTAMP(),
    EXTRACT_END_TMSTP = CURRENT_TIMESTAMP(),
    UPDATE_TMSTP = CURRENT_TIMESTAMP(),
    STEP_EXECUTION_OBJ = object_insert(
        STEP_EXECUTION_OBJ,
        'current_step',
        'JOB_COMPLETED',
        true
    )
where PROCESS_STEP_ID = '{{ process_step_id }}'

{%- endmacro -%}
