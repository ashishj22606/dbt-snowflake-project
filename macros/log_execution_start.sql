{%- macro log_execution_start() -%}

{#- 
    This macro UPDATES the single job record when a model STARTS executing.
    Updates current_step to show which model is running.
-#}

{% set log_table = 'DEV_PROVIDERPDM.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG' %}
{% set model_name = this.name %}
{% set run_id = invocation_id %}
{% set process_step_id = 'JOB_' ~ run_id %}

update {{ log_table }}
set
    UPDATE_TMSTP = CURRENT_TIMESTAMP(),
    EXECUTION_STATUS_NAME = 'RUNNING',
    STEP_EXECUTION_OBJ = object_insert(STEP_EXECUTION_OBJ, 'current_step', 'RUNNING: {{ model_name }}', true)
where PROCESS_STEP_ID = '{{ process_step_id }}'

{%- endmacro -%}

{%- endmacro -%}
