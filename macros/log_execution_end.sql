{%- macro log_execution_end(
    execution_status_name='SUCCESS',
    error_message=none
) -%}

{#- 
    This macro updates the existing record in PROCESS_EXECUTION_LOG when a model finishes executing.
    It uses Snowflake's CURRENT_TIMESTAMP() to get server time.
    The PROCESS_STEP_ID is the unique identifier for each model run (model_name + invocation_id).
-#}

{% set log_table = 'DEV_PROVIDERPDM.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG' %}
{% set model_name = this.name %}
{% set run_id = invocation_id %}
{% set process_step_id = model_name ~ '_' ~ run_id %}

update {{ log_table }}
set
    EXECUTION_STATUS_NAME = '{{ execution_status_name }}',
    EXECUTION_COMPLETED_IND = 'Y',
    EXECUTION_END_TWSTP = CURRENT_TIMESTAMP(),
    EXTRACT_END_TWSTP = CURRENT_TIMESTAMP(),
    UPDATE_TWSTP = CURRENT_TIMESTAMP(),
    STEP_EXECUTION_OBJ = parse_json('{"step": "END", "model": "{{ model_name }}", "status": "{{ execution_status_name }}"}'),
    ERROR_MESSAGE_OBJ = {% if error_message is not none %}parse_json('{{ error_message }}'){% else %}null{% endif %}
where PROCESS_STEP_ID = '{{ process_step_id }}'

{%- endmacro -%}
