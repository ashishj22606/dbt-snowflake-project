{%- macro log_execution_start() -%}

{#- 
    This macro UPDATES the single job record when a model STARTS executing.
    It appends model details to STEP_EXECUTION_OBJ.models array and updates summary.
-#}

{% set log_table = 'DEV_PROVIDERPDM.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG' %}
{% set model_name = this.name %}
{% set run_id = invocation_id %}
{% set process_step_id = 'JOB_' ~ run_id %}

update {{ log_table }}
set
    UPDATE_TMSTP = CURRENT_TIMESTAMP(),
    STEP_EXECUTION_OBJ = object_insert(
        object_insert(
            STEP_EXECUTION_OBJ,
            'models',
            array_append(
                STEP_EXECUTION_OBJ:models,
                parse_json('{
                    "model_name": "{{ model_name }}",
                    "database": "{{ this.database }}",
                    "schema": "{{ this.schema }}",
                    "status": "RUNNING",
                    "start_time": "' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH24:MI:SS.FF3') || '",
                    "end_time": null,
                    "error": null
                }')
            ),
            true
        ),
        'current_step',
        'RUNNING: {{ model_name }}',
        true
    ),
    DESTINATION_DATA_CNT_OBJ = object_insert(
        object_insert(
            DESTINATION_DATA_CNT_OBJ,
            'total_models',
            COALESCE(DESTINATION_DATA_CNT_OBJ:total_models::INT, 0) + 1,
            true
        ),
        'running',
        COALESCE(DESTINATION_DATA_CNT_OBJ:running::INT, 0) + 1,
        true
    )
where PROCESS_STEP_ID = '{{ process_step_id }}'

{%- endmacro -%}
