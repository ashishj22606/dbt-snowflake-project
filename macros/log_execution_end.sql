{%- macro log_execution_end() -%}

{#- 
    This macro UPDATES the single job record when a model FINISHES executing.
    It updates the model entry in STEP_EXECUTION_OBJ.models array with end time and status.
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
            (
                select array_agg(
                    case 
                        when value:model_name::STRING = '{{ model_name }}' then
                            object_insert(
                                object_insert(
                                    object_insert(value, 'status', 'SUCCESS', true),
                                    'end_time', TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH24:MI:SS.FF3'), true
                                ),
                                'duration_seconds', 
                                TIMESTAMPDIFF(SECOND, value:start_time::TIMESTAMP_NTZ, CURRENT_TIMESTAMP()),
                                true
                            )
                        else value
                    end
                ) within group (order by index)
                from table(flatten(input => STEP_EXECUTION_OBJ:models))
            ),
            true
        ),
        'current_step',
        'COMPLETED: {{ model_name }}',
        true
    ),
    DESTINATION_DATA_CNT_OBJ = object_insert(
        object_insert(
            DESTINATION_DATA_CNT_OBJ,
            'completed',
            COALESCE(DESTINATION_DATA_CNT_OBJ:completed::INT, 0) + 1,
            true
        ),
        'running',
        GREATEST(COALESCE(DESTINATION_DATA_CNT_OBJ:running::INT, 0) - 1, 0),
        true
    )
where PROCESS_STEP_ID = '{{ process_step_id }}'

{%- endmacro -%}
