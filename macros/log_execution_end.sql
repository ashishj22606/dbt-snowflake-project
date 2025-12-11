{%- macro log_execution_end() -%}

{#- 
    This macro UPDATES the single job record when a model FINISHES executing.
    Updates model with SUCCESS status, end time, duration, query ID, and row counts.
    Adds completion event to timeline.
-#}

{% set log_table = 'DEV_PROVIDERPDM.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG' %}
{% set model_name = this.name %}
{% set run_id = invocation_id %}
{% set process_step_id = 'JOB_' ~ run_id %}

merge into {{ log_table }} as target
using (
    select 
        '{{ process_step_id }}' as process_step_id,
        array_agg(
            case 
                when model_data.value:model_name::string = '{{ model_name }}' then
                    object_construct(
                        'model_name', '{{ model_name }}',
                        'database', model_data.value:database,
                        'schema', model_data.value:schema,
                        'alias', model_data.value:alias,
                        'materialization', model_data.value:materialization,
                        'status', 'SUCCESS',
                        'start_time', model_data.value:start_time,
                        'end_time', to_varchar(current_timestamp(), 'YYYY-MM-DD HH24:MI:SS.FF3'),
                        'duration_seconds', timestampdiff(second, model_data.value:start_time::timestamp_ntz, current_timestamp()),
                        'query_id_start', model_data.value:query_id_start,
                        'query_id_end', LAST_QUERY_ID(),
                        'rows_affected', null
                    )
                else model_data.value
            end
        ) within group (order by model_data.index) as new_models_array,
        array_append(
            coalesce(base.STEP_EXECUTION_OBJ:execution_timeline, parse_json('[]')),
            object_construct(
                'timestamp', to_varchar(current_timestamp(), 'YYYY-MM-DD HH24:MI:SS.FF3'),
                'level', 'Info',
                'title', 'Model Completed: {{ model_name }}',
                'content', object_construct(
                    'model', '{{ model_name }}',
                    'status', 'SUCCESS',
                    'query_id', LAST_QUERY_ID()
                )
            )
        ) as new_timeline,
        object_construct(
            'total_models', coalesce(base.DESTINATION_DATA_CNT_OBJ:total_models::int, 0),
            'success', coalesce(base.DESTINATION_DATA_CNT_OBJ:success::int, 0) + 1,
            'failed', coalesce(base.DESTINATION_DATA_CNT_OBJ:failed::int, 0),
            'skipped', coalesce(base.DESTINATION_DATA_CNT_OBJ:skipped::int, 0),
            'running', greatest(coalesce(base.DESTINATION_DATA_CNT_OBJ:running::int, 0) - 1, 0)
        ) as new_counts
    from {{ log_table }} base,
         lateral flatten(input => base.STEP_EXECUTION_OBJ:models) model_data
    where base.PROCESS_STEP_ID = '{{ process_step_id }}'
    group by base.PROCESS_STEP_ID, base.DESTINATION_DATA_CNT_OBJ, base.STEP_EXECUTION_OBJ:execution_timeline
) as source
on target.PROCESS_STEP_ID = source.process_step_id
when matched then update set
    target.UPDATE_TMSTP = current_timestamp(),
    target.STEP_EXECUTION_OBJ = object_insert(
        object_insert(
            object_insert(
                target.STEP_EXECUTION_OBJ,
                'models',
                source.new_models_array,
                true
            ),
            'execution_timeline',
            source.new_timeline,
            true
        ),
        'current_step',
        'COMPLETED: {{ model_name }}',
        true
    ),
    target.DESTINATION_DATA_CNT_OBJ = source.new_counts

{%- endmacro -%}
