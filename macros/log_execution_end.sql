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
        (select count(*) from {{ this }}) as destination_row_count,
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
                'step_number', array_size(coalesce(base.STEP_EXECUTION_OBJ:execution_timeline, parse_json('[]'))) + 1,
                'timestamp', to_varchar(current_timestamp(), 'YYYY-MM-DD HH24:MI:SS.FF3'),
                'level', 'Info',
                'step_type', 'MODEL_COMPLETE',
                'title', 'Model Completed: {{ model_name }}',
                'query_id', LAST_QUERY_ID(),
                'query_result', object_construct(
                    'rows_in_destination', source.destination_row_count,
                    'execution_status', 'SUCCESS'
                ),
                'content', object_construct(
                    'model', '{{ model_name }}',
                    'status', 'SUCCESS',
                    'rows_processed', source.destination_row_count,
                    'destination_table', '{{ this.database }}.{{ this.schema }}.{{ this.identifier }}'
                )
            )
        ) as new_timeline
    from {{ log_table }} base,
         lateral flatten(input => base.STEP_EXECUTION_OBJ:models) model_data
    where base.PROCESS_STEP_ID = '{{ process_step_id }}'
    group by base.PROCESS_STEP_ID, base.STEP_EXECUTION_OBJ:execution_timeline, base.SOURCE_DATA_CNT, base.DESTINATION_DATA_CNT_OBJ
) as source
on target.PROCESS_STEP_ID = source.process_step_id
when matched then update set
    target.UPDATE_TMSTP = current_timestamp(),
    target.SOURCE_DATA_CNT = coalesce(target.SOURCE_DATA_CNT, 0) + source.destination_row_count,
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
    target.DESTINATION_DATA_CNT_OBJ = object_insert(
        coalesce(target.DESTINATION_DATA_CNT_OBJ, parse_json('{}')),
        '{{ model_name }}',
        source.destination_row_count,
        true
    )

{%- endmacro -%}
