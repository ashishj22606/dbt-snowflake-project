{%- macro log_execution_start() -%}

{#- 
    This macro UPDATES the single job record when a model STARTS executing.
    Appends model to array with config details and adds timeline event.
    Captures query ID for tracking in Snowflake.
-#}

{% set log_table = 'DEV_PROVIDERPDM.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG' %}
{% set model_name = this.name %}
{% set run_id = invocation_id %}
{% set process_step_id = 'JOB_' ~ run_id %}

merge into {{ log_table }} as target
using (
    select 
        '{{ process_step_id }}' as process_step_id,
        array_append(
            coalesce(STEP_EXECUTION_OBJ:models, parse_json('[]')),
            object_construct(
                'model_name', '{{ model_name }}',
                'database', '{{ this.database }}',
                'schema', '{{ this.schema }}',
                'alias', '{{ this.identifier }}',
                'materialization', '{{ config.get("materialized", "view") }}',
                'status', 'RUNNING',
                'start_time', to_varchar(current_timestamp(), 'YYYY-MM-DD HH24:MI:SS.FF3'),
                'end_time', null,
                'duration_seconds', null,
                'query_id_start', LAST_QUERY_ID(),
                'query_id_end', null,
                'rows_affected', null
            )
        ) as new_models_array,
        array_append(
            coalesce(STEP_EXECUTION_OBJ:execution_timeline, parse_json('[]')),
            object_construct(
                'timestamp', to_varchar(current_timestamp(), 'YYYY-MM-DD HH24:MI:SS.FF3'),
                'level', 'Info',
                'title', 'Model Started: {{ model_name }}',
                'content', object_construct('model', '{{ model_name }}', 'materialization', '{{ config.get("materialized", "view") }}')
            )
        ) as new_timeline,
        object_construct(
            'total_models', coalesce(DESTINATION_DATA_CNT_OBJ:total_models::int, 0) + 1,
            'success', coalesce(DESTINATION_DATA_CNT_OBJ:success::int, 0),
            'failed', coalesce(DESTINATION_DATA_CNT_OBJ:failed::int, 0),
            'skipped', coalesce(DESTINATION_DATA_CNT_OBJ:skipped::int, 0),
            'running', coalesce(DESTINATION_DATA_CNT_OBJ:running::int, 0) + 1
        ) as new_counts
    from {{ log_table }}
    where PROCESS_STEP_ID = '{{ process_step_id }}'
) as source
on target.PROCESS_STEP_ID = source.process_step_id
when matched then update set
    target.UPDATE_TMSTP = current_timestamp(),
    target.EXECUTION_STATUS_NAME = 'RUNNING',
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
        'RUNNING: {{ model_name }}',
        true
    ),
    target.DESTINATION_DATA_CNT_OBJ = source.new_counts

{%- endmacro -%}
