{%- macro log_execution_end() -%}

{% set snowflake_db = env_var('SNOWFLAKE_DATABASE', 'DEV_PROVIDERPDM') %}
{% set log_table = snowflake_db ~ '.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG' %}
{% set model_name = this.name %}
{% set run_id = invocation_id %}
{% set process_step_id = 'JOB_' ~ run_id %}

update {{ log_table }} t
    set EXECUTION_STATUS_NAME = 'SUCCESS',
        EXECUTION_COMPLETED_IND = 'Y',
        EXECUTION_END_TMSTP = CURRENT_TIMESTAMP(),
        UPDATE_TMSTP = CURRENT_TIMESTAMP(),
        SOURCE_DATA_CNT = c.row_count,
        DESTINATION_DATA_CNT_OBJ = c.row_count,
        METRICS_OBJ = object_construct(
            'load_type', c.execution_type,
            'materialization', c.materialization,
            'processing_ms', datediff('millisecond', c.EXECUTION_START_TMSTP, current_timestamp()),
            'rows_total', c.row_count,
            'rows_inserted', c.rows_inserted,
            'rows_updated', c.rows_updated,
            'rows_deleted', c.rows_deleted,
            'rows_affected', c.rows_affected
        ),
        STEP_EXECUTION_OBJ = object_construct(
            'model_name', c.STEP_EXECUTION_OBJ:model_name::varchar,
            'current_step', 'MODEL_COMPLETED',
            'query_id_start', c.STEP_EXECUTION_OBJ:query_id_start::varchar,
            'query_id_end', LAST_QUERY_ID(),
            'execution_timeline', array_append(
                case
                    when array_size(iff(is_array(c.STEP_EXECUTION_OBJ:execution_timeline), c.STEP_EXECUTION_OBJ:execution_timeline, array_construct())) = 0
                    then array_cat(
                        array_construct(),
                        array_construct(
                            object_construct(
                                'step_number', 1,
                                'timestamp', to_varchar(c.EXECUTION_START_TMSTP, 'YYYY-MM-DD HH24:MI:SS.FF3'),
                                'level', 'Info',
                                'step_type', 'MODEL_START',
                                'title', 'Model Started: {{ model_name }}',
                                'query_id', c.STEP_EXECUTION_OBJ:query_id_start::varchar,
                                'content', object_construct(
                                    'model', '{{ model_name }}'
                                )
                            )
                        )
                    )
                    else iff(is_array(c.STEP_EXECUTION_OBJ:execution_timeline), c.STEP_EXECUTION_OBJ:execution_timeline, array_construct())
                end,
                object_construct(
                    'step_number', array_size(
                        case
                            when array_size(iff(is_array(c.STEP_EXECUTION_OBJ:execution_timeline), c.STEP_EXECUTION_OBJ:execution_timeline, array_construct())) = 0
                            then array_construct(
                                object_construct(
                                    'step_number', 1,
                                    'timestamp', to_varchar(c.EXECUTION_START_TMSTP, 'YYYY-MM-DD HH24:MI:SS.FF3'),
                                    'level', 'Info',
                                    'step_type', 'MODEL_START',
                                    'title', 'Model Started: {{ model_name }}',
                                    'query_id', c.STEP_EXECUTION_OBJ:query_id_start::varchar,
                                    'content', object_construct(
                                        'model', '{{ model_name }}'
                                    )
                                )
                            )
                            else iff(is_array(c.STEP_EXECUTION_OBJ:execution_timeline), c.STEP_EXECUTION_OBJ:execution_timeline, array_construct())
                        end
                    ) + 1,
                    'timestamp', to_varchar(current_timestamp(), 'YYYY-MM-DD HH24:MI:SS.FF3'),
                    'level', 'Info',
                    'step_type', 'MODEL_COMPLETE',
                    'title', 'Model Completed: {{ model_name }}',
                    'query_id', LAST_QUERY_ID(),
                    'query_result', object_construct(
                        'rows_in_destination', c.row_count,
                        'execution_status', 'SUCCESS'
                    ),
                    'metrics', object_construct(
                        'load_type', c.execution_type,
                        'materialization', c.materialization,
                        'processing_ms', datediff('millisecond', c.EXECUTION_START_TMSTP, current_timestamp()),
                        'rows_total', c.row_count,
                        'rows_inserted', c.rows_inserted,
                        'rows_updated', c.rows_updated,
                        'rows_deleted', c.rows_deleted,
                        'rows_affected', c.rows_affected
                    ),
                    'content', object_construct(
                        'model', '{{ model_name }}',
                        'status', 'SUCCESS',
                        'rows_processed', c.row_count,
                        'destination_table', '{{ this.database }}.{{ this.schema }}.{{ this.identifier }}'
                    )
                )
            )
        )
from (
    select 
        l.PROCESS_STEP_ID,
        l.RECORD_TYPE,
        l.MODEL_NAME,
        l.STEP_EXECUTION_OBJ,
        l.EXECUTION_START_TMSTP,
        l.PROCESS_CONFIG_OBJ:materialization::varchar as materialization,
        l.PROCESS_CONFIG_OBJ:execution_type::varchar as execution_type,
        (select count(*) from {{ this }}) as row_count,
        coalesce(q.ROWS_INSERTED, 0) as rows_inserted,
        coalesce(q.ROWS_UPDATED, 0) as rows_updated,
        coalesce(q.ROWS_DELETED, 0) as rows_deleted,
        coalesce(q.ROWS_PRODUCED, 0) as rows_affected,
        row_number() over (
            partition by l.PROCESS_STEP_ID, l.RECORD_TYPE, l.MODEL_NAME
            order by coalesce(l.UPDATE_TMSTP, l.INSERT_TMSTP) desc, l.INSERT_TMSTP desc
        ) as rn
    from {{ log_table }} l
    left join lateral (
        select
            QUERY_ID,
            ROWS_INSERTED,
            ROWS_UPDATED,
            ROWS_DELETED,
            ROWS_PRODUCED
        from table(information_schema.query_history_by_session())
        where QUERY_ID = LAST_QUERY_ID()
        limit 1
    ) q
    where l.PROCESS_STEP_ID = '{{ process_step_id }}'
      and l.RECORD_TYPE = 'MODEL'
      and l.MODEL_NAME = '{{ model_name }}'
      qualify rn = 1
) c
where t.PROCESS_STEP_ID = c.PROCESS_STEP_ID
  and t.RECORD_TYPE = c.RECORD_TYPE
  and t.MODEL_NAME = c.MODEL_NAME

{%- endmacro -%}