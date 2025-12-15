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
        EXTRACT_END_TMSTP = CURRENT_TIMESTAMP(),
        UPDATE_TMSTP = CURRENT_TIMESTAMP(),
        SOURCE_DATA_CNT = c.row_count,
        DESTINATION_DATA_CNT_OBJ = c.row_count,
        STEP_EXECUTION_OBJ = object_construct(
            'model_name', c.STEP_EXECUTION_OBJ:model_name::varchar,
            'current_step', 'MODEL_COMPLETED',
            'query_id_start', c.STEP_EXECUTION_OBJ:query_id_start::varchar,
            'query_id_end', LAST_QUERY_ID(),
            'execution_timeline', array_append(
                coalesce(c.STEP_EXECUTION_OBJ:execution_timeline, parse_json('[]')),
                object_construct(
                    'step_number', array_size(coalesce(c.STEP_EXECUTION_OBJ:execution_timeline, parse_json('[]'))) + 1,
                    'timestamp', to_varchar(current_timestamp(), 'YYYY-MM-DD HH24:MI:SS.FF3'),
                    'level', 'Info',
                    'step_type', 'MODEL_COMPLETE',
                    'title', 'Model Completed: {{ model_name }}',
                    'query_id', LAST_QUERY_ID(),
                    'query_result', object_construct(
                        'rows_in_destination', c.row_count,
                        'execution_status', 'SUCCESS'
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
        PROCESS_STEP_ID,
        RECORD_TYPE,
        MODEL_NAME,
        STEP_EXECUTION_OBJ,
        (select count(*) from {{ this }}) as row_count
    from {{ log_table }}
    where PROCESS_STEP_ID = '{{ process_step_id }}'
      and RECORD_TYPE = 'MODEL'
      and MODEL_NAME = '{{ model_name }}'
) c
where t.PROCESS_STEP_ID = c.PROCESS_STEP_ID
  and t.RECORD_TYPE = c.RECORD_TYPE
  and t.MODEL_NAME = c.MODEL_NAME

{%- endmacro -%}