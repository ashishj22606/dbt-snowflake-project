{%- macro log_execution_end() -%}

{% set snowflake_db = env_var('SNOWFLAKE_DATABASE', 'DEV_PROVIDERPDM') %}
{% set log_table = snowflake_db ~ '.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG' %}
{% set model_name = this.name %}
{% set run_id = invocation_id %}
{% set process_step_id = 'JOB_' ~ run_id %}

merge into {{ log_table }} as target
using (
    select 
        PROCESS_STEP_ID,
        STEP_EXECUTION_OBJ,
        (select count(*) from {{ this }}) as row_count
    from {{ log_table }}
    where PROCESS_STEP_ID = '{{ process_step_id }}'
      and RECORD_TYPE = 'MODEL'
      and MODEL_NAME = '{{ model_name }}'
) as source
on target.PROCESS_STEP_ID = source.PROCESS_STEP_ID 
   and target.RECORD_TYPE = 'MODEL'
   and target.MODEL_NAME = '{{ model_name }}'
when matched then update set
    target.EXECUTION_STATUS_NAME = 'SUCCESS',
    target.EXECUTION_COMPLETED_IND = 'Y',
    target.EXECUTION_END_TMSTP = CURRENT_TIMESTAMP(),
    target.EXTRACT_END_TMSTP = CURRENT_TIMESTAMP(),
    target.UPDATE_TMSTP = CURRENT_TIMESTAMP(),
    target.SOURCE_DATA_CNT = source.row_count,
    target.DESTINATION_DATA_CNT_OBJ = source.row_count,
    target.STEP_EXECUTION_OBJ = object_construct(
        'model_name', source.STEP_EXECUTION_OBJ:model_name::varchar,
        'current_step', 'MODEL_COMPLETED',
        'query_id_start', source.STEP_EXECUTION_OBJ:query_id_start::varchar,
        'query_id_end', LAST_QUERY_ID(),
        'execution_timeline', 
            array_append(
                source.STEP_EXECUTION_OBJ:execution_timeline,
                object_construct(
                    'step_number', array_size(source.STEP_EXECUTION_OBJ:execution_timeline) + 1,
                    'timestamp', to_varchar(current_timestamp(), 'YYYY-MM-DD HH24:MI:SS.FF3'),
                    'level', 'Info',
                    'step_type', 'MODEL_COMPLETE',
                    'title', 'Model Completed: {{ model_name }}',
                    'query_id', LAST_QUERY_ID(),
                    'query_result', object_construct(
                        'rows_in_destination', source.row_count,
                        'execution_status', 'SUCCESS'
                    ),
                    'content', object_construct(
                        'model', '{{ model_name }}',
                        'status', 'SUCCESS',
                        'rows_processed', source.row_count,
                        'destination_table', '{{ this.database }}.{{ this.schema }}.{{ this.identifier }}'
                    )
                )
            )
    )
  and MODEL_NAME = '{{ model_name }}'

{%- endmacro -%}