{%- macro log_execution_end() -%}

{% set snowflake_db = env_var('SNOWFLAKE_DATABASE', 'DEV_PROVIDERPDM') %}
{% set log_table = snowflake_db ~ '.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG' %}
{% set model_name = this.name %}
{% set run_id = invocation_id %}
{% set process_step_id = 'JOB_' ~ run_id %}

update {{ log_table }}
set
    EXECUTION_STATUS_NAME = 'SUCCESS',
    EXECUTION_COMPLETED_IND = 'Y',
    EXECUTION_END_TMSTP = CURRENT_TIMESTAMP(),
    EXTRACT_END_TMSTP = CURRENT_TIMESTAMP(),
    UPDATE_TMSTP = CURRENT_TIMESTAMP(),
    SOURCE_DATA_CNT = (select count(*) from {{ this }}),
    DESTINATION_DATA_CNT_OBJ = (select count(*) from {{ this }}),
    STEP_EXECUTION_OBJ = object_insert(
        object_insert(
            object_insert(
                STEP_EXECUTION_OBJ,
                'current_step',
                'MODEL_COMPLETED',
                true
            ),
            'query_id_end',
            LAST_QUERY_ID(),
            true
        ),
        'execution_timeline',
        array_append(
            coalesce(STEP_EXECUTION_OBJ:execution_timeline, parse_json('[]')),
            object_construct(
                'step_number', array_size(coalesce(STEP_EXECUTION_OBJ:execution_timeline, parse_json('[]'))) + 1,
                'timestamp', to_varchar(current_timestamp(), 'YYYY-MM-DD HH24:MI:SS.FF3'),
                'level', 'Info',
                'step_type', 'MODEL_COMPLETE',
                'title', 'Model Completed: {{ model_name }}',
                'query_id', LAST_QUERY_ID(),
                'query_result', object_construct(
                    'rows_in_destination', (select count(*) from {{ this }}),
                    'execution_status', 'SUCCESS'
                ),
                'content', object_construct(
                    'model', '{{ model_name }}',
                    'status', 'SUCCESS',
                    'rows_processed', (select count(*) from {{ this }}),
                    'destination_table', '{{ this.database }}.{{ this.schema }}.{{ this.identifier }}'
                )
            )
        ),
        true
    )
where PROCESS_STEP_ID = '{{ process_step_id }}'
  and RECORD_TYPE = 'MODEL'
  and MODEL_NAME = '{{ model_name }}'

{%- endmacro -%}
