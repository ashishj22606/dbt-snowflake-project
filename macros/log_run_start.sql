{%- macro log_run_start() -%}

{#- 
    This macro inserts a SINGLE record into PROCESS_EXECUTION_LOG when a dbt run starts.
    Captures enhanced configuration and metadata for ETL tracking.
-#}

{% set log_table = 'DEV_PROVIDERPDM.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG' %}
{% set run_id = invocation_id %}
{% set process_step_id = 'JOB_' ~ run_id %}

insert into {{ log_table }} (
    PROCESS_CONFIG_SK,
    PROCESS_STEP_ID,
    EXECUTION_STATUS_NAME,
    EXECUTION_COMPLETED_IND,
    EXECUTION_START_TMSTP,
    EXECUTION_END_TMSTP,
    SOURCE_OBJ,
    DESTINATION_OBJ,
    PROCESS_CONFIG_OBJ,
    SOURCE_DATA_CNT,
    DESTINATION_DATA_CNT_OBJ,
    EXECUTION_TYPE_NAME,
    EXTRACT_START_TMSTP,
    EXTRACT_END_TMSTP,
    ERROR_MESSAGE_OBJ,
    STEP_EXECUTION_OBJ,
    INSERT_TMSTP,
    UPDATE_TMSTP,
    DELETED_IND
)
select
    null as PROCESS_CONFIG_SK,
    '{{ process_step_id }}' as PROCESS_STEP_ID,
    'RUNNING' as EXECUTION_STATUS_NAME,
    'N' as EXECUTION_COMPLETED_IND,
    CURRENT_TIMESTAMP() as EXECUTION_START_TMSTP,
    null as EXECUTION_END_TMSTP,
    object_construct(
        'source_type', 'DBT_PROJECT',
        'project_name', '{{ project_name }}',
        'dbt_version', '{{ dbt_version }}',
        'run_started_at', '{{ run_started_at }}'
    ) as SOURCE_OBJ,
    object_construct(
        'target_name', '{{ target.name }}',
        'target_schema', '{{ target.schema }}',
        'target_database', '{{ target.database }}',
        'target_type', '{{ target.type }}',
        'threads', {{ threads }},
        'warehouse', '{{ target.warehouse | default("N/A") }}'
    ) as DESTINATION_OBJ,
    object_construct(
        'invocation_id', '{{ run_id }}',
        'project_name', '{{ project_name }}',
        'target_name', '{{ target.name }}',
        'dbt_version', '{{ dbt_version }}',
        'run_started_at', '{{ run_started_at }}',
        'which', '{{ invocation_args_dict.get("which", "run") }}',
        'full_refresh', {{ flags.FULL_REFRESH | default(false) | lower }}
    ) as PROCESS_CONFIG_OBJ,
    0 as SOURCE_DATA_CNT,
    parse_json('{"total_models": 0, "success": 0, "failed": 0, "skipped": 0, "running": 0}') as DESTINATION_DATA_CNT_OBJ,
    'DBT_JOB_RUN' as EXECUTION_TYPE_NAME,
    CURRENT_TIMESTAMP() as EXTRACT_START_TMSTP,
    null as EXTRACT_END_TMSTP,
    parse_json('{"errors":[]}') as ERROR_MESSAGE_OBJ,
    object_construct(
        'models', parse_json('[]'),
        'current_step', 'JOB_STARTED',
        'execution_timeline', array_construct(
            object_construct(
                'timestamp', to_varchar(current_timestamp(), 'YYYY-MM-DD HH24:MI:SS.FF3'),
                'level', 'Info',
                'title', 'Job Started',
                'content', object_construct('invocation_id', '{{ run_id }}', 'project', '{{ project_name }}')
            )
        )
    ) as STEP_EXECUTION_OBJ,
    CURRENT_TIMESTAMP() as INSERT_TMSTP,
    CURRENT_TIMESTAMP() as UPDATE_TMSTP,
    'N' as DELETED_IND

{%- endmacro -%}
