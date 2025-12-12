{%- macro log_execution_start() -%}

{#- 
    This macro INSERTS a new MODEL record when a model STARTS executing.
    Each model gets its own record with RECORD_TYPE='MODEL' and the same PROCESS_STEP_ID as the job.
    MODEL_NAME identifies the specific model within the job.
    Captures query ID and source dependencies for tracking.
-#}

{% set log_table = 'DEV_PROVIDERPDM.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG' %}
{% set model_name = this.name %}
{% set run_id = invocation_id %}
{% set process_step_id = 'JOB_' ~ run_id %}

{#- Dependencies will be captured in log_execution_end (post-hook) where graph is fully available -#}
{% set sources_json = '{}' %}

{#- Determine execution type based on materialization and full_refresh -#}
{% set materialization = config.get("materialized", "view") %}
{% set is_full_refresh = flags.FULL_REFRESH | default(false) %}
{% if materialization == 'incremental' %}
    {% if is_full_refresh %}
        {% set execution_type = 'TRUNCATE_FULL_LOAD' %}
    {% else %}
        {% set execution_type = 'INCREMENTAL_LOAD' %}
    {% endif %}
{% elif materialization == 'table' %}
    {% set execution_type = 'TRUNCATE_FULL_LOAD' %}
{% elif materialization == 'view' %}
    {% set execution_type = 'VIEW_REFRESH' %}
{% elif materialization == 'ephemeral' %}
    {% set execution_type = 'CTE_EPHEMERAL' %}
{% else %}
    {% set execution_type = 'FULL_LOAD' %}
{% endif %}

insert into {{ log_table }} (
    PROCESS_CONFIG_SK,
    PROCESS_STEP_ID,
    RECORD_TYPE,
    MODEL_NAME,
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
    'MODEL' as RECORD_TYPE,
    '{{ model_name }}' as MODEL_NAME,
    'RUNNING' as EXECUTION_STATUS_NAME,
    'N' as EXECUTION_COMPLETED_IND,
    CURRENT_TIMESTAMP() as EXECUTION_START_TMSTP,
    null::TIMESTAMP_NTZ as EXECUTION_END_TMSTP,
    parse_json('{{ sources_json }}') as SOURCE_OBJ,
    object_construct(
        'database', '{{ this.database }}',
        'schema', '{{ this.schema }}',
        'table', '{{ this.identifier }}',
        'materialization', '{{ materialization }}',
        'execution_type', '{{ execution_type }}',
        'full_name', '{{ this.database }}.{{ this.schema }}.{{ this.identifier }}'
    ) as DESTINATION_OBJ,
    object_construct(
        'model_name', '{{ model_name }}',
        'materialization', '{{ materialization }}',
        'execution_type', '{{ execution_type }}'
    ) as PROCESS_CONFIG_OBJ,
    0 as SOURCE_DATA_CNT,
    parse_json('null') as DESTINATION_DATA_CNT_OBJ,
    '{{ execution_type }}' as EXECUTION_TYPE_NAME,
    CURRENT_TIMESTAMP() as EXTRACT_START_TMSTP,
    null::TIMESTAMP_NTZ as EXTRACT_END_TMSTP,
    parse_json('null') as ERROR_MESSAGE_OBJ,
    object_construct(
        'model_name', '{{ model_name }}',
        'current_step', 'MODEL_STARTED',
        'query_id_start', LAST_QUERY_ID(),
        'query_id_end', null,
        'execution_timeline', parse_json('[' ||
            '{"step_number":1,' ||
            '"timestamp":"' || to_varchar(current_timestamp(), 'YYYY-MM-DD HH24:MI:SS.FF3') || '",' ||
            '"level":"Info",' ||
            '"step_type":"MODEL_START",' ||
            '"title":"Model Started: {{ model_name }}",' ||
            '"query_id":"' || LAST_QUERY_ID() || '",' ||
            '"content":{"model":"{{ model_name }}","materialization":"{{ materialization }}","execution_type":"{{ execution_type }}","database":"{{ this.database }}","schema":"{{ this.schema }}","table":"{{ this.identifier }}"}}' ||
            ']')
    ) as STEP_EXECUTION_OBJ,
    CURRENT_TIMESTAMP() as INSERT_TMSTP,
    CURRENT_TIMESTAMP() as UPDATE_TMSTP,
    'N' as DELETED_IND

{%- endmacro -%}
