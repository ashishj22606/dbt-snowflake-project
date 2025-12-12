{%- macro log_run_end() -%}

{#- 
    This macro updates the JOB record in PROCESS_EXECUTION_LOG when a dbt run ends.
    It uses dbt's 'results' variable to capture ALL model results including failures.
    Adds final timeline event and enhanced error details to the JOB record.
    
    IMPORTANT: Only ONE SQL statement allowed in hooks.
-#}

{% set log_table = 'DEV_PROVIDERPDM.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG' %}
{% set run_id = invocation_id %}
{% set process_step_id = 'JOB_' ~ run_id %}

{#- Count results by status and collect error details -#}
{% set ns = namespace(
    success_count=0, 
    error_count=0, 
    skip_count=0,
    total_count=0,
    models_list=[],
    error_list=[]
) %}

{% if results is defined and results is iterable %}
    {% for res in results %}
        {% set ns.total_count = ns.total_count + 1 %}
        {% if res.status == 'success' %}
            {% set ns.success_count = ns.success_count + 1 %}
            {% set status_str = 'SUCCESS' %}
        {% elif res.status == 'error' %}
            {% set ns.error_count = ns.error_count + 1 %}
            {% set status_str = 'FAILED' %}
            {#- Collect error details -#}
            {% set error_msg = res.message | default('Unknown error') | replace('"', '\\"') | replace('\n', ' ') | replace('\r', '') %}
            {% do ns.error_list.append({
                'model': res.node.name | default('unknown'),
                'error': error_msg[:500],
                'time': res.execution_time | default(0) | round(2)
            }) %}
        {% elif res.status == 'skipped' %}
            {% set ns.skip_count = ns.skip_count + 1 %}
            {% set status_str = 'SKIPPED' %}
        {% else %}
            {% set status_str = res.status | upper | default('UNKNOWN') %}
        {% endif %}
        {% do ns.models_list.append({
            'name': res.node.name | default('unknown'),
            'status': status_str,
            'time': res.execution_time | default(0) | round(2)
        }) %}
    {% endfor %}
{% endif %}

{#- Determine overall job status -#}
{% if ns.error_count > 0 %}
    {% set job_status = 'FAILED' %}
{% else %}
    {% set job_status = 'SUCCESS' %}
{% endif %}

{#- Build models summary using object_construct (safer than JSON strings) -#}
{#- We'll build this in SQL using array_construct and object_construct -#}

update {{ log_table }}
set
    EXECUTION_STATUS_NAME = '{{ job_status }}',
    EXECUTION_COMPLETED_IND = 'Y',
    EXECUTION_END_TMSTP = CURRENT_TIMESTAMP(),
    EXTRACT_END_TMSTP = CURRENT_TIMESTAMP(),
    UPDATE_TMSTP = CURRENT_TIMESTAMP(),
    SOURCE_DATA_CNT = (
        select coalesce(sum(SOURCE_DATA_CNT), 0) 
        from {{ log_table }} 
        where PROCESS_STEP_ID = '{{ process_step_id }}' 
        and RECORD_TYPE = 'MODEL'
    ),
    DESTINATION_DATA_CNT_OBJ = object_construct(
        'total_models', {{ ns.total_count }},
        'successful_models', {{ ns.success_count }},
        'failed_models', {{ ns.error_count }},
        'total_rows', (
            select coalesce(sum(SOURCE_DATA_CNT), 0) 
            from {{ log_table }} 
            where PROCESS_STEP_ID = '{{ process_step_id }}' 
            and RECORD_TYPE = 'MODEL'
        )
    ),
    STEP_EXECUTION_OBJ = object_insert(
        object_insert(
            object_insert(
                STEP_EXECUTION_OBJ,
                'current_step',
                'JOB_COMPLETED'
            ),
            'job_status',
            '{{ job_status }}'
        ),
        'execution_timeline',
        array_append(
            coalesce(STEP_EXECUTION_OBJ:execution_timeline, parse_json('[]')),
            object_construct(
                'step_number', array_size(coalesce(STEP_EXECUTION_OBJ:execution_timeline, parse_json('[]'))) + 1,
                'timestamp', to_varchar(current_timestamp(), 'YYYY-MM-DD HH24:MI:SS.FF3'),
                'level', '{% if ns.error_count > 0 %}Error{% else %}Info{% endif %}',
                'step_type', 'JOB_COMPLETE',
                'title', 'Job Completed: {{ job_status }}',
                'query_id', null,
                'query_result', object_construct(
                    'total_models', {{ ns.total_count }},
                    'successful_models', {{ ns.success_count }},
                    'failed_models', {{ ns.error_count }},
                    'skipped_models', {{ ns.skip_count }},
                    'final_status', '{{ job_status }}'
                ),
                'content', object_construct(
                    'job_status', '{{ job_status }}',
                    'total_models', {{ ns.total_count }},
                    'successful_models', {{ ns.success_count }},
                    'failed_models', {{ ns.error_count }},
                    'skipped_models', {{ ns.skip_count }},
                    'models_summary', array_construct(
                        {%- for m in ns.models_list %}
                        object_construct('model_name', '{{ m.name }}', 'status', '{{ m.status }}', 'execution_time_seconds', {{ m.time }})
                        {%- if not loop.last %},{% endif %}
                        {%- endfor %}
                    )
                )
            )
        )
    ),
    ERROR_MESSAGE_OBJ = {% if ns.error_count > 0 %}object_construct(
        'error_count', {{ ns.error_count }},
        'errors', array_construct(
            {%- for e in ns.error_list %}
            object_construct('model_name', '{{ e.model }}', 'error_type', 'MODEL_EXECUTION_FAILED', 'error_message', '{{ e.error | replace("'", "''") }}', 'execution_time_seconds', {{ e.time }})
            {%- if not loop.last %},{% endif %}
            {%- endfor %}
        )
    ){% else %}null{% endif %}
where PROCESS_STEP_ID = '{{ process_step_id }}'
  and RECORD_TYPE = 'JOB'

{%- endmacro -%}
