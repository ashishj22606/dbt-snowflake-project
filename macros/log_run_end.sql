{%- macro log_run_end() -%}

{#- 
    This macro updates the SINGLE record in PROCESS_EXECUTION_LOG when a dbt run ends.
    It uses dbt's 'results' variable to capture ALL model results including failures.
    Adds final timeline event and enhanced error details.
    
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

{#- Build models JSON array -#}
{% set models_json_parts = [] %}
{% for m in ns.models_list %}
    {% do models_json_parts.append('{"model_name":"' ~ m.name ~ '","status":"' ~ m.status ~ '","execution_time_seconds":' ~ m.time ~ '}') %}
{% endfor %}
{% set models_json = '[' ~ models_json_parts | join(',') ~ ']' %}

{#- Build error details JSON array -#}
{% set error_json_parts = [] %}
{% for e in ns.error_list %}
    {% do error_json_parts.append('{"model_name":"' ~ e.model ~ '","error_type":"MODEL_EXECUTION_FAILED","error_message":"' ~ e.error ~ '","execution_time_seconds":' ~ e.time ~ '}') %}
{% endfor %}
{% set error_json = '[' ~ error_json_parts | join(',') ~ ']' %}

merge into {{ log_table }} as target
using (
    select 
        '{{ process_step_id }}' as process_step_id,
        parse_json('{{ models_json }}') as final_models_summary,
        base.STEP_EXECUTION_OBJ as current_step_obj,
        base.STEP_EXECUTION_OBJ:execution_timeline as current_timeline
    from {{ log_table }} base
    where base.PROCESS_STEP_ID = '{{ process_step_id }}'
) as source
on target.PROCESS_STEP_ID = source.process_step_id
when matched then update set
    target.EXECUTION_STATUS_NAME = '{{ job_status }}',
    target.EXECUTION_COMPLETED_IND = 'Y',
    target.EXECUTION_END_TMSTP = CURRENT_TIMESTAMP(),
    target.EXTRACT_END_TMSTP = CURRENT_TIMESTAMP(),
    target.UPDATE_TMSTP = CURRENT_TIMESTAMP(),
    target.STEP_EXECUTION_OBJ = object_insert(
        object_insert(
            object_insert(
                object_insert(
                    coalesce(source.current_step_obj, parse_json('{}')),
                    'current_step',
                    'JOB_COMPLETED',
                    true
                ),
                'job_status',
                '{{ job_status }}',
                true
            ),
            'summary',
            object_construct(
                'total', {{ ns.total_count }},
                'success', {{ ns.success_count }},
                'error', {{ ns.error_count }},
                'skipped', {{ ns.skip_count }}
            ),
            true
        ),
        'execution_timeline',
        array_append(
            coalesce(source.current_timeline, parse_json('[]')),
            object_construct(
                'step_number', array_size(coalesce(source.current_timeline, parse_json('[]'))) + 1,
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
                    'models_summary', source.final_models_summary
                )
            )
        ),
        true
    ),
    target.ERROR_MESSAGE_OBJ = {% if ns.error_count > 0 %}parse_json('{"error_count":{{ ns.error_count }},"errors":{{ error_json }}}'){% else %}parse_json('null'){% endif %}

{%- endmacro -%}
