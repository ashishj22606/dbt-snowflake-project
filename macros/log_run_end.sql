{%- macro log_run_end() -%}

{#- 
    This macro updates the SINGLE record in PROCESS_EXECUTION_LOG when a dbt run ends.
    It uses dbt's 'results' variable to capture ALL model results including failures.
    This is the authoritative source for model execution status.
    
    ROBUST ERROR HANDLING: Uses simple, safe SQL to avoid failures in this hook.
-#}

{% set log_table = 'DEV_PROVIDERPDM.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG' %}
{% set run_id = invocation_id %}
{% set process_step_id = 'JOB_' ~ run_id %}

{#- Count results by status safely -#}
{% set ns = namespace(
    success_count=0, 
    error_count=0, 
    skip_count=0,
    total_count=0
) %}

{% if results is defined and results is iterable %}
    {% for res in results %}
        {% set ns.total_count = ns.total_count + 1 %}
        {% if res.status == 'success' %}
            {% set ns.success_count = ns.success_count + 1 %}
        {% elif res.status == 'error' %}
            {% set ns.error_count = ns.error_count + 1 %}
        {% elif res.status == 'skipped' %}
            {% set ns.skip_count = ns.skip_count + 1 %}
        {% endif %}
    {% endfor %}
{% endif %}

{#- Determine overall job status -#}
{% if ns.error_count > 0 %}
    {% set job_status = 'FAILED' %}
{% else %}
    {% set job_status = 'SUCCESS' %}
{% endif %}

{#- First update: Set basic completion status (this should always work) -#}
update {{ log_table }}
set
    EXECUTION_STATUS_NAME = '{{ job_status }}',
    EXECUTION_COMPLETED_IND = 'Y',
    EXECUTION_END_TMSTP = CURRENT_TIMESTAMP(),
    EXTRACT_END_TMSTP = CURRENT_TIMESTAMP(),
    UPDATE_TMSTP = CURRENT_TIMESTAMP(),
    DESTINATION_DATA_CNT_OBJ = parse_json('{"total_models": {{ ns.total_count }}, "success": {{ ns.success_count }}, "failed": {{ ns.error_count }}, "skipped": {{ ns.skip_count }}}'),
    STEP_EXECUTION_OBJ = object_insert(
        object_insert(
            STEP_EXECUTION_OBJ,
            'current_step',
            'JOB_COMPLETED',
            true
        ),
        'summary',
        parse_json('{"total": {{ ns.total_count }}, "success": {{ ns.success_count }}, "error": {{ ns.error_count }}, "skipped": {{ ns.skip_count }}}'),
        true
    ),
    ERROR_MESSAGE_OBJ = {% if ns.error_count > 0 %}parse_json('{"error_count": {{ ns.error_count }}, "message": "{{ ns.error_count }} model(s) failed"}'){% else %}parse_json('null'){% endif %}
where PROCESS_STEP_ID = '{{ process_step_id }}';

{#- Second update: Add detailed model info (separate to isolate potential JSON errors) -#}
{% if results is defined and results is iterable %}
update {{ log_table }}
set
    STEP_EXECUTION_OBJ = object_insert(
        STEP_EXECUTION_OBJ,
        'models',
        parse_json('[
            {% for res in results %}
            {
                "model_name": "{{ res.node.name | default('unknown') }}",
                "status": "{{ 'SUCCESS' if res.status == 'success' else ('FAILED' if res.status == 'error' else ('SKIPPED' if res.status == 'skipped' else res.status | upper)) }}",
                "execution_time_seconds": {{ res.execution_time | default(0) | round(2) }}
            }{% if not loop.last %},{% endif %}
            {% endfor %}
        ]'),
        true
    ),
    UPDATE_TMSTP = CURRENT_TIMESTAMP()
where PROCESS_STEP_ID = '{{ process_step_id }}';
{% endif %}

{%- endmacro -%}
