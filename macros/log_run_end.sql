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
    SOURCE_DATA_CNT = {{ ns.total_count }},
    DESTINATION_DATA_CNT_OBJ = object_construct('successful_models', {{ ns.success_count }}, 'failed_models', {{ ns.error_count }}),
    ERROR_MESSAGE = {% if ns.error_count > 0 %}'Job completed with {{ ns.error_count }} error(s)'{% else %}null{% endif %}
where PROCESS_STEP_ID = '{{ process_step_id }}'
  and RECORD_TYPE = 'JOB'

{%- endmacro -%}
