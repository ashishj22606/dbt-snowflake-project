{%- macro log_run_end() -%}

{#- 
    This macro updates the SINGLE record in PROCESS_EXECUTION_LOG when a dbt run ends.
    It uses dbt's 'results' variable to capture ALL model results including failures.
    
    IMPORTANT: Only ONE SQL statement allowed in hooks.
-#}

{% set log_table = 'DEV_PROVIDERPDM.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG' %}
{% set run_id = invocation_id %}
{% set process_step_id = 'JOB_' ~ run_id %}

{#- Count results by status safely -#}
{% set ns = namespace(
    success_count=0, 
    error_count=0, 
    skip_count=0,
    total_count=0,
    models_list=[]
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

update {{ log_table }}
set
    EXECUTION_STATUS_NAME = '{{ job_status }}',
    EXECUTION_COMPLETED_IND = 'Y',
    EXECUTION_END_TMSTP = CURRENT_TIMESTAMP(),
    EXTRACT_END_TMSTP = CURRENT_TIMESTAMP(),
    UPDATE_TMSTP = CURRENT_TIMESTAMP(),
    DESTINATION_DATA_CNT_OBJ = parse_json('{"total_models":{{ ns.total_count }},"success":{{ ns.success_count }},"failed":{{ ns.error_count }},"skipped":{{ ns.skip_count }}}'),
    STEP_EXECUTION_OBJ = parse_json('{"current_step":"JOB_COMPLETED","job_status":"{{ job_status }}","summary":{"total":{{ ns.total_count }},"success":{{ ns.success_count }},"error":{{ ns.error_count }},"skipped":{{ ns.skip_count }}},"models":{{ models_json }}}'),
    ERROR_MESSAGE_OBJ = {% if ns.error_count > 0 %}parse_json('{"error_count":{{ ns.error_count }},"message":"{{ ns.error_count }} model(s) failed"}'){% else %}parse_json('null'){% endif %}
where PROCESS_STEP_ID = '{{ process_step_id }}'

{%- endmacro -%}
