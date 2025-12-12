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

{#- Build list of failed model names and their error messages -#}
{% set failed_models = [] %}
{% for res in results %}
    {% if res.status == 'error' %}
        {% set error_msg = res.message | default('Unknown error') | replace("'", "''") | replace('\n', ' ') | replace('\r', '') %}
        {% do failed_models.append({'name': res.node.name, 'error': error_msg[:500]}) %}
    {% endif %}
{% endfor %}

update {{ log_table }}
set
    EXECUTION_STATUS_NAME = case 
        when RECORD_TYPE = 'JOB' then '{{ job_status }}'
        when RECORD_TYPE = 'MODEL' and MODEL_NAME in ({% for m in failed_models %}'{{ m.name }}'{% if not loop.last %},{% endif %}{% endfor %}) then 'FAILED'
        else EXECUTION_STATUS_NAME
    end,
    EXECUTION_COMPLETED_IND = case
        when RECORD_TYPE = 'JOB' then 'Y'
        when RECORD_TYPE = 'MODEL' and MODEL_NAME in ({% for m in failed_models %}'{{ m.name }}'{% if not loop.last %},{% endif %}{% endfor %}) then 'Y'
        else EXECUTION_COMPLETED_IND
    end,
    EXECUTION_END_TMSTP = case
        when RECORD_TYPE = 'JOB' then CURRENT_TIMESTAMP()
        when RECORD_TYPE = 'MODEL' and MODEL_NAME in ({% for m in failed_models %}'{{ m.name }}'{% if not loop.last %},{% endif %}{% endfor %}) then CURRENT_TIMESTAMP()
        else EXECUTION_END_TMSTP
    end,
    EXTRACT_END_TMSTP = case
        when RECORD_TYPE = 'JOB' then CURRENT_TIMESTAMP()
        when RECORD_TYPE = 'MODEL' and MODEL_NAME in ({% for m in failed_models %}'{{ m.name }}'{% if not loop.last %},{% endif %}{% endfor %}) then CURRENT_TIMESTAMP()
        else EXTRACT_END_TMSTP
    end,
    UPDATE_TMSTP = CURRENT_TIMESTAMP(),
    SOURCE_DATA_CNT = case
        when RECORD_TYPE = 'JOB' then {{ ns.total_count }}
        else SOURCE_DATA_CNT
    end,
    DESTINATION_DATA_CNT_OBJ = case
        when RECORD_TYPE = 'JOB' then object_construct('successful_models', {{ ns.success_count }}, 'failed_models', {{ ns.error_count }})
        else DESTINATION_DATA_CNT_OBJ
    end,
    ERROR_MESSAGE_OBJ = case
        when RECORD_TYPE = 'JOB' then {% if ns.error_count > 0 %}object_construct('error_count', {{ ns.error_count }}, 'status', '{{ job_status }}'){% else %}null{% endif %}
        {% for m in failed_models %}
        when RECORD_TYPE = 'MODEL' and MODEL_NAME = '{{ m.name }}' then object_construct('error_type', 'MODEL_EXECUTION_FAILED', 'error_message', '{{ m.error }}', 'status', 'FAILED')
        {% endfor %}
        else ERROR_MESSAGE_OBJ
    end
where PROCESS_STEP_ID = '{{ process_step_id }}'
  and (RECORD_TYPE = 'JOB' {% if failed_models | length > 0 %}or (RECORD_TYPE = 'MODEL' and MODEL_NAME in ({% for m in failed_models %}'{{ m.name }}'{% if not loop.last %},{% endif %}{% endfor %})){% endif %})

{%- endmacro -%}
