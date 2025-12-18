{%- macro log_run_end() -%}

{#- 
    This macro updates the JOB record in PROCESS_EXECUTION_LOG when a dbt run ends.
    It uses dbt's 'results' variable to capture ALL model results including failures.
    Adds final timeline event and enhanced error details to the JOB record.
    
    IMPORTANT: Only ONE SQL statement allowed in hooks.
-#}

{% set snowflake_db = env_var('SNOWFLAKE_DATABASE', 'DEV_PROVIDERPDM') %}
{% set log_table = snowflake_db ~ '.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG' %}
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

update {{ log_table }} t
set
    EXECUTION_STATUS_NAME = case 
        when t.RECORD_TYPE = 'JOB' then '{{ job_status }}'
        {% if failed_models | length > 0 %}when t.RECORD_TYPE = 'MODEL' and t.MODEL_NAME in ({% for m in failed_models %}'{{ m.name }}'{% if not loop.last %},{% endif %}{% endfor %}) then 'FAILED'{% endif %}
        else t.EXECUTION_STATUS_NAME
    end,
    EXECUTION_COMPLETED_IND = case
        when t.RECORD_TYPE = 'JOB' then 'Y'
        {% if failed_models | length > 0 %}when t.RECORD_TYPE = 'MODEL' and t.MODEL_NAME in ({% for m in failed_models %}'{{ m.name }}'{% if not loop.last %},{% endif %}{% endfor %}) then 'Y'{% endif %}
        else t.EXECUTION_COMPLETED_IND
    end,
    EXECUTION_END_TMSTP = case
        when t.RECORD_TYPE = 'JOB' then CURRENT_TIMESTAMP()
        {% if failed_models | length > 0 %}when t.RECORD_TYPE = 'MODEL' and t.MODEL_NAME in ({% for m in failed_models %}'{{ m.name }}'{% if not loop.last %},{% endif %}{% endfor %}) then CURRENT_TIMESTAMP(){% endif %}
        else t.EXECUTION_END_TMSTP
    end,
    UPDATE_TMSTP = CURRENT_TIMESTAMP(),
    ERROR_MESSAGE_OBJ = case
        when t.RECORD_TYPE = 'JOB' then {% if ns.error_count > 0 %}object_construct('error_count', {{ ns.error_count }}, 'status', '{{ job_status }}'){% else %}null{% endif %}
        {% for m in failed_models %}
        when t.RECORD_TYPE = 'MODEL' and t.MODEL_NAME = '{{ m.name }}' then object_construct('error_type', 'MODEL_EXECUTION_FAILED', 'error_message', '{{ m.error }}', 'status', 'FAILED')
        {% endfor %}
        else t.ERROR_MESSAGE_OBJ
    end,
    METRICS_OBJ = case
        when t.RECORD_TYPE = 'JOB' then parse_json('null')
        {% for m in failed_models %}
        when t.RECORD_TYPE = 'MODEL' and t.MODEL_NAME = '{{ m.name }}' then object_construct('load_type', 'failed', 'status', 'FAILED', 'error', '{{ m.error }}')
        {% endfor %}
        else t.METRICS_OBJ
    end,
    STEP_EXECUTION_OBJ = case
        when t.RECORD_TYPE = 'JOB' then object_construct('current_step', 'JOB_COMPLETED', 'job_status', '{{ job_status }}', 'total_models', {{ ns.total_count }}, 'successful_models', {{ ns.success_count }}, 'failed_models', {{ ns.error_count }}, 'skipped_models', {{ ns.skip_count }})
        {% for m in failed_models %}
        when t.RECORD_TYPE = 'MODEL' and t.MODEL_NAME = '{{ m.name }}' then 
            object_construct(
                'model_name', t.MODEL_NAME,
                'current_step', 'MODEL_FAILED',
                'status', 'FAILED',
                'execution_timeline', array_append(
                    case
                        when array_size(iff(is_array(t.STEP_EXECUTION_OBJ:execution_timeline), t.STEP_EXECUTION_OBJ:execution_timeline, array_construct())) = 0
                        then array_construct(
                            object_construct(
                                'step_number', 1,
                                'timestamp', to_varchar(t.EXECUTION_START_TMSTP, 'YYYY-MM-DD HH24:MI:SS.FF3'),
                                'level', 'Info',
                                'step_type', 'MODEL_START',
                                'title', 'Model Started: ' || t.MODEL_NAME,
                                'query_id', t.STEP_EXECUTION_OBJ:query_id_start::varchar,
                                'content', object_construct('model', t.MODEL_NAME)
                            )
                        )
                        else iff(is_array(t.STEP_EXECUTION_OBJ:execution_timeline), t.STEP_EXECUTION_OBJ:execution_timeline, array_construct())
                    end,
                    object_construct(
                        'step_number', array_size(
                            case
                                when array_size(iff(is_array(t.STEP_EXECUTION_OBJ:execution_timeline), t.STEP_EXECUTION_OBJ:execution_timeline, array_construct())) = 0
                                then array_construct(
                                    object_construct(
                                        'step_number', 1,
                                        'timestamp', to_varchar(t.EXECUTION_START_TMSTP, 'YYYY-MM-DD HH24:MI:SS.FF3'),
                                        'level', 'Info',
                                        'step_type', 'MODEL_START',
                                        'title', 'Model Started: ' || t.MODEL_NAME,
                                        'query_id', t.STEP_EXECUTION_OBJ:query_id_start::varchar,
                                        'content', object_construct('model', t.MODEL_NAME)
                                    )
                                )
                                else iff(is_array(t.STEP_EXECUTION_OBJ:execution_timeline), t.STEP_EXECUTION_OBJ:execution_timeline, array_construct())
                            end
                        ) + 1,
                        'timestamp', to_varchar(current_timestamp(), 'YYYY-MM-DD HH24:MI:SS.FF3'),
                        'level', 'Error',
                        'step_type', 'MODEL_FAILED',
                        'title', 'Model Failed: ' || t.MODEL_NAME,
                        'error_message', '{{ m.error }}',
                        'content', object_construct(
                            'model', t.MODEL_NAME,
                            'status', 'FAILED',
                            'error', '{{ m.error }}'
                        )
                    )
                )
            )
        {% endfor %}
        else t.STEP_EXECUTION_OBJ
    end
where t.PROCESS_STEP_ID = '{{ process_step_id }}'
  and (t.RECORD_TYPE = 'JOB' {% if failed_models | length > 0 %}or (t.RECORD_TYPE = 'MODEL' and t.MODEL_NAME in ({% for m in failed_models %}'{{ m.name }}'{% if not loop.last %},{% endif %}{% endfor %})){% endif %})

{%- endmacro -%}
