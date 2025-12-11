{%- macro log_execution_start() -%}

{#- 
    This macro UPDATES the single job record when a model STARTS executing.
    Appends model to array with config details and adds timeline event.
    Captures query ID and source dependencies for tracking.
-#}

{% set log_table = 'DEV_PROVIDERPDM.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG' %}
{% set model_name = this.name %}
{% set run_id = invocation_id %}
{% set process_step_id = 'JOB_' ~ run_id %}

{#- Build source dependencies object -#}
{% set sources_dict = {} %}
{% set source_counter = 1 %}

{% if model.depends_on is defined and model.depends_on.nodes is defined %}
    {% for node_id in model.depends_on.nodes %}
        {% set node_parts = node_id.split('.') %}
        {% if node_parts[0] == 'source' %}
            {#- This is a source() reference -#}
            {% set source_name = node_parts[1] ~ '.' ~ node_parts[2] %}
            {% set source_key = 'source_' ~ source_counter %}
            {% do sources_dict.update({source_key: {'type': 'source', 'name': source_name, 'node_id': node_id}}) %}
            {% set source_counter = source_counter + 1 %}
        {% elif node_parts[0] == 'model' %}
            {#- This is a ref() reference -#}
            {% set ref_name = node_parts[2] %}
            {% set source_key = 'source_' ~ source_counter %}
            {% do sources_dict.update({source_key: {'type': 'ref', 'name': ref_name, 'node_id': node_id}}) %}
            {% set source_counter = source_counter + 1 %}
        {% endif %}
    {% endfor %}
{% endif %}

{#- Build JSON string for sources -#}
{% set sources_json_parts = [] %}
{% for key, val in sources_dict.items() %}
    {% do sources_json_parts.append('"' ~ key ~ '":{"type":"' ~ val.type ~ '","name":"' ~ val.name ~ '","node_id":"' ~ val.node_id ~ '"}') %}
{% endfor %}
{% set sources_json = '{' ~ sources_json_parts | join(',') ~ '}' %}

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

merge into {{ log_table }} as target
using (
    select 
        '{{ process_step_id }}' as process_step_id,
        array_append(
            coalesce(STEP_EXECUTION_OBJ:models, parse_json('[]')),
            object_construct(
                'model_name', '{{ model_name }}',
                'database', '{{ this.database }}',
                'schema', '{{ this.schema }}',
                'alias', '{{ this.identifier }}',
                'materialization', '{{ materialization }}',
                'execution_type', '{{ execution_type }}',
                'status', 'RUNNING',
                'start_time', to_varchar(current_timestamp(), 'YYYY-MM-DD HH24:MI:SS.FF3'),
                'end_time', null,
                'duration_seconds', null,
                'query_id_start', LAST_QUERY_ID(),
                'query_id_end', null,
                'rows_affected', null
            )
        ) as new_models_array,
        array_append(
            coalesce(STEP_EXECUTION_OBJ:execution_timeline, parse_json('[]')),
            object_construct(
                'timestamp', to_varchar(current_timestamp(), 'YYYY-MM-DD HH24:MI:SS.FF3'),
                'level', 'Info',
                'title', 'Model Started: {{ model_name }}',
                'content', object_construct('model', '{{ model_name }}', 'materialization', '{{ config.get("materialized", "view") }}')
            )
        ) as new_timeline,
        object_insert(
            coalesce(base.SOURCE_OBJ, parse_json('{}')),
            '{{ model_name }}',
            parse_json('{{ sources_json }}'),
            true
        ) as new_source_obj,
        object_insert(
            coalesce(base.DESTINATION_OBJ, parse_json('{}')),
            '{{ model_name }}',
            object_construct(
                'database', '{{ this.database }}',
                'schema', '{{ this.schema }}',
                'table', '{{ this.identifier }}',
                'materialization', '{{ materialization }}',
                'execution_type', '{{ execution_type }}',
                'full_name', '{{ this.database }}.{{ this.schema }}.{{ this.identifier }}'
            ),
            true
        ) as new_dest_obj
    from {{ log_table }} base
    where base.PROCESS_STEP_ID = '{{ process_step_id }}'
) as source
on target.PROCESS_STEP_ID = source.process_step_id
when matched then update set
    target.UPDATE_TMSTP = current_timestamp(),
    target.EXECUTION_STATUS_NAME = 'RUNNING',
    target.EXECUTION_TYPE_NAME = '{{ execution_type }}',
    target.SOURCE_OBJ = source.new_source_obj,
    target.DESTINATION_OBJ = source.new_dest_obj,
    target.STEP_EXECUTION_OBJ = object_insert(
        object_insert(
            object_insert(
                target.STEP_EXECUTION_OBJ,
                'models',
                source.new_models_array,
                true
            ),
            'execution_timeline',
            source.new_timeline,
            true
        ),
        'current_step',
        'RUNNING: {{ model_name }}',
        true
    )

{%- endmacro -%}
