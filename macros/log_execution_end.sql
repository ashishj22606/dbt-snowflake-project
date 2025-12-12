{%- macro log_execution_end() -%}

{% set log_table = 'DEV_PROVIDERPDM.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG' %}
{% set model_name = this.name %}
{% set run_id = invocation_id %}
{% set process_step_id = 'JOB_' ~ run_id %}

{% set sources_dict = {} %}
{% set current_model_id = model.unique_id if model.unique_id is defined else ('model.' ~ project_name ~ '.' ~ this.name) %}

{% if graph is defined and graph.nodes is defined and current_model_id in graph.nodes %}
    {% set current_node = graph.nodes[current_model_id] %}
    {% if current_node.depends_on is defined and current_node.depends_on.nodes is defined %}
        {% set dependency_nodes = current_node.depends_on.nodes %}
        {% for i in range(dependency_nodes | length) %}
            {% set node_id = dependency_nodes[i] %}
            {% set source_key = 'source_' ~ (i + 1) %}
            {% set node_parts = node_id.split('.') %}
            {% set node_type = node_parts[0] %}
            {% if node_type == 'source' %}
                {% set source_name = node_parts[1] ~ '.' ~ node_parts[2] %}
                {% do sources_dict.update({source_key: {'type': 'source', 'name': source_name, 'node_id': node_id}}) %}
            {% elif node_type == 'model' %}
                {% set ref_name = node_parts[2] %}
                {% do sources_dict.update({source_key: {'type': 'ref', 'name': ref_name, 'node_id': node_id}}) %}
            {% elif node_type == 'seed' %}
                {% set seed_name = node_parts[2] %}
                {% do sources_dict.update({source_key: {'type': 'seed', 'name': seed_name, 'node_id': node_id}}) %}
            {% elif node_type == 'snapshot' %}
                {% set snapshot_name = node_parts[2] %}
                {% do sources_dict.update({source_key: {'type': 'snapshot', 'name': snapshot_name, 'node_id': node_id}}) %}
            {% endif %}
        {% endfor %}
    {% endif %}
{% endif %}

{% set sources_json_parts = [] %}
{% for key, val in sources_dict.items() %}
    {% do sources_json_parts.append('\"' ~ key ~ '\":{\"type\":\"' ~ val.type ~ '\",\"name\":\"' ~ val.name ~ '\",\"node_id\":\"' ~ val.node_id ~ '\"}') %}
{% endfor %}
{% set sources_json = '{' ~ sources_json_parts | join(',') ~ '}' %}

update {{ log_table }}
set
    EXECUTION_STATUS_NAME = 'SUCCESS',
    EXECUTION_COMPLETED_IND = 'Y',
    EXECUTION_END_TMSTP = CURRENT_TIMESTAMP(),
    EXTRACT_END_TMSTP = CURRENT_TIMESTAMP(),
    UPDATE_TMSTP = CURRENT_TIMESTAMP(),
    SOURCE_OBJ = parse_json('{{ sources_json }}'),
    SOURCE_DATA_CNT = (select count(*) from {{ this }}),
    DESTINATION_DATA_CNT_OBJ = (select count(*) from {{ this }}),
    STEP_EXECUTION_OBJ = object_insert(
        object_insert(
            object_insert(
                STEP_EXECUTION_OBJ,
                'current_step',
                'MODEL_COMPLETED',
                true
            ),
            'query_id_end',
            LAST_QUERY_ID(),
            true
        ),
        'execution_timeline',
        array_append(
            coalesce(STEP_EXECUTION_OBJ:execution_timeline, parse_json('[]')),
            object_construct(
                'step_number', array_size(coalesce(STEP_EXECUTION_OBJ:execution_timeline, parse_json('[]'))) + 1,
                'timestamp', to_varchar(current_timestamp(), 'YYYY-MM-DD HH24:MI:SS.FF3'),
                'level', 'Info',
                'step_type', 'MODEL_COMPLETE',
                'title', 'Model Completed: {{ model_name }}',
                'query_id', LAST_QUERY_ID(),
                'query_result', object_construct(
                    'rows_in_destination', (select count(*) from {{ this }}),
                    'execution_status', 'SUCCESS'
                ),
                'content', object_construct(
                    'model', '{{ model_name }}',
                    'status', 'SUCCESS',
                    'rows_processed', (select count(*) from {{ this }}),
                    'destination_table', '{{ this.database }}.{{ this.schema }}.{{ this.identifier }}'
                )
            )
        ),
        true
    )
where PROCESS_STEP_ID = '{{ process_step_id }}'
  and RECORD_TYPE = 'MODEL'
  and MODEL_NAME = '{{ model_name }}'

{%- endmacro -%}
