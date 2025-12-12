{%- macro log_execution_end() -%}

{#- 
    This macro UPDATES the specific MODEL record when a model FINISHES executing.
    Updates model with SUCCESS status, end time, duration, query ID, and row counts.
    Adds completion event to timeline.
-#}

{% set log_table = 'DEV_PROVIDERPDM.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG' %}
{% set model_name = this.name %}
{% set run_id = invocation_id %}
{% set process_step_id = 'JOB_' ~ run_id %}

{#- Build source dependencies object (graph is fully available in post-hook) -#}
{% set sources_dict = {} %}
{% set source_counter = 1 %}
{% set current_model_id = model.unique_id if model.unique_id is defined else ('model.' ~ project_name ~ '.' ~ this.name) %}

{{ log("=== POST-HOOK DEBUG for " ~ this.name ~ " ===", info=true) }}
{{ log("current_model_id: " ~ current_model_id, info=true) }}

{% if graph is defined and graph.nodes is defined and current_model_id in graph.nodes %}
    {% set current_node = graph.nodes[current_model_id] %}
    {{ log("Found in graph.nodes", info=true) }}
    
    {% if current_node.depends_on is defined and current_node.depends_on.nodes is defined %}
        {{ log("Dependencies found: " ~ (current_node.depends_on.nodes | length), info=true) }}
        {{ log("Full dependency list: " ~ current_node.depends_on.nodes, info=true) }}
        
        {% for node_id in current_node.depends_on.nodes %}
            {{ log("Processing: " ~ node_id, info=true) }}
            {% set node_parts = node_id.split('.') %}
            {% set node_type = node_parts[0] %}
            {{ log("  Type: " ~ node_type, info=true) }}
            
            {% if node_type == 'source' %}
                {% set source_name = node_parts[1] ~ '.' ~ node_parts[2] %}
                {% set source_key = 'source_' ~ source_counter %}
                {% do sources_dict.update({source_key: {'type': 'source', 'name': source_name, 'node_id': node_id}}) %}
                {{ log("  Added SOURCE: " ~ source_name, info=true) }}
                {% set source_counter = source_counter + 1 %}
                
            {% elif node_type == 'model' %}
                {% set ref_name = node_parts[2] %}
                {% set source_key = 'source_' ~ source_counter %}
                {% do sources_dict.update({source_key: {'type': 'ref', 'name': ref_name, 'node_id': node_id}}) %}
                {{ log("  Added REF: " ~ ref_name, info=true) }}
                {% set source_counter = source_counter + 1 %}
                
            {% elif node_type == 'seed' %}
                {% set seed_name = node_parts[2] %}
                {% set source_key = 'source_' ~ source_counter %}
                {% do sources_dict.update({source_key: {'type': 'seed', 'name': seed_name, 'node_id': node_id}}) %}
                {% set source_counter = source_counter + 1 %}
                
            {% elif node_type == 'snapshot' %}
                {% set snapshot_name = node_parts[2] %}
                {% set source_key = 'source_' ~ source_counter %}
                {% do sources_dict.update({source_key: {'type': 'snapshot', 'name': snapshot_name, 'node_id': node_id}}) %}
                {% set source_counter = source_counter + 1 %}
                
            {% elif node_type == 'test' %}
                {% set test_name = node_parts[2] %}
                {% set source_key = 'source_' ~ source_counter %}
                {% do sources_dict.update({source_key: {'type': 'test', 'name': test_name, 'node_id': node_id}}) %}
                {% set source_counter = source_counter + 1 %}
                
            {% elif node_type == 'analysis' %}
                {% set analysis_name = node_parts[2] %}
                {% set source_key = 'source_' ~ source_counter %}
                {% do sources_dict.update({source_key: {'type': 'analysis', 'name': analysis_name, 'node_id': node_id}}) %}
                {% set source_counter = source_counter + 1 %}
                
            {% elif node_type == 'exposure' %}
                {% set exposure_name = node_parts[2] %}
                {% set source_key = 'source_' ~ source_counter %}
                {% do sources_dict.update({source_key: {'type': 'exposure', 'name': exposure_name, 'node_id': node_id}}) %}
                {% set source_counter = source_counter + 1 %}
                
            {% else %}
                {% set unknown_name = node_parts[2] if node_parts | length > 2 else node_id %}
                {% set source_key = 'source_' ~ source_counter %}
                {% do sources_dict.update({source_key: {'type': 'unknown_' ~ node_type, 'name': unknown_name, 'node_id': node_id}}) %}
                {% set source_counter = source_counter + 1 %}
            {% endif %}
        {% endfor %}
        {{ log("Total captured: " ~ (sources_dict | length), info=true) }}
        {{ log("Sources dict: " ~ sources_dict, info=true) }}
    {% else %}
        {{ log("depends_on.nodes NOT available", info=true) }}
    {% endif %}
{% else %}
    {{ log("Model NOT in graph or graph unavailable", info=true) }}
{% endif %}

{#- Build JSON string for sources -#}
{% set sources_json_parts = [] %}
{% for key, val in sources_dict.items() %}
    {% do sources_json_parts.append('"' ~ key ~ '":{"type":"' ~ val.type ~ '","name":"' ~ val.name ~ '","node_id":"' ~ val.node_id ~ '"}') %}
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
