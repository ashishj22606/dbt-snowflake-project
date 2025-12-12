{%- macro log_execution_start() -%}

{#- 
    This macro INSERTS a new MODEL record when a model STARTS executing.
    Each model gets its own record with RECORD_TYPE='MODEL' and the same PROCESS_STEP_ID as the job.
    MODEL_NAME identifies the specific model within the job.
    Captures query ID and source dependencies for tracking.
-#}

{% set log_table = 'DEV_PROVIDERPDM.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG' %}
{% set model_name = this.name %}
{% set run_id = invocation_id %}
{% set process_step_id = 'JOB_' ~ run_id %}

{#- Build source dependencies object -#}
{% set sources_dict = {} %}
{% set source_counter = 1 %}

{#- Get the current model's unique_id -#}
{% set current_model_id = model.unique_id if model.unique_id is defined else ('model.' ~ project_name ~ '.' ~ this.name) %}

{{ log("=== DEPENDENCY DEBUG for " ~ this.name ~ " ===", info=true) }}
{{ log("Model unique_id: " ~ current_model_id, info=true) }}
{{ log("Graph defined: " ~ (graph is defined), info=true) }}
{{ log("Graph.nodes defined: " ~ (graph.nodes is defined if graph is defined else false), info=true) }}

{#- Try to get dependencies from the graph object (most reliable in hooks) -#}
{% if graph is defined and graph.nodes is defined and current_model_id in graph.nodes %}
    {% set current_node = graph.nodes[current_model_id] %}
    {{ log("Found model in graph.nodes", info=true) }}
    {{ log("Dependencies count: " ~ (current_node.depends_on.nodes | length if current_node.depends_on is defined and current_node.depends_on.nodes is defined else 0), info=true) }}
    
    {% if current_node.depends_on is defined and current_node.depends_on.nodes is defined %}
        {% for node_id in current_node.depends_on.nodes %}
            {{ log("Processing dependency: " ~ node_id, info=true) }}
            {% set node_parts = node_id.split('.') %}
            {% set node_type = node_parts[0] %}
            {{ log("Node type: " ~ node_type, info=true) }}
            
            {% if node_type == 'source' %}
                {#- This is a source() reference -#}
                {% set source_name = node_parts[1] ~ '.' ~ node_parts[2] %}
                {% set source_key = 'source_' ~ source_counter %}
                {% do sources_dict.update({source_key: {'type': 'source', 'name': source_name, 'node_id': node_id}}) %}
                {% set source_counter = source_counter + 1 %}
                
            {% elif node_type == 'model' %}
                {#- This is a ref() reference to a model -#}
                {% set ref_name = node_parts[2] %}
                {% set source_key = 'source_' ~ source_counter %}
                {% do sources_dict.update({source_key: {'type': 'ref', 'name': ref_name, 'node_id': node_id}}) %}
                {% set source_counter = source_counter + 1 %}
                
            {% elif node_type == 'seed' %}
                {#- This is a ref() reference to a seed -#}
                {% set seed_name = node_parts[2] %}
                {% set source_key = 'source_' ~ source_counter %}
                {% do sources_dict.update({source_key: {'type': 'seed', 'name': seed_name, 'node_id': node_id}}) %}
                {% set source_counter = source_counter + 1 %}
                
            {% elif node_type == 'snapshot' %}
                {#- This is a ref() reference to a snapshot -#}
                {% set snapshot_name = node_parts[2] %}
                {% set source_key = 'source_' ~ source_counter %}
                {% do sources_dict.update({source_key: {'type': 'snapshot', 'name': snapshot_name, 'node_id': node_id}}) %}
                {% set source_counter = source_counter + 1 %}
                
            {% elif node_type == 'test' %}
                {#- This is a test dependency (rare but possible) -#}
                {% set test_name = node_parts[2] %}
                {% set source_key = 'source_' ~ source_counter %}
                {% do sources_dict.update({source_key: {'type': 'test', 'name': test_name, 'node_id': node_id}}) %}
                {% set source_counter = source_counter + 1 %}
                
            {% elif node_type == 'analysis' %}
                {#- This is an analysis dependency (rare but possible) -#}
                {% set analysis_name = node_parts[2] %}
                {% set source_key = 'source_' ~ source_counter %}
                {% do sources_dict.update({source_key: {'type': 'analysis', 'name': analysis_name, 'node_id': node_id}}) %}
                {% set source_counter = source_counter + 1 %}
                
            {% elif node_type == 'exposure' %}
                {#- This is an exposure dependency (rare but possible) -#}
                {% set exposure_name = node_parts[2] %}
                {% set source_key = 'source_' ~ source_counter %}
                {% do sources_dict.update({source_key: {'type': 'exposure', 'name': exposure_name, 'node_id': node_id}}) %}
                {% set source_counter = source_counter + 1 %}
                
            {% else %}
                {#- Unknown node type - capture it anyway -#}
                {% set unknown_name = node_parts[2] if node_parts | length > 2 else node_id %}
                {% set source_key = 'source_' ~ source_counter %}
                {% do sources_dict.update({source_key: {'type': 'unknown_' ~ node_type, 'name': unknown_name, 'node_id': node_id}}) %}
                {% set source_counter = source_counter + 1 %}
            {% endif %}
        {% endfor %}
    {% endif %}
    
    {{ log("Total sources captured: " ~ (sources_dict | length), info=true) }}
    
{% elif model.depends_on is defined and model.depends_on.nodes is defined %}
    {{ log("Using fallback: model.depends_on", info=true) }}
    {{ log("Dependencies count: " ~ (model.depends_on.nodes | length), info=true) }}
    {#- Fallback: Try model.depends_on if graph is not available -#}
    {% for node_id in model.depends_on.nodes %}
        {% set node_parts = node_id.split('.') %}
        {% set node_type = node_parts[0] %}
        
        {% if node_type == 'source' %}
            {% set source_name = node_parts[1] ~ '.' ~ node_parts[2] %}
            {% set source_key = 'source_' ~ source_counter %}
            {% do sources_dict.update({source_key: {'type': 'source', 'name': source_name, 'node_id': node_id}}) %}
            {% set source_counter = source_counter + 1 %}
            
        {% elif node_type == 'model' %}
            {% set ref_name = node_parts[2] %}
            {% set source_key = 'source_' ~ source_counter %}
            {% do sources_dict.update({source_key: {'type': 'ref', 'name': ref_name, 'node_id': node_id}}) %}
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
        {% endif %}
    {% endfor %}
    {{ log("Total sources captured (fallback): " ~ (sources_dict | length), info=true) }}
{% else %}
    {{ log("WARNING: No dependency info available!", info=true) }}
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

insert into {{ log_table }} (
    PROCESS_CONFIG_SK,
    PROCESS_STEP_ID,
    RECORD_TYPE,
    MODEL_NAME,
    EXECUTION_STATUS_NAME,
    EXECUTION_COMPLETED_IND,
    EXECUTION_START_TMSTP,
    EXECUTION_END_TMSTP,
    SOURCE_OBJ,
    DESTINATION_OBJ,
    PROCESS_CONFIG_OBJ,
    SOURCE_DATA_CNT,
    DESTINATION_DATA_CNT_OBJ,
    EXECUTION_TYPE_NAME,
    EXTRACT_START_TMSTP,
    EXTRACT_END_TMSTP,
    ERROR_MESSAGE_OBJ,
    STEP_EXECUTION_OBJ,
    INSERT_TMSTP,
    UPDATE_TMSTP,
    DELETED_IND
)
select
    null as PROCESS_CONFIG_SK,
    '{{ process_step_id }}' as PROCESS_STEP_ID,
    'MODEL' as RECORD_TYPE,
    '{{ model_name }}' as MODEL_NAME,
    'RUNNING' as EXECUTION_STATUS_NAME,
    'N' as EXECUTION_COMPLETED_IND,
    CURRENT_TIMESTAMP() as EXECUTION_START_TMSTP,
    null::TIMESTAMP_NTZ as EXECUTION_END_TMSTP,
    parse_json('{{ sources_json }}') as SOURCE_OBJ,
    object_construct(
        'database', '{{ this.database }}',
        'schema', '{{ this.schema }}',
        'table', '{{ this.identifier }}',
        'materialization', '{{ materialization }}',
        'execution_type', '{{ execution_type }}',
        'full_name', '{{ this.database }}.{{ this.schema }}.{{ this.identifier }}'
    ) as DESTINATION_OBJ,
    object_construct(
        'model_name', '{{ model_name }}',
        'materialization', '{{ materialization }}',
        'execution_type', '{{ execution_type }}'
    ) as PROCESS_CONFIG_OBJ,
    0 as SOURCE_DATA_CNT,
    parse_json('null') as DESTINATION_DATA_CNT_OBJ,
    '{{ execution_type }}' as EXECUTION_TYPE_NAME,
    CURRENT_TIMESTAMP() as EXTRACT_START_TMSTP,
    null::TIMESTAMP_NTZ as EXTRACT_END_TMSTP,
    parse_json('null') as ERROR_MESSAGE_OBJ,
    object_construct(
        'model_name', '{{ model_name }}',
        'current_step', 'MODEL_STARTED',
        'query_id_start', LAST_QUERY_ID(),
        'query_id_end', null,
        'execution_timeline', parse_json('[' ||
            '{"step_number":1,' ||
            '"timestamp":"' || to_varchar(current_timestamp(), 'YYYY-MM-DD HH24:MI:SS.FF3') || '",' ||
            '"level":"Info",' ||
            '"step_type":"MODEL_START",' ||
            '"title":"Model Started: {{ model_name }}",' ||
            '"query_id":"' || LAST_QUERY_ID() || '",' ||
            '"content":{"model":"{{ model_name }}","materialization":"{{ materialization }}","execution_type":"{{ execution_type }}","database":"{{ this.database }}","schema":"{{ this.schema }}","table":"{{ this.identifier }}"}}' ||
            ']')
    ) as STEP_EXECUTION_OBJ,
    CURRENT_TIMESTAMP() as INSERT_TMSTP,
    CURRENT_TIMESTAMP() as UPDATE_TMSTP,
    'N' as DELETED_IND

{%- endmacro -%}
