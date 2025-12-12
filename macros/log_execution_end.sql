{%- macro log_execution_end() -%}{%- macro log_execution_end() -%}{%- macro log_execution_e    {% if current_node.depends_on is defined and current_node.depends_on.nodes is defined %}



{#-         {% set dependency_nodes = current_node.depends_on.nodes %}

    This macro UPDATES the specific MODEL record when a model FINISHES executing.

    Updates model with SUCCESS status, end time, duration, query ID, and row counts.{#-         {{ log("Dependencies found: " ~ (dependency_nodes | length), info=true) }}

    Adds completion event to timeline.

    Now also captures SOURCE_OBJ dependencies using the graph object.    This macro UPDATES the specific MODEL record when a model FINISHES executing.        {{ log("Full dependency list: " ~ dependency_nodes, info=true) }}

-#}

    Updates model with SUCCESS status, end time, duration, query ID, and row counts.        

{% set log_table = 'DEV_PROVIDERPDM.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG' %}

{% set model_name = this.name %}    Adds completion event to timeline.        {% for i in range(dependency_nodes | length) %}

{% set run_id = invocation_id %}

{% set process_step_id = 'JOB_' ~ run_id %}    Now also captures SOURCE_OBJ dependencies using the graph object.            {% set node_id = dependency_nodes[i] %}



{#- Build source dependencies object (graph is fully available in post-hook) -#}-#}            {{ log("Processing [" ~ i ~ "]: " ~ node_id, info=true) }}

{% set sources_dict = {} %}

{% set current_model_id = model.unique_id if model.unique_id is defined else ('model.' ~ project_name ~ '.' ~ this.name) %}            {% set node_parts = node_id.split('.') %}



{{ log("=== POST-HOOK DEBUG for " ~ this.name ~ " ===", info=true) }}{% set log_table = 'DEV_PROVIDERPDM.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG' %}            {% set node_type = node_parts[0] %}



{% if graph is defined and graph.nodes is defined and current_model_id in graph.nodes %}{% set model_name = this.name %}            {{ log("  Type: " ~ node_type ~ ", Counter: " ~ source_counter, info=true) %}

    {% set current_node = graph.nodes[current_model_id] %}

    {% set run_id = invocation_id %}{#- 

    {% if current_node.depends_on is defined and current_node.depends_on.nodes is defined %}

        {% set dependency_nodes = current_node.depends_on.nodes %}{% set process_step_id = 'JOB_' ~ run_id %}    This macro UPDATES the specific MODEL record when a model FINISHES executing.

        {{ log("Dependencies found: " ~ (dependency_nodes | length), info=true) }}

            Updates model with SUCCESS status, end time, duration, query ID, and row counts.

        {#- Use index-based loop to avoid Jinja variable scoping issues -#}

        {% for i in range(dependency_nodes | length) %}{#- Build source dependencies object (graph is fully available in post-hook) -#}    Adds completion event to timeline.

            {% set node_id = dependency_nodes[i] %}

            {% set source_key = 'source_' ~ (i + 1) %}{% set sources_dict = {} %}-#}

            {% set node_parts = node_id.split('.') %}

            {% set node_type = node_parts[0] %}{% set current_model_id = model.unique_id if model.unique_id is defined else ('model.' ~ project_name ~ '.' ~ this.name) %}

            

            {% if node_type == 'source' %}{% set log_table = 'DEV_PROVIDERPDM.PROVIDERPDM_CORE_TARGET.PROCESS_EXECUTION_LOG' %}

                {% set source_name = node_parts[1] ~ '.' ~ node_parts[2] %}

                {% do sources_dict.update({source_key: {'type': 'source', 'name': source_name, 'node_id': node_id}}) %}{{ log("=== POST-HOOK DEBUG for " ~ this.name ~ " ===", info=true) }}{% set model_name = this.name %}

                

            {% elif node_type == 'model' %}{{ log("current_model_id: " ~ current_model_id, info=true) }}{% set run_id = invocation_id %}

                {% set ref_name = node_parts[2] %}

                {% do sources_dict.update({source_key: {'type': 'ref', 'name': ref_name, 'node_id': node_id}}) %}{% set process_step_id = 'JOB_' ~ run_id %}

                

            {% elif node_type == 'seed' %}{% if graph is defined and graph.nodes is defined and current_model_id in graph.nodes %}

                {% set seed_name = node_parts[2] %}

                {% do sources_dict.update({source_key: {'type': 'seed', 'name': seed_name, 'node_id': node_id}}) %}    {% set current_node = graph.nodes[current_model_id] %}{#- Build source dependencies object (graph is fully available in post-hook) -#}

                

            {% elif node_type == 'snapshot' %}    {{ log("Found in graph.nodes", info=true) }}{% set sources_dict = {} %}

                {% set snapshot_name = node_parts[2] %}

                {% do sources_dict.update({source_key: {'type': 'snapshot', 'name': snapshot_name, 'node_id': node_id}}) %}    {% set source_counter = 1 %}

                

            {% elif node_type == 'test' %}    {% if current_node.depends_on is defined and current_node.depends_on.nodes is defined %}{% set current_model_id = model.unique_id if model.unique_id is defined else ('model.' ~ project_name ~ '.' ~ this.name) %}

                {% set test_name = node_parts[2] %}

                {% do sources_dict.update({source_key: {'type': 'test', 'name': test_name, 'node_id': node_id}}) %}        {% set dependency_nodes = current_node.depends_on.nodes %}

                

            {% elif node_type == 'analysis' %}        {{ log("Dependencies found: " ~ (dependency_nodes | length), info=true) }}{{ log("=== POST-HOOK DEBUG for " ~ this.name ~ " ===", info=true) }}

                {% set analysis_name = node_parts[2] %}

                {% do sources_dict.update({source_key: {'type': 'analysis', 'name': analysis_name, 'node_id': node_id}}) %}        {{ log("Full dependency list: " ~ dependency_nodes, info=true) }}{{ log("current_model_id: " ~ current_model_id, info=true) }}

                

            {% elif node_type == 'exposure' %}        

                {% set exposure_name = node_parts[2] %}

                {% do sources_dict.update({source_key: {'type': 'exposure', 'name': exposure_name, 'node_id': node_id}}) %}        {#- Use index-based loop to avoid Jinja variable scoping issues -#}{% if graph is defined and graph.nodes is defined and current_model_id in graph.nodes %}

                

            {% else %}        {% for i in range(dependency_nodes | length) %}    {% set current_node = graph.nodes[current_model_id] %}

                {% set unknown_name = node_parts[2] if node_parts | length > 2 else node_id %}

                {% do sources_dict.update({source_key: {'type': 'unknown_' ~ node_type, 'name': unknown_name, 'node_id': node_id}}) %}            {% set node_id = dependency_nodes[i] %}    {{ log("Found in graph.nodes", info=true) }}

            {% endif %}

        {% endfor %}            {% set source_key = 'source_' ~ (i + 1) %}    

        {{ log("Total captured: " ~ (sources_dict | length), info=true) }}

    {% endif %}            {{ log("Processing [" ~ i ~ "]: " ~ node_id ~ " -> key: " ~ source_key, info=true) }}    {% if current_node.depends_on is defined and current_node.depends_on.nodes is defined %}

{% endif %}

            {% set node_parts = node_id.split('.') %}        {% set dependency_nodes = current_node.depends_on.nodes %}

{#- Build JSON string for sources -#}

{% set sources_json_parts = [] %}            {% set node_type = node_parts[0] %}        {{ log("Dependencies found: " ~ (dependency_nodes | length), info=true) }}

{% for key, val in sources_dict.items() %}

    {% do sources_json_parts.append('"' ~ key ~ '":{"type":"' ~ val.type ~ '","name":"' ~ val.name ~ '","node_id":"' ~ val.node_id ~ '"}') %}            {{ log("  Type: " ~ node_type, info=true) }}        {{ log("Full dependency list: " ~ dependency_nodes, info=true) }}

{% endfor %}

{% set sources_json = '{' ~ sources_json_parts | join(',') ~ '}' %}                    {{ log("Dependency list type: " ~ (dependency_nodes | string), info=true) }}



{{ log("Total in JSON: " ~ (sources_json_parts | length), info=true) }}            {% if node_type == 'source' %}        



update {{ log_table }}                {% set source_name = node_parts[1] ~ '.' ~ node_parts[2] %}        {% for node_id in dependency_nodes %}

set

    EXECUTION_STATUS_NAME = 'SUCCESS',                {% do sources_dict.update({source_key: {'type': 'source', 'name': source_name, 'node_id': node_id}}) %}            {{ log("Processing: " ~ node_id, info=true) }}

    EXECUTION_COMPLETED_IND = 'Y',

    EXECUTION_END_TMSTP = CURRENT_TIMESTAMP(),                {{ log("  Added SOURCE: " ~ source_name ~ ", Dict size: " ~ (sources_dict | length), info=true) }}            {% set node_parts = node_id.split('.') %}

    EXTRACT_END_TMSTP = CURRENT_TIMESTAMP(),

    UPDATE_TMSTP = CURRENT_TIMESTAMP(),                            {% set node_type = node_parts[0] %}

    SOURCE_OBJ = parse_json('{{ sources_json }}'),

    SOURCE_DATA_CNT = (select count(*) from {{ this }}),            {% elif node_type == 'model' %}            {{ log("  Type: " ~ node_type ~ ", Counter: " ~ source_counter, info=true) }}

    DESTINATION_DATA_CNT_OBJ = (select count(*) from {{ this }}),

    STEP_EXECUTION_OBJ = object_insert(                {% set ref_name = node_parts[2] %}            {{ log("  Parts: " ~ node_parts, info=true) }}

        object_insert(

            object_insert(                {% do sources_dict.update({source_key: {'type': 'ref', 'name': ref_name, 'node_id': node_id}}) %}            

                STEP_EXECUTION_OBJ,

                'current_step',                {{ log("  Added REF: " ~ ref_name ~ ", Dict size: " ~ (sources_dict | length), info=true) }}            {% if node_type == 'source' %}

                'MODEL_COMPLETED',

                true                                {% set source_name = node_parts[1] ~ '.' ~ node_parts[2] %}

            ),

            'query_id_end',            {% elif node_type == 'seed' %}                {% set source_key = 'source_' ~ source_counter %}

            LAST_QUERY_ID(),

            true                {% set seed_name = node_parts[2] %}                {{ log("  Creating source_key: " ~ source_key ~ " for " ~ source_name, info=true) }}

        ),

        'execution_timeline',                {% do sources_dict.update({source_key: {'type': 'seed', 'name': seed_name, 'node_id': node_id}}) %}                {% do sources_dict.update({source_key: {'type': 'source', 'name': source_name, 'node_id': node_id}}) %}

        array_append(

            coalesce(STEP_EXECUTION_OBJ:execution_timeline, parse_json('[]')),                {{ log("  Added SEED: " ~ seed_name ~ ", Dict size: " ~ (sources_dict | length), info=true) }}                {{ log("  Added SOURCE: " ~ source_name ~ ", Dict size now: " ~ (sources_dict | length), info=true) }}

            object_construct(

                'step_number', array_size(coalesce(STEP_EXECUTION_OBJ:execution_timeline, parse_json('[]'))) + 1,                                {% set source_counter = source_counter + 1 %}

                'timestamp', to_varchar(current_timestamp(), 'YYYY-MM-DD HH24:MI:SS.FF3'),

                'level', 'Info',            {% elif node_type == 'snapshot' %}                

                'step_type', 'MODEL_COMPLETE',

                'title', 'Model Completed: {{ model_name }}',                {% set snapshot_name = node_parts[2] %}            {% elif node_type == 'model' %}

                'query_id', LAST_QUERY_ID(),

                'query_result', object_construct(                {% do sources_dict.update({source_key: {'type': 'snapshot', 'name': snapshot_name, 'node_id': node_id}}) %}                {% set ref_name = node_parts[2] %}

                    'rows_in_destination', (select count(*) from {{ this }}),

                    'execution_status', 'SUCCESS'                {{ log("  Added SNAPSHOT: " ~ snapshot_name ~ ", Dict size: " ~ (sources_dict | length), info=true) }}                {% set source_key = 'source_' ~ source_counter %}

                ),

                'content', object_construct(                                {{ log("  Creating source_key: " ~ source_key ~ " for " ~ ref_name, info=true) }}

                    'model', '{{ model_name }}',

                    'status', 'SUCCESS',            {% elif node_type == 'test' %}                {% do sources_dict.update({source_key: {'type': 'ref', 'name': ref_name, 'node_id': node_id}}) %}

                    'rows_processed', (select count(*) from {{ this }}),

                    'destination_table', '{{ this.database }}.{{ this.schema }}.{{ this.identifier }}'                {% set test_name = node_parts[2] %}                {{ log("  Added REF: " ~ ref_name ~ ", Dict size now: " ~ (sources_dict | length), info=true) }}

                )

            )                {% do sources_dict.update({source_key: {'type': 'test', 'name': test_name, 'node_id': node_id}}) %}                {% set source_counter = source_counter + 1 %}

        ),

        true                {{ log("  Added TEST: " ~ test_name ~ ", Dict size: " ~ (sources_dict | length), info=true) }}                

    )

where PROCESS_STEP_ID = '{{ process_step_id }}'                            {% elif node_type == 'seed' %}

  and RECORD_TYPE = 'MODEL'

  and MODEL_NAME = '{{ model_name }}'            {% elif node_type == 'analysis' %}                {% set seed_name = node_parts[2] %}



{%- endmacro -%}                {% set analysis_name = node_parts[2] %}                {% set source_key = 'source_' ~ source_counter %}


                {% do sources_dict.update({source_key: {'type': 'analysis', 'name': analysis_name, 'node_id': node_id}}) %}                {% do sources_dict.update({source_key: {'type': 'seed', 'name': seed_name, 'node_id': node_id}}) %}

                {{ log("  Added ANALYSIS: " ~ analysis_name ~ ", Dict size: " ~ (sources_dict | length), info=true) }}                {% set source_counter = source_counter + 1 %}

                                

            {% elif node_type == 'exposure' %}            {% elif node_type == 'snapshot' %}

                {% set exposure_name = node_parts[2] %}                {% set snapshot_name = node_parts[2] %}

                {% do sources_dict.update({source_key: {'type': 'exposure', 'name': exposure_name, 'node_id': node_id}}) %}                {% set source_key = 'source_' ~ source_counter %}

                {{ log("  Added EXPOSURE: " ~ exposure_name ~ ", Dict size: " ~ (sources_dict | length), info=true) }}                {% do sources_dict.update({source_key: {'type': 'snapshot', 'name': snapshot_name, 'node_id': node_id}}) %}

                                {% set source_counter = source_counter + 1 %}

            {% else %}                

                {% set unknown_name = node_parts[2] if node_parts | length > 2 else node_id %}            {% elif node_type == 'test' %}

                {% do sources_dict.update({source_key: {'type': 'unknown_' ~ node_type, 'name': unknown_name, 'node_id': node_id}}) %}                {% set test_name = node_parts[2] %}

                {{ log("  Added UNKNOWN: " ~ unknown_name ~ ", Dict size: " ~ (sources_dict | length), info=true) }}                {% set source_key = 'source_' ~ source_counter %}

            {% endif %}                {% do sources_dict.update({source_key: {'type': 'test', 'name': test_name, 'node_id': node_id}}) %}

        {% endfor %}                {% set source_counter = source_counter + 1 %}

        {{ log("Total captured: " ~ (sources_dict | length), info=true) }}                

        {{ log("Sources dict: " ~ sources_dict, info=true) }}            {% elif node_type == 'analysis' %}

    {% else %}                {% set analysis_name = node_parts[2] %}

        {{ log("depends_on.nodes NOT available", info=true) }}                {% set source_key = 'source_' ~ source_counter %}

    {% endif %}                {% do sources_dict.update({source_key: {'type': 'analysis', 'name': analysis_name, 'node_id': node_id}}) %}

{% else %}                {% set source_counter = source_counter + 1 %}

    {{ log("Model NOT in graph or graph unavailable", info=true) }}                

{% endif %}            {% elif node_type == 'exposure' %}

                {% set exposure_name = node_parts[2] %}

{#- Build JSON string for sources -#}                {% set source_key = 'source_' ~ source_counter %}

{% set sources_json_parts = [] %}                {% do sources_dict.update({source_key: {'type': 'exposure', 'name': exposure_name, 'node_id': node_id}}) %}

{% for key, val in sources_dict.items() %}                {% set source_counter = source_counter + 1 %}

    {% do sources_json_parts.append('"' ~ key ~ '":{"type":"' ~ val.type ~ '","name":"' ~ val.name ~ '","node_id":"' ~ val.node_id ~ '"}') %}                

{% endfor %}            {% else %}

{% set sources_json = '{' ~ sources_json_parts | join(',') ~ '}' %}                {% set unknown_name = node_parts[2] if node_parts | length > 2 else node_id %}

                {% set source_key = 'source_' ~ source_counter %}

{{ log("sources_json_parts count: " ~ (sources_json_parts | length), info=true) }}                {% do sources_dict.update({source_key: {'type': 'unknown_' ~ node_type, 'name': unknown_name, 'node_id': node_id}}) %}

{{ log("sources_json (first 500 chars): " ~ sources_json[:500], info=true) }}                {% set source_counter = source_counter + 1 %}

            {% endif %}

update {{ log_table }}        {% endfor %}

set        {{ log("Total captured: " ~ (sources_dict | length), info=true) }}

    EXECUTION_STATUS_NAME = 'SUCCESS',        {{ log("Sources dict: " ~ sources_dict, info=true) }}

    EXECUTION_COMPLETED_IND = 'Y',    {% else %}

    EXECUTION_END_TMSTP = CURRENT_TIMESTAMP(),        {{ log("depends_on.nodes NOT available", info=true) }}

    EXTRACT_END_TMSTP = CURRENT_TIMESTAMP(),    {% endif %}

    UPDATE_TMSTP = CURRENT_TIMESTAMP(),{% else %}

    SOURCE_OBJ = parse_json('{{ sources_json }}'),    {{ log("Model NOT in graph or graph unavailable", info=true) }}

    SOURCE_DATA_CNT = (select count(*) from {{ this }}),{% endif %}

    DESTINATION_DATA_CNT_OBJ = (select count(*) from {{ this }}),

    STEP_EXECUTION_OBJ = object_insert({#- Build JSON string for sources -#}

        object_insert({% set sources_json_parts = [] %}

            object_insert({% for key, val in sources_dict.items() %}

                STEP_EXECUTION_OBJ,    {% do sources_json_parts.append('"' ~ key ~ '":{"type":"' ~ val.type ~ '","name":"' ~ val.name ~ '","node_id":"' ~ val.node_id ~ '"}') %}

                'current_step',{% endfor %}

                'MODEL_COMPLETED',{% set sources_json = '{' ~ sources_json_parts | join(',') ~ '}' %}

                true

            ),{{ log("sources_json_parts count: " ~ (sources_json_parts | length), info=true) }}

            'query_id_end',{{ log("sources_json (first 500 chars): " ~ sources_json[:500], info=true) }}

            LAST_QUERY_ID(),

            trueupdate {{ log_table }}

        ),set

        'execution_timeline',    EXECUTION_STATUS_NAME = 'SUCCESS',

        array_append(    EXECUTION_COMPLETED_IND = 'Y',

            coalesce(STEP_EXECUTION_OBJ:execution_timeline, parse_json('[]')),    EXECUTION_END_TMSTP = CURRENT_TIMESTAMP(),

            object_construct(    EXTRACT_END_TMSTP = CURRENT_TIMESTAMP(),

                'step_number', array_size(coalesce(STEP_EXECUTION_OBJ:execution_timeline, parse_json('[]'))) + 1,    UPDATE_TMSTP = CURRENT_TIMESTAMP(),

                'timestamp', to_varchar(current_timestamp(), 'YYYY-MM-DD HH24:MI:SS.FF3'),    SOURCE_OBJ = parse_json('{{ sources_json }}'),

                'level', 'Info',    SOURCE_DATA_CNT = (select count(*) from {{ this }}),

                'step_type', 'MODEL_COMPLETE',    DESTINATION_DATA_CNT_OBJ = (select count(*) from {{ this }}),

                'title', 'Model Completed: {{ model_name }}',    STEP_EXECUTION_OBJ = object_insert(

                'query_id', LAST_QUERY_ID(),        object_insert(

                'query_result', object_construct(            object_insert(

                    'rows_in_destination', (select count(*) from {{ this }}),                STEP_EXECUTION_OBJ,

                    'execution_status', 'SUCCESS'                'current_step',

                ),                'MODEL_COMPLETED',

                'content', object_construct(                true

                    'model', '{{ model_name }}',            ),

                    'status', 'SUCCESS',            'query_id_end',

                    'rows_processed', (select count(*) from {{ this }}),            LAST_QUERY_ID(),

                    'destination_table', '{{ this.database }}.{{ this.schema }}.{{ this.identifier }}'            true

                )        ),

            )        'execution_timeline',

        ),        array_append(

        true            coalesce(STEP_EXECUTION_OBJ:execution_timeline, parse_json('[]')),

    )            object_construct(

where PROCESS_STEP_ID = '{{ process_step_id }}'                'step_number', array_size(coalesce(STEP_EXECUTION_OBJ:execution_timeline, parse_json('[]'))) + 1,

  and RECORD_TYPE = 'MODEL'                'timestamp', to_varchar(current_timestamp(), 'YYYY-MM-DD HH24:MI:SS.FF3'),

  and MODEL_NAME = '{{ model_name }}'                'level', 'Info',

                'step_type', 'MODEL_COMPLETE',

{%- endmacro -%}                'title', 'Model Completed: {{ model_name }}',

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
