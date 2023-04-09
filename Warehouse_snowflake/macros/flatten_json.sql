{% macro snowflake_flatten_json(stg_table_name, json_column) -%}

    {%- set columns = [] -%}
    {%- set json_column_quoted = json_column|string|safe -%}
    {%- for column in execute('select parse_json(' ~ json_column_quoted ~ ') as json_data from ' ~ stg_table_name ~ ' limit 1', adapter='snowflake').fetchall()[0]['JSON_DATA']: -%}
        {%- if column.type == 'object' -%}
            {%- for nested_col in column.value -%}
                {%- set col_name = nested_col.key | replace(" ", "_") | replace(".", "_") | lower() -%}
                {{ col_name }} {{ nested_col.type }},
                {%- do columns.append(col_name) -%}
            {%- endfor -%}
        {%- else -%}
            {%- set col_name = column.key | replace(" ", "_") | replace(".", "_") | lower() -%}
            {{ col_name }} {{ column.type }},
            {%- do columns.append(col_name) -%}
        {%- endif -%}
    {%- endfor -%}

    create or replace table {{ stg_table_name }}_flat as
    select
        {%- for column in columns[:-1] -%}
            {{ column }},
        {%- endfor -%}
        {{ columns[-1] }}
    from (
        select
            {%- for column_name in execute('select distinct column_name from ' ~ stg_table_name ~ '_metadata', adapter='snowflake') -%}
                {%- set keys = row.json_data.get(column_name.key).object_keys() -%}
                {%- for key in keys -%}
                    json_data:{{ column_name.key }}:{{ key }} as {{ column_name.key }}_{{ key }},
                {%- endfor -%}
            {%- endfor -%}
            json_data:{{ columns[-1] }}::{{ columns[-1].type }} as {{ columns[-1] }}
        from {{ stg_table_name }}
    );

{%- endmacro %}
