{% macro normalise_json(model_name, json_column) %}
{% set columns = [] %}
{% set comma = joiner() %}
{% for column_name, data_type in run_query("SELECT column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{}' AND column_name = '{}'".format(model_name, json_column)) %}
    {% for inner_column in run_query("SELECT json_object_keys({}::json) as column_name FROM {} LIMIT 1".format(json_column, model_name)) if inner_column is not none %}
        {% set columns = columns + [(inner_column.column, run_query("SELECT data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{}' AND column_name = '{}'".format(model_name, inner_column.column)).column[0])] %}
        {% do comma.append() %}
    {% endfor %}
{% endfor %}

{% set select_columns = [] %}
{% for column_name, data_type in columns %}
    {% set select_columns = select_columns + ["{} ->> '{}'::{} as {}".format(json_column, column_name, data_type, column_name)] %}
{% endfor %}

SELECT {{ model_name }}.raw_data,
{{ select_columns | join(', ') }}
FROM {{ model_name }}, LATERAL jsonb_to_record({{ json_column }}::jsonb) AS x({{ ", ".join(['"%s" %s' % (column_name, data_type) for column_name, data_type in columns]) }});
{% endmacro %}
