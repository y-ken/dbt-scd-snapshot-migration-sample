{% macro query_with_type_conversion(source_dataset,source_table,snapshot_dataset,snapshot_table) -%}
{% set table_id = source_dataset + '.' + source_table %}

{%- set get_schema_query -%}
SELECT 
  sf_current.column_name,
  sf_current.data_type as data_type_from,
  sf_snapshot.data_type as data_type_to,
  coalesce(sf_snapshot.data_type, sf_current.data_type) as compatible_data_type
FROM 
  `{{ target.project }}.{{ source_dataset }}.INFORMATION_SCHEMA.COLUMNS` sf_current
  LEFT JOIN `{{ target.project }}.{{ snapshot_dataset }}.INFORMATION_SCHEMA.COLUMNS` sf_snapshot
    ON sf_current.column_name = sf_snapshot.column_name AND sf_snapshot.table_name = '{{ snapshot_table }}'
WHERE
  sf_current.table_name = '{{ source_table }}' 
{%- endset -%}

{%- set results = run_query(get_schema_query) -%}
{%- if execute -%}
    {% set columns = results.rows %}
{%- else -%}
    {% set columns = [] %}
{%- endif -%}

select
  {% for column in columns -%}
  {% if column.data_type_from == 'STRING' and column.data_type_to == 'DATE' -%}
    CAST(LEFT(CAST({{ column.column_name }} AS STRING), 10) AS DATE) AS {{ column.column_name }}{% if not loop.last %},{% endif %}
  {% elif column.data_type_from == 'STRING' and column.data_type_to == 'INT64' -%}
    {# Bad int64 value: 30000.0 #}
    CAST(CAST({{ column.column_name }} AS FLOAT64) AS INT64) AS {{ column.column_name }}{% if not loop.last %},{% endif %}
  {%- else -%}
    CAST({{ column.column_name }} AS {{ column.compatible_data_type }}) AS {{ column.column_name }}{% if not loop.last %},{% endif %}
  {%- endif -%}
  {% endfor %}
from {{ table_id }}
{%- endmacro %}