{% macro default__snapshot_get_time() %}
    {% set session_date = var('session_date', none) %}
    {% if session_date is not none %}
        {{ log("Using session_date: " ~ session_date, info=True) }}
        TIMESTAMP_SUB(TIMESTAMP('{{ session_date }}'), INTERVAL 1 DAY)
    {% else %}
        {{ current_timestamp() }}
    {% endif %}
{% endmacro %}