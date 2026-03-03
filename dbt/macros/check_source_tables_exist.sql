{% macro check_source_tables_exist() %}
    {#
        C1: Source Arrival Detection
        Queries INFORMATION_SCHEMA to verify every table declared in the
        'raw' source actually exists.  Called from on-run-start so that a
        missing source file blocks the entire dbt run before any model is built.
    #}

    {% set expected_tables = [
        'patients',
        'encounters',
        'encounters_schema_change_batch',
        'conditions',
        'observations',
        'medications',
        'clinical_notes'
    ] %}

    {% set schema = 'raw' %}
    {% set project = var('project_id') %}

    {% set query %}
        select table_name
        from `{{ project }}`.`{{ schema }}`.INFORMATION_SCHEMA.TABLES
    {% endset %}

    {% set results = run_query(query) %}

    {% if execute %}
        {% set existing_tables = results.columns[0].values() | map('lower') | list %}
        {% set missing = [] %}

        {% for tbl in expected_tables %}
            {% if tbl | lower not in existing_tables %}
                {% do missing.append(tbl) %}
            {% endif %}
        {% endfor %}

        {% if missing | length > 0 %}
            {{ exceptions.raise_compiler_error(
                "SOURCE ARRIVAL FAILURE – the following expected source tables are missing from `"
                ~ project ~ "." ~ schema ~ "`: " ~ missing | join(', ')
                ~ ".  The dbt run has been aborted to prevent downstream models from building on stale or absent data."
            ) }}
        {% else %}
            {{ log("All " ~ expected_tables | length ~ " expected source tables present in `" ~ project ~ "." ~ schema ~ "`.", info=True) }}
        {% endif %}
    {% endif %}

{% endmacro %}
