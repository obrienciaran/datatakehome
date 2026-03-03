{% macro validate_row_counts() %}
    {#
        C2: Silent Failure Detection (on-run-end)
        Checks that mart tables have non-zero row counts and are within a
        plausible ratio of the staging encounter count.  Catches silent
        failures such as broken joins that drop all rows.
    #}

    {% set project = var('project_id') %}
    {% set mart_schema = target.schema ~ '_mart' %}

    {% set mart_tables = [
        'frequent_attenders',
        'length_of_stay'
    ] %}

    {% set stg_query %}
        select count(*) as cnt
        from `{{ project }}`.`{{ target.schema }}`.stg_encounters
    {% endset %}

    {% set stg_result = run_query(stg_query) %}

    {% if execute %}
        {% set stg_count = stg_result.columns[0].values()[0] %}

        {% for tbl in mart_tables %}
            {% set mart_query %}
                select count(*) as cnt
                from `{{ project }}`.`{{ mart_schema }}`.{{ tbl }}
            {% endset %}

            {% set mart_result = run_query(mart_query) %}
            {% set mart_count = mart_result.columns[0].values()[0] %}

            {% if mart_count == 0 %}
                {{ log("WARNING: mart table `" ~ tbl ~ "` has 0 rows — possible silent failure.", info=True) }}
            {% elif stg_count > 0 and mart_count > stg_count * 10 %}
                {{ log("WARNING: mart table `" ~ tbl ~ "` has " ~ mart_count ~ " rows vs " ~ stg_count ~ " staging encounters — possible fan-out.", info=True) }}
            {% else %}
                {{ log("`" ~ tbl ~ "` row count looks healthy (" ~ mart_count ~ " rows).", info=True) }}
            {% endif %}
        {% endfor %}
    {% endif %}

{% endmacro %}
