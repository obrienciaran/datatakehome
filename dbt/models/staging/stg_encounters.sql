with main_encounters as (
    select
        Id                                          as encounter_id,
        cast(START as TIMESTAMP)                    as encounter_start,
        cast(STOP as TIMESTAMP)                     as encounter_stop,
        PATIENT                                     as patient_id,
        ORGANIZATION                                as organization_id,
        PROVIDER                                    as provider_id,
        PAYER                                       as payer_id,
        lower(ENCOUNTERCLASS)                       as encounter_class,
        cast(CODE as STRING)                        as encounter_code,
        DESCRIPTION                                 as encounter_description,
        cast(BASE_ENCOUNTER_COST as FLOAT64)        as base_encounter_cost,
        cast(TOTAL_CLAIM_COST as FLOAT64)           as total_claim_cost,
        cast(PAYER_COVERAGE as FLOAT64)             as payer_coverage,
        cast(REASONCODE as STRING)                  as reason_code,
        REASONDESCRIPTION                           as reason_description,
        'MAIN'                                      as source_system
    from {{ source('raw', 'encounters') }}
),

schema_change_batch as (
    select * from {{ ref('stg_encounters_schema_change_batch') }}
),

unioned as (
    select * from main_encounters
    union all
    select * from schema_change_batch
)

select * from unioned
