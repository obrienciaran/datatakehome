-- This model transforms the schema-change batch encounters to match the main encounters schema.
-- It is unioned into stg_encounters and not exposed independently.

with source as (
    select * from {{ source('raw', 'encounters_schema_change_batch') }}
),

renamed as (
    select
        Id                                                      as encounter_id,
        PATIENT                                                 as patient_id,
        lower(ENCOUNTER_TYPE)                                   as encounter_class,
        cast(CODE as STRING)                                    as encounter_code,
        DESCRIPTION                                             as encounter_description,
        TIMESTAMP_MILLIS(cast(START as INT64))                  as encounter_start,
        TIMESTAMP_MILLIS(cast(STOP as INT64))                   as encounter_stop,
        ORGANIZATION                                            as organization_id,
        PROVIDER                                                as provider_id,
        PAYER                                                   as payer_id,
        cast(BASE_ENCOUNTER_COST as FLOAT64)                    as base_encounter_cost,
        cast(TOTAL_CLAIM_COST as FLOAT64)                       as total_claim_cost,
        cast(PAYER_COVERAGE as FLOAT64)                         as payer_coverage,
        cast(REASONCODE as STRING)                              as reason_code,
        REASONDESCRIPTION                                       as reason_description,
        SOURCE_SYSTEM                                           as source_system
    from source
)

select * from renamed
