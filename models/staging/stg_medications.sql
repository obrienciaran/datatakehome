with source as (
    select * from {{ source('raw', 'medications') }}
),

renamed as (
    select
        cast(START as TIMESTAMP)            as medication_start,
        cast(STOP as TIMESTAMP)             as medication_stop,
        PATIENT                             as patient_id,
        PAYER                               as payer_id,
        ENCOUNTER                           as encounter_id,
        cast(CODE as STRING)                as medication_code,
        DESCRIPTION                         as medication_description,
        cast(BASE_COST as FLOAT64)          as base_cost,
        cast(PAYER_COVERAGE as FLOAT64)     as payer_coverage,
        cast(DISPENSES as INT64)            as dispenses,
        cast(TOTALCOST as FLOAT64)          as total_cost,
        cast(REASONCODE as STRING)          as reason_code,
        REASONDESCRIPTION                   as reason_description
    from source
)

select * from renamed
