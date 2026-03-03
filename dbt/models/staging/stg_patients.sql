with source as (
    select * from {{ source('raw', 'patients') }}
),

renamed as (
    select
        Id                                          as patient_id,
        date(cast(BIRTHDATE as TIMESTAMP))            as birth_date,
        date(cast(DEATHDATE as TIMESTAMP))            as death_date,
        SSN                                         as ssn,
        DRIVERS                                     as drivers_licence,
        PASSPORT                                    as passport_number,
        PREFIX                                      as name_prefix,
        FIRST                                       as first_name,
        LAST                                        as last_name,
        SUFFIX                                      as name_suffix,
        MAIDEN                                      as maiden_name,
        MARITAL                                     as marital_status,
        RACE                                        as race,
        ETHNICITY                                   as ethnicity,
        GENDER                                      as gender,
        BIRTHPLACE                                  as birth_place,
        ADDRESS                                     as address,
        CITY                                        as city,
        STATE                                       as state,
        COUNTY                                      as county,
        ZIP                                         as zip_code,
        cast(LAT as FLOAT64)                        as latitude,
        cast(LON as FLOAT64)                        as longitude,
        cast(HEALTHCARE_EXPENSES as FLOAT64)        as healthcare_expenses,
        cast(HEALTHCARE_COVERAGE as FLOAT64)        as healthcare_coverage
    from source
)

select * from renamed
