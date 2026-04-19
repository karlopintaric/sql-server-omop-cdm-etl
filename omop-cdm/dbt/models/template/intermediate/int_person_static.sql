{{
  config(
    materialized='table',
    tags=['static'],
    pre_hook="{{ drop_all_indexes_on_table(this.identifier) }}",
    post_hook=[
      "{{ add_identity_column('person_id') }}",
      "{{ create_primary_key(this.identifier, ['person_id']) }}",
      "{{ create_index(this.identifier, ['person_source_value']) }}"
    ]
  )
}}

with source_persons as (
    select distinct
        cast(patient_source_id as varchar(50)) as person_source_value
    from {{ source('stage', 'source_events') }}
)

select {{ limit_rows() }}
    cast(0 as int) as gender_concept_id,
    cast(1900 as int) as year_of_birth,
    cast(null as int) as month_of_birth,
    cast(null as int) as day_of_birth,
    cast(null as datetime2) as birth_datetime,
    cast(0 as int) as race_concept_id,
    cast(0 as int) as ethnicity_concept_id,
    cast(null as bigint) as location_id,
    cast(null as bigint) as provider_id,
    cast(null as bigint) as care_site_id,
    sp.person_source_value,
    cast(null as varchar(50)) as gender_source_value,
    cast(null as int) as gender_source_concept_id,
    cast(null as varchar(50)) as race_source_value,
    cast(null as int) as race_source_concept_id,
    cast(null as varchar(50)) as ethnicity_source_value,
    cast(null as int) as ethnicity_source_concept_id
from source_persons as sp