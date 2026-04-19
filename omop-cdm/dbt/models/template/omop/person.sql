{{
  config(
    materialized='table',
    tags=['static'],
    pre_hook="{{ drop_all_indexes_on_table(this.identifier) }}",
    post_hook=[
      "{{ create_primary_key(this.identifier, ['person_id']) }}",
      "{{ create_index(this.identifier, ['person_source_value']) }}",
      "{{ create_clustered_columnstore_index(this.identifier) }}"
    ]
  )
}}

select {{ limit_rows() }}
    person_id,
    gender_concept_id,
    year_of_birth,
    month_of_birth,
    day_of_birth,
    birth_datetime,
    race_concept_id,
    ethnicity_concept_id,
    location_id,
    provider_id,
    care_site_id,
    person_source_value,
    gender_source_value,
    gender_source_concept_id,
    race_source_value,
    race_source_concept_id,
    ethnicity_source_value,
    ethnicity_source_concept_id
from {{ ref('int_person_static') }}