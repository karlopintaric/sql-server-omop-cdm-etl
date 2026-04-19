{{
  config(
    materialized='insert_by_period',
    tags=['partitioned'],
    timestamp_field='condition_start_date',
    post_hook=[
      "{{ add_identity_column('condition_occurrence_id') }}",
      "{{ create_primary_key(this.identifier, ['condition_occurrence_id']) }}",
      "{{ create_index(this.identifier, ['person_id', 'condition_start_date']) }}"
    ]
  )
}}

  {% set default_condition_concept_id = var('default_condition_concept_id', 0) %}
  {% set default_condition_type_concept_id = var('default_condition_type_concept_id', 32817) %}
  {% set default_condition_status_concept_id = var('default_condition_status_concept_id', 0) %}

select
    p.person_id,
    cast({{ default_condition_concept_id }} as int) as condition_concept_id,
    e.condition_start_date,
    cast(e.condition_start_date as datetime2) as condition_start_datetime,
    e.condition_start_date as condition_end_date,
    cast(e.condition_start_date as datetime2) as condition_end_datetime,
    cast({{ default_condition_type_concept_id }} as int) as condition_type_concept_id,
    cast({{ default_condition_status_concept_id }} as int) as condition_status_concept_id,
    cast(null as varchar(20)) as stop_reason,
    cast(null as bigint) as provider_id,
    e.visit_occurrence_id,
    cast(null as bigint) as visit_detail_id,
    e.condition_source_value,
    cast(coalesce(try_cast(e.condition_source_code as int), 0) as int) as condition_source_concept_id,
    cast(null as varchar(50)) as condition_status_source_value
from {{ ref('int_condition_events_filtered') }} as e
inner join {{ ref('person') }} as p
    on p.person_source_value = cast(e.patient_source_id as varchar(50))
where __PERIOD_FILTER__