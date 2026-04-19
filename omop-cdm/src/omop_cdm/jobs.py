import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from omop_cdm.assets.dbt import dbt_models_entities, dbt_models_sample
from omop_cdm.assets.data_quality import (
    run_achilles_container,
    run_cdm_onboarding_container,
    run_data_quality_dashboard_container,
)
from omop_cdm.assets.export import export_condition_occurrence_to_s3
from omop_cdm.assets.sample_seed import load_sample_source_events
from omop_cdm.assets.vocabulary_seed import load_omop_vocabularies_from_csv


load_csvs_job = dg.define_asset_job(
    "load_csvs",
    selection=dg.AssetSelection.assets(
        load_omop_vocabularies_from_csv,
        load_sample_source_events,
    ),
    description="Load all CSV files: OMOP vocabulary and sample source events",
)


build_static_entities_job = dg.define_asset_job(
    "build_static_entities",
    selection=build_dbt_asset_selection([dbt_models_entities]),
    description="Run dbt static models (e.g. person) — non-partitioned",
)


transform_event_data_job = dg.define_asset_job(
    "transform_event_data",
    selection=build_dbt_asset_selection([dbt_models_sample]),
    description="Run dbt transformations for event data (partitioned)",
)


run_all_data_quality_job = dg.define_asset_job(
    "run_all_data_quality",
    selection=dg.AssetSelection.assets(
        run_data_quality_dashboard_container,
        run_cdm_onboarding_container,
        run_achilles_container,
    ),
    description="Run all containerized data-quality checks against OMOP sample outputs",
)


export_to_s3_job = dg.define_asset_job(
    "export_to_s3",
    selection=dg.AssetSelection.assets(export_condition_occurrence_to_s3),
    description="Export transformed sample OMOP model to S3",
)
