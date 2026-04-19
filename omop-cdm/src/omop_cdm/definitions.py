import dagster as dg
from pathlib import Path

from omop_cdm.assets import dbt as dbt_assets
from omop_cdm.assets import data_quality
from omop_cdm.assets import export as export_assets
from omop_cdm.assets import sample_seed
from omop_cdm.assets import vocabulary_seed
from omop_cdm.resources import dbt_resource, dwh_resource, s3_resource
from omop_cdm.jobs import (
    export_to_s3_job,
    load_csvs_job,
    build_static_entities_job,
    transform_event_data_job,
    run_all_data_quality_job,
)
from omop_cdm.sensors import email_on_run_failure, email_on_run_success


module_assets = dg.load_assets_from_modules(
    [
        sample_seed,
        vocabulary_seed,
        dbt_assets,
        export_assets,
        data_quality,
    ]
)


pythonic_defs = dg.Definitions(
    assets=[
        *module_assets,
    ],
    resources={
        "dbt": dbt_resource,
        "dwh": dwh_resource,
        "s3": s3_resource,
    },
    jobs=[
        load_csvs_job,
        build_static_entities_job,
        transform_event_data_job,
        run_all_data_quality_job,
        export_to_s3_job,
    ],
    sensors=[
        email_on_run_success,
        email_on_run_failure,
    ],
)


@dg.definitions
def defs():
    component_defs = dg.load_from_defs_folder(path_within_project=Path(__file__).parent)
    return dg.Definitions.merge(component_defs, pythonic_defs)
