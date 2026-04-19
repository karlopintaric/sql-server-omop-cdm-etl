from datetime import datetime
import json
from typing import Any, Mapping, Optional

import dagster as dg
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets

from omop_cdm.resources import dbt_project


class SampleDagsterDbtTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> Optional[str]:
        return "dbt_sample"


half_year_partitions_def = dg.TimeWindowPartitionsDefinition(
    start=datetime(2014, 1, 1),
    cron_schedule="0 0 1 */6 *",
    fmt="%Y-%m",
)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=SampleDagsterDbtTranslator(),
    select="tag:static",
)
def dbt_models_entities(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["run"], context=context).stream()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=SampleDagsterDbtTranslator(),
    select="tag:partitioned",
    partitions_def=half_year_partitions_def,
    backfill_policy=dg.BackfillPolicy.multi_run(),
    pool="dbt",
)
def dbt_models_sample(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    start, end = context.partition_time_window
    dbt_vars = {
        "min_date": start.date().isoformat(),
        "max_date": end.date().isoformat(),
    }
    yield from dbt.cli(["run", "--vars", json.dumps(dbt_vars)], context=context).stream()
