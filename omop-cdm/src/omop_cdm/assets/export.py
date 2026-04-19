import os

import dagster as dg
from shared.resources.dwh import SqlServerConnectionResource
from shared.resources.s3 import S3Resource


CONDITION_OCCURRENCE_ASSET_KEY = dg.AssetKey(["condition_occurrence"])
EXPORT_TABLE_NAME = "condition_occurrence"


@dg.asset(
    group_name="sample_export",
    kinds=["python", "sqlserver", "s3", "csv"],
    deps=[CONDITION_OCCURRENCE_ASSET_KEY],
    name="export_condition_occurrence_to_s3",
)
def export_condition_occurrence_to_s3(
    context: dg.AssetExecutionContext,
    dwh: SqlServerConnectionResource,
    s3: S3Resource,
):
    database = os.getenv("DB_DATABASE") or os.getenv("DB_DATABASE_DEV")
    schema = os.getenv("DB_SCHEMA", "cdm_omop_dbt")
    bucket = os.getenv("EXPORT_S3_BUCKET", "05-curated")
    prefix = os.getenv("EXPORT_S3_PREFIX", "OMOP/sample")
    filename = f"{EXPORT_TABLE_NAME}.csv"

    if not database:
        raise RuntimeError("Set DB_DATABASE or DB_DATABASE_DEV before running export_condition_occurrence_to_s3")

    s3_client = s3.get_client()
    dwh.export_csv_in_batches(
        s3_client=s3_client,
        table_name=EXPORT_TABLE_NAME,
        s3_bucket=bucket,
        s3_key=prefix,
        filename=filename,
        db_name=database,
        schema=schema,
        batch_size=10000,
    )

    target_uri = f"s3://{bucket}/{prefix.strip('/')}/{filename}"
    context.log.info(f"Exported {schema}.{EXPORT_TABLE_NAME} to {target_uri}")

    return dg.MaterializeResult(
        metadata={
            "database": database,
            "schema": schema,
            "table": EXPORT_TABLE_NAME,
            "target_uri": target_uri,
            "batch_size": 10000,
        }
    )
