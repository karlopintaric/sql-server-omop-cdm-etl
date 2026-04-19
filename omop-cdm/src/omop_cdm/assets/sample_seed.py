from pathlib import Path
import os

import dagster as dg

from shared.resources.dwh import SqlServerConnectionResource


@dg.asset(
    group_name="sample_seed",
    kinds=["python", "csv", "sqlserver"],
    name="load_sample_source_events",
)
def load_sample_source_events(
    context: dg.AssetExecutionContext,
    dwh: SqlServerConnectionResource,
):
    csv_path = Path(__file__).resolve().parents[3] / "data" / "sample_condition_events.csv"
    schema = os.getenv("SOURCE_SCHEMA", "stage")
    database = os.getenv("DB_DATABASE") or os.getenv("DB_DATABASE_DEV")

    if not database:
        raise RuntimeError("Set DB_DATABASE or DB_DATABASE_DEV before running load_sample_source_events")

    dwh.execute_sql(
        f"DROP TABLE IF EXISTS {schema}.source_events;",
        db_name=database,
    )

    rows = dwh.load_csv(
        file_path=str(csv_path),
        table_name="source_events",
        db_name=database,
        schema=schema,
        delimiter=",",
        batch_size=10000,
    )

    context.log.info(f"Loaded {rows} sample rows into {schema}.source_events")

    return dg.MaterializeResult(
        metadata={
            "rows_loaded": rows,
            "target_table": f"{schema}.source_events",
            "source_csv": str(csv_path),
        }
    )
