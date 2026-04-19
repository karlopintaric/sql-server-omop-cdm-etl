import os
from pathlib import Path

import dagster as dg

from shared.resources.dwh import SqlServerConnectionResource


OMOP_VOCAB_TABLES = [
    "CONCEPT",
    "CONCEPT_ANCESTOR",
    "CONCEPT_CLASS",
    "CONCEPT_RELATIONSHIP",
    "CONCEPT_SYNONYM",
    "DOMAIN",
    "DRUG_STRENGTH",
    "RELATIONSHIP",
    "VOCABULARY",
]


def _parse_bool(value: str, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _resolve_delimiter(value: str | None) -> str:
    if not value:
        return "\t"
    if value == "\\t":
        return "\t"
    return value


def _find_vocab_csv(vocab_dir: Path, table_name: str) -> Path | None:
    candidates = [
        vocab_dir / f"{table_name}.csv",
        vocab_dir / f"{table_name.lower()}.csv",
        vocab_dir / f"{table_name.upper()}.csv",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


@dg.asset(
    group_name="sample_seed",
    kinds=["python", "csv", "sqlserver", "vocabulary"],
    name="load_omop_vocabularies_from_csv",
)
def load_omop_vocabularies_from_csv(
    context: dg.AssetExecutionContext,
    dwh: SqlServerConnectionResource,
):
    vocab_dir = Path(os.getenv("VOCAB_CSV_DIR", str(Path(__file__).resolve().parents[3] / "data" / "vocabulary")))
    vocab_schema = os.getenv("VOCAB_SCHEMA", "cdm_omop_vocabulary")
    vocab_tables = OMOP_VOCAB_TABLES
    delimiter = _resolve_delimiter(os.getenv("VOCAB_CSV_DELIMITER", "\\t"))
    skip_missing = _parse_bool(os.getenv("VOCAB_SKIP_MISSING", "true"), default=True)
    drop_existing = _parse_bool(os.getenv("VOCAB_DROP_EXISTING", "true"), default=True)

    database = os.getenv("DB_DATABASE") or os.getenv("DB_DATABASE_DEV")
    if not database:
        raise RuntimeError("Set DB_DATABASE or DB_DATABASE_DEV before running load_omop_vocabularies_from_csv")

    if not vocab_dir.exists():
        raise RuntimeError(f"Vocabulary CSV directory not found: {vocab_dir}")

    loaded_tables: list[str] = []
    missing_tables: list[str] = []
    total_rows = 0

    for table_name in vocab_tables:
        csv_path = _find_vocab_csv(vocab_dir, table_name)
        if not csv_path:
            if skip_missing:
                missing_tables.append(table_name)
                context.log.warning(f"Skipping missing vocabulary CSV for table {table_name}")
                continue
            raise RuntimeError(f"Missing vocabulary CSV for table {table_name} under {vocab_dir}")

        if drop_existing:
            dwh.execute_sql(
                f"DROP TABLE IF EXISTS {vocab_schema}.{table_name};",
                db_name=database,
            )

        rows = dwh.load_csv(
            file_path=str(csv_path),
            table_name=table_name,
            db_name=database,
            schema=vocab_schema,
            delimiter=delimiter,
            batch_size=10000,
        )

        loaded_tables.append(table_name)
        total_rows += int(rows)
        context.log.info(f"Loaded {rows} rows into {vocab_schema}.{table_name} from {csv_path.name}")

    return dg.MaterializeResult(
        metadata={
            "database": database,
            "vocab_schema": vocab_schema,
            "vocab_dir": str(vocab_dir),
            "loaded_table_count": len(loaded_tables),
            "loaded_tables": ",".join(loaded_tables),
            "missing_table_count": len(missing_tables),
            "missing_tables": ",".join(missing_tables),
            "total_rows_loaded": total_rows,
        }
    )
