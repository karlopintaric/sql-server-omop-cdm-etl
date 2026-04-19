import pandas as pd
import numpy as np
import yaml
import re
from typing import BinaryIO, Tuple
import pyodbc


class CsvToSqlLoader:
    """
    Class responsible for loading CSV files into SQL.
    """

    def __init__(self, batch_size: int = 10000):
        self.batch_size = batch_size

    # --- PRIVATE HELPERS ---

    def _detect_encoding_and_read_sample(
        self,
        csv_path: str | BinaryIO,
        delimiter: str = ";",
        encoding: str = None,
        storage_options: dict = None,
        sample_rows: int = 1000,
        na_values: list = [""],
        quoting: int = None,
    ) -> Tuple[str, pd.DataFrame]:
        """
        Tries to read a CSV sample using a list of encodings.
        Returns (detected_encoding, sample_dataframe).
        """

        # 1. Candidate encodings
        # - utf-8 (strict standard)
        # - cp1250 (Croatian/Central European - should be tried before cp1252)
        # - cp1252 (Western - permissive fallback)
        candidate_encodings = ["utf-8", "cp1250", "cp1252"]

        # Use user-provided encoding if available, otherwise try candidates in order.
        encodings_to_try = [encoding] if encoding else candidate_encodings

        final_encoding = None
        sample_df = None

        print(f"Detecting encoding... Candidates: {encodings_to_try}")

        for enc in encodings_to_try:
            try:
                # Reset pointer if this is a file-like object (important for retries)
                if hasattr(csv_path, "seek"):
                    csv_path.seek(0)

                # Read only the sample
                read_params = {
                    "filepath_or_buffer": csv_path,
                    "sep": delimiter,
                    "nrows": sample_rows,
                    "encoding": enc,
                    "storage_options": storage_options,
                    "na_values": na_values,
                    "keep_default_na": False,  # Only use na_values, don't treat 'None', 'null' etc as NaN
                }
                
                # Add quoting parameter if specified
                if quoting is not None:
                    read_params["quoting"] = quoting
                
                sample_df = pd.read_csv(**read_params)

                # If we reached this point, encoding works
                final_encoding = enc
                print(f"Success! Detected encoding: {final_encoding}")
                break

            except UnicodeDecodeError:
                # Not this encoding, try the next one
                continue
            except Exception as e:
                # Another error (e.g., file not found); no point trying more encodings
                raise e

        if final_encoding is None or sample_df is None:
            raise ValueError(
                f"Failed to read CSV with any of candidates: {encodings_to_try}"
            )

        return final_encoding, sample_df

    def _parse_yaml_schema(self, yaml_config: str | dict):
        """Returns column definitions and aliases from YAML."""
        if isinstance(yaml_config, str):
            with open(yaml_config, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
        else:
            config = yaml_config

        col_defs = []  # For CREATE TABLE
        target_cols = []  # For INSERT (order)
        alias_map = {}  # For RENAME
        yaml_date_cols = []  # Date columns from YAML
        date_formats = {}  # Format for each date column

        for col in config["columns"]:
            name = col["column"]
            dtype = col["datatype"]
            col_defs.append(f"[{name}] {dtype}")
            target_cols.append(name)

            # Alias mapping
            alias_map[name.lower()] = name
            if "aliases" in col:
                for alias in col["aliases"]:
                    alias_map[alias.lower()] = name

            # Check whether this is a date/time column
            if re.search(r"(date|time)", dtype, re.IGNORECASE):
                yaml_date_cols.append(name)
                # Extract explicit format if provided
                if "date_format" in col:
                    date_formats[name] = col["date_format"]

        return col_defs, target_cols, alias_map, yaml_date_cols, date_formats

    def _infer_schema(self, sample_df: pd.DataFrame):
        """Analyzes a Pandas DataFrame and returns SQL definitions."""
        col_defs = []
        target_cols = sample_df.columns.tolist()
        date_cols = []

        # Date regex
        for col in target_cols:
            if re.search(r"(date|datum|vrijeme)", col, re.IGNORECASE):
                date_cols.append(col)

        for col in target_cols:
            if col in date_cols:
                sql_type = "DATETIME2"
            else:
                # SAFE MODE: Everything except date columns goes to NVARCHAR(MAX)
                sql_type = "NVARCHAR(MAX)"

            col_defs.append(f"[{col}] {sql_type}")

        return col_defs, target_cols, date_cols

    def _transform_batch(
        self, batch_df: pd.DataFrame, strategy: dict
    ) -> list[tuple] | None:
        """Cleans data and prepares a list of tuples."""
        if batch_df.empty:
            return None

        # 1. Rename
        if strategy.get("rename_map"):
            batch_df.rename(columns=strategy["rename_map"], inplace=True)

        # 2. Reindex (Osigurava redoslijed)
        batch_df = batch_df.reindex(columns=strategy["target_columns"])

        # 3. Convert to numpy array with NaN -> None conversion (fast and automatic)
        # Using dtype=object with na_value=None ensures SQL Server gets proper NULLs
        arr = batch_df.to_numpy(dtype=object, na_value=None)
        
        return [tuple(row) for row in arr]

    def load(
        self,
        cursor: pyodbc.Cursor,
        csv_path: str | BinaryIO,
        table_name: str,
        schema: str,
        yaml_config: str | dict = None,
        storage_options: dict = None,
        delimiter: str = ";",
        encoding: str = None,
        na_values: list | None = None,
        quoting: int = None,
    ) -> int:
        """
        Main method that orchestrates the full process.
        """
        print(f"--- [Loader] Starting CSV load for {schema}.{table_name} ---")

        # 1. Prepare strategy (encoding and schema)
        final_encoding, sample_df = self._detect_encoding_and_read_sample(
            csv_path, delimiter, encoding, storage_options, na_values=na_values, quoting=quoting
        )

        strategy = {
            "encoding": final_encoding,
            "col_defs": [],
            "target_columns": [],
            "rename_map": {},
            "mode": "YAML" if yaml_config else "INFERENCE",
            "date_cols": [],
            "date_formats": {},
        }

        if yaml_config:
            col_defs, target_cols, alias_map, yaml_date_cols, date_formats = self._parse_yaml_schema(
                yaml_config
            )
            strategy["col_defs"] = col_defs
            strategy["target_columns"] = target_cols
            strategy["date_cols"] = yaml_date_cols
            strategy["date_formats"] = date_formats
            # Header mapping...
            for col in sample_df.columns:
                clean = col.lower().strip()
                if clean in alias_map:
                    strategy["rename_map"][col] = alias_map[clean]
        else:
            col_defs, target_cols, date_cols = self._infer_schema(sample_df)
            strategy["col_defs"] = col_defs
            strategy["target_columns"] = target_cols
            strategy["date_cols"] = date_cols

        # 2. DDL (create table)
        full_table_name = f"[{schema}].[{table_name}]"
        ddl = f"""
        IF OBJECT_ID('{full_table_name}', 'U') IS NOT NULL DROP TABLE {full_table_name};
        CREATE TABLE {full_table_name} ({", ".join(strategy["col_defs"])});
        """
        cursor.execute(ddl)
        print(f"--- [Loader] Table created: {full_table_name}")

        # 3. Insert loop
        if hasattr(csv_path, "seek"):
            csv_path.seek(0)

        read_kwargs = {
            "filepath_or_buffer": csv_path,
            "sep": delimiter,
            "chunksize": self.batch_size,
            "encoding": final_encoding,
            "storage_options": storage_options,
            "na_values": na_values,
            "keep_default_na": False,  # Only use na_values, don't treat 'None', 'null' etc as NaN
            "dayfirst": True,
        }
        
        # Add quoting parameter if specified
        if quoting is not None:
            read_kwargs["quoting"] = quoting

        # Prepare list of date columns for parse_dates
        csv_date_cols = []

        if strategy["mode"] == "YAML":
            read_kwargs["dtype"] = str

            # We have target date cols (e.g. "DATUM_RODJENJA")
            # We need source CSV cols (e.g. "dob")
            # We look at rename_map: { "dob": "DATUM_RODJENJA" }
            if strategy["date_cols"]:
                for csv_col, target_col in strategy["rename_map"].items():
                    if target_col in strategy["date_cols"]:
                        csv_date_cols.append(csv_col)
                
                # If date formats are specified, build format dict for pandas
                if strategy["date_formats"]:
                    date_format_dict = {}
                    for csv_col, target_col in strategy["rename_map"].items():
                        if target_col in strategy["date_formats"]:
                            date_format_dict[csv_col] = strategy["date_formats"][target_col]
                    
                    if date_format_dict:
                        read_kwargs["date_format"] = date_format_dict
        else:
            # In inference mode, CSV name == DB name
            csv_date_cols = strategy["date_cols"]

        # Apply parse_dates if we found any matching columns in the CSV
        if csv_date_cols:
            read_kwargs["parse_dates"] = csv_date_cols

        total_rows = 0

        # Prepare SQL insert statement once
        cols = [f"[{c}]" for c in strategy["target_columns"]]
        placeholders = ", ".join(["?"] * len(cols))
        insert_sql = (
            f"INSERT INTO {full_table_name} ({', '.join(cols)}) VALUES ({placeholders})"
        )

        for batch in pd.read_csv(**read_kwargs):
            batch_data = self._transform_batch(batch, strategy)

            if batch_data:
                try:
                    cursor.executemany(insert_sql, batch_data)
                    total_rows += len(batch_data)
                except Exception as e:
                    print(f"Error inserting around row {total_rows}")
                    raise e

        return total_rows
