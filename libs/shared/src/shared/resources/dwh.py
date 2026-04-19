import os
from shared.loaders.csv import CsvToSqlLoader
import pyodbc
from contextlib import contextmanager
import dagster as dg
import logging
from jinja2 import Template
import boto3
import csv
import smart_open
from pyodbc import DatabaseError

# Environment configuration (dev/prod)
deployment_name = os.getenv("DAGSTER_DEPLOYMENT", "dev")


class SqlServerConnectionResource(dg.ConfigurableResource):
    """
    SQL Server connection resource for data warehouse access.
    """

    server: str
    port: int = 1433  # Default value
    username: str
    password: str
    driver: str = "ODBC Driver 18 for SQL Server"  # Exposed so the version can be changed if needed
    environment: str = deployment_name  # "dev" or "prod"

    def _create_connection_string(self) -> str:
        return (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.server},{self.port};"
            f"UID={self.username};"
            f"PWD={self.password};"
            "TrustServerCertificate=yes;"
        )

    @contextmanager
    def get_connection(self, db_name: str = None):
        conn = None
        conn_string = self._create_connection_string()

        # OVERRIDE: Connect to temp analytics database in dev
        if self.environment == "dev":
            analitika_db = os.getenv("DB_DATABASE_DEV")
            conn_string += f"DATABASE={analitika_db};"
        elif db_name:
            conn_string += f"DATABASE={db_name};"
        else:
            logging.warning("No database specified, connecting without a default database.")

        try:
            # autocommit=False is the default, but explicit is better
            conn = pyodbc.connect(conn_string, autocommit=False)
            yield conn
        except pyodbc.Error as e:
            logging.error(f"Failed to connect to SQL Server: {e}")
            raise RuntimeError(f"Database connection failed: {e}") from e
        finally:
            if conn:
                try:
                    conn.close()
                except Exception as e:
                    logging.warning(f"Failed to close SQL Server connection: {e}")

    @contextmanager
    def get_cursor(self, db_name: str = None):
        with self.get_connection(db_name=db_name) as conn:
            cursor = None
            try:
                cursor = conn.cursor()
                # Enable fast_executemany for bulk insert performance (10-100x faster)
                cursor.fast_executemany = True
                yield cursor
                # Automatically commit if no error occurred in the block
                conn.commit()
            except Exception as e:
                conn.rollback()  # Roll back on failure
                logging.error(f"SQL execution error: {e}")
                raise RuntimeError(f"Cursor operation failed: {e}") from e
            finally:
                if cursor:
                    try:
                        cursor.close()
                    except Exception:
                        pass

    def _load_sql_file(self, file_path: str) -> str:
        """Loads a SQL file from an absolute path or a path relative to the execution root."""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"SQL file not found at: {file_path}")

        with open(file_path, "r", encoding="utf-8") as file:
            return file.read()

    def execute_sql(
        self, sql_statement: str, db_name: str, params: tuple = None, as_dict: bool = False
    ):
        """
        Executes a raw SQL string.
        This is a low-level method used by execute_templated_sql,
        but it can also be called directly.

        Args:
            sql_statement: SQL string to execute.
            params: Tuple values for parameterized SQL queries (for ? placeholders).
        """
        with self.get_cursor(db_name=db_name) as cursor:
            # Log the first 100 query characters to avoid log spam
            log_snippet = sql_statement[:100].replace("\n", " ") + "..."
            logging.info(f"Executing SQL: {log_snippet}")

            if params:
                cursor.execute(sql_statement, params)
            else:
                cursor.execute(sql_statement)

            if cursor.description is not None:
                if as_dict:
                    # Get column names
                    columns = [column[0] for column in cursor.description]
                    # Convert rows to dictionaries { 'NAME': 'Ivan', ... }
                    return [dict(zip(columns, row)) for row in cursor.fetchall()]
                else:
                    return cursor.fetchall()

            # For INSERT/UPDATE/DDL we do not return a result
            return None

    def execute_templated_sql(
        self,
        sql_path: str,
        db_name: str,
        params: tuple = None,
        as_dict: bool = False,
        **template_vars,
    ):
        """
        Loads a file, renders a Jinja template, and passes the result to execute_sql.

        Args:
            sql_path: Path to a .sql file.
            params: Tuple values for parameterized SQL queries.
            **template_vars: Variables passed into the Jinja template.
        """
        # 1. Load file contents
        sql_template = self._load_sql_file(sql_path)

        # 2. Render Jinja template (replace {{ variables }})
        template = Template(sql_template)
        rendered_sql = template.render(**template_vars)

        logging.info(f"Preparing to execute SQL from file: {sql_path}")

        # 3. Execute SQL
        return self.execute_sql(rendered_sql, db_name=db_name, params=params, as_dict=as_dict)

    # Load CSV into a table
    def load_csv(
        self,
        file_path: str,
        table_name: str,
        db_name: str = None,
        schema="dbo",
        yaml_schema: str | dict = None,
        storage_options: dict = None,
        delimiter=";",
        encoding=None,
        na_values=None,
        batch_size=10000,
        quoting: int = None,
    ):
        """
        Delegates work to CsvToSqlLoader within a safe transaction.
        """
        # 1. Create loader worker
        loader = CsvToSqlLoader(batch_size=batch_size)

        # 2. Open connection (resource manager)
        with self.get_cursor(db_name=db_name) as cursor:
            # 3. Pass cursor to worker
            rows = loader.load(
                cursor=cursor,
                csv_path=file_path,
                table_name=table_name,
                schema=schema,
                yaml_config=yaml_schema,
                storage_options=storage_options,
                delimiter=delimiter,
                encoding=encoding,
                na_values=na_values,
                quoting=quoting,
            )

            return rows

    def get_table_row_count(self, table_name: str, db_name: str, schema: str = None) -> dict:
        """
        Returns row count metadata.
        """
        # If schema is provided, prefix table name with schema
        full_table_name = f"{schema}.{table_name}" if schema else table_name

        query = f"SELECT COUNT(*) FROM {full_table_name}"

        with self.get_cursor(db_name=db_name) as cursor:
            try:
                row_count = cursor.execute(query).fetchone()
                count = row_count[0] if row_count else 0
            except pyodbc.Error as e:
                # If table does not exist or query fails, return 0 and log warning
                logging.warning(f"Could not get row count for {full_table_name}: {e}")
                count = 0

        return count


    def export_csv_in_batches(
        self,
        s3_client: boto3.client,
        table_name: str,
        s3_bucket: str,
        s3_key: str,
        filename: str,
        db_name: str,
        schema: str = "dbo",
        batch_size: int = 10000,
        columns: list | None = None,
        delimiter: str = "|",
    ):
        """
        Export table data to S3 in batches as a compressed CSV file.
        
        Args:
            s3_client: Boto3 S3 client for uploading to S3.
            table_name: Name of the table to export.
            s3_bucket: S3 bucket name (e.g., '05-curated').
            s3_key: S3 key prefix/path (e.g., 'OMOP/2025_NAJS_OMOP_r2').
            filename: Output filename (e.g., 'person.csv.gz').
            db_name: Database name to connect to.
            schema: Schema name (default: 'dbo').
            batch_size: Number of rows to fetch per batch (default: 10000).
            columns: List of column names in desired order. If None, uses SELECT *.
            delimiter: CSV delimiter character (default: '|').
        
        Returns:
            None
        
        Raises:
            DatabaseError: If database query fails.
            Exception: If S3 upload fails.
        """
        # Construct S3 URI from components
        s3_uri = f"s3://{s3_bucket}/{s3_key.strip('/')}/{filename}"
        
        # Prepare column selection
        if columns:
            select_cols = ", ".join(columns)
        else:
            select_cols = "*"

        full_table_name = f"{schema}.{table_name}"

        with self.get_cursor(db_name=db_name) as cursor:
            try:
                logging.info(f"Starting export of {full_table_name} to {s3_uri}")
                
                # Execute query
                cursor.execute(f"SELECT {select_cols} FROM {full_table_name}")
                
                # Get column names if not provided
                if not columns:
                    columns = [desc[0] for desc in cursor.description]

                # Open S3 file and write in batches
                with smart_open.open(
                    s3_uri,
                    "wt",
                    transport_params={"client": s3_client},
                    encoding="utf-8",
                ) as csvfile:
                    writer = csv.writer(csvfile, delimiter=delimiter, quoting=csv.QUOTE_MINIMAL)
                    
                    # Write header
                    writer.writerow(columns)
                    
                    # Write data in batches
                    row_count = 0
                    while True:
                        rows = cursor.fetchmany(batch_size)
                        if not rows:
                            break
                        
                        writer.writerows(rows)
                        row_count += len(rows)
                    
                    logging.info(f"Successfully exported {row_count} rows to {s3_uri}")

            except DatabaseError as db_err:
                logging.error(f"Database error during export of {full_table_name}: {db_err}")
                raise
            except Exception as e:
                logging.error(f"Error uploading {full_table_name} to S3: {e}")
                raise


if deployment_name == "dev":
    dwh_resource = SqlServerConnectionResource(
        server=dg.EnvVar("DB_SERVER_DEV"),
        port=dg.EnvVar.int("DB_PORT_DEV"),
        username=dg.EnvVar("DB_USERNAME_DEV"),
        password=dg.EnvVar("DB_PASSWORD_DEV"),
    )
else:
    dwh_resource = SqlServerConnectionResource(
        server=dg.EnvVar("DB_SERVER"),
        port=dg.EnvVar.int("DB_PORT"),
        username=dg.EnvVar("DB_USERNAME"),
        password=dg.EnvVar("DB_PASSWORD"),
    )
