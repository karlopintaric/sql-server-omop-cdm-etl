import os
import sys
from pathlib import Path
import dagster as dg

from shared.resources.dwh import SqlServerConnectionResource
from shared.resources.s3 import S3Resource
from dagster_dbt import DbtCliResource, DbtProject
from dagster_docker import PipesDockerClient

deployment_name = os.getenv("DAGSTER_DEPLOYMENT", "dev")


def _db_env_for_deployment() -> dict[str, dg.EnvVar | str]:
    if deployment_name == "dev":
        return {
            "DB_DBMS": "sql server",
            "DB_SERVER": dg.EnvVar("DB_SERVER_DEV"),
            "DB_PORT": dg.EnvVar("DB_PORT_DEV"),
            "DB_NAME": dg.EnvVar("DB_DATABASE_DEV"),
            "DB_USER": dg.EnvVar("DB_USERNAME_DEV"),
            "DB_PASSWORD": dg.EnvVar("DB_PASSWORD_DEV"),
        }

    return {
        "DB_DBMS": "sql server",
        "DB_SERVER": dg.EnvVar("DB_SERVER"),
        "DB_PORT": dg.EnvVar("DB_PORT"),
        "DB_NAME": dg.EnvVar("DB_DATABASE"),
        "DB_USER": dg.EnvVar("DB_USERNAME"),
        "DB_PASSWORD": dg.EnvVar("DB_PASSWORD"),
    }


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

s3_resource = S3Resource(
    endpoint_url=dg.EnvVar("S3_ENDPOINT"),
    aws_access_key_id=dg.EnvVar("S3_ACCESS_KEY_ID"),
    aws_secret_access_key=dg.EnvVar("S3_SECRET_KEY"),
)

docker_pipes_client = PipesDockerClient(_db_env_for_deployment())

dbt_project_directory = Path(__file__).parent.parent.parent / "dbt"
dbt_project = DbtProject(
    project_dir=dbt_project_directory,
    target=deployment_name,
)

dbt_resource = DbtCliResource(
    project_dir=dbt_project,
    dbt_executable=str(Path(sys.executable).with_name("dbt")),
)

dbt_project.prepare_if_dev()
