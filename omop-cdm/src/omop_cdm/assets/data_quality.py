import os
import shlex
import subprocess

import dagster as dg


CONDITION_OCCURRENCE_ASSET_KEY = dg.AssetKey(["condition_occurrence"])


def _get_db_env() -> dict[str, str]:
    deployment = os.getenv("DAGSTER_DEPLOYMENT", "dev")

    if deployment == "dev":
        return {
            "DB_SERVER": os.getenv("DB_SERVER_DEV", ""),
            "DB_PORT": os.getenv("DB_PORT_DEV", "1433"),
            "DB_DATABASE": os.getenv("DB_DATABASE_DEV", ""),
            "DB_USERNAME": os.getenv("DB_USERNAME_DEV", ""),
            "DB_PASSWORD": os.getenv("DB_PASSWORD_DEV", ""),
        }

    return {
        "DB_SERVER": os.getenv("DB_SERVER", ""),
        "DB_PORT": os.getenv("DB_PORT", "1433"),
        "DB_DATABASE": os.getenv("DB_DATABASE", ""),
        "DB_USERNAME": os.getenv("DB_USERNAME", ""),
        "DB_PASSWORD": os.getenv("DB_PASSWORD", ""),
    }


@dg.asset(
    group_name="sample_data_quality",
    kinds=["docker", "data-quality"],
    deps=[CONDITION_OCCURRENCE_ASSET_KEY],
    name="run_data_quality_dashboard_container",
)
def run_data_quality_dashboard_container(context: dg.AssetExecutionContext):
    image = os.getenv("DATA_QUALITY_DASHBOARD_IMAGE", "data_quality_dashboard:latest")
    command = os.getenv("DATA_QUALITY_DASHBOARD_COMMAND", "")

    return _run_data_quality_container(context=context, image=image, command=command, check_name="dashboard")


@dg.asset(
    group_name="sample_data_quality",
    kinds=["docker", "data-quality", "onboarding"],
    deps=[CONDITION_OCCURRENCE_ASSET_KEY],
    name="run_cdm_onboarding_container",
)
def run_cdm_onboarding_container(context: dg.AssetExecutionContext):
    image = os.getenv("CDM_ONBOARDING_IMAGE", "cdm_onboarding:latest")
    command = os.getenv("CDM_ONBOARDING_COMMAND", "")

    return _run_data_quality_container(context=context, image=image, command=command, check_name="cdm_onboarding")


@dg.asset(
    group_name="sample_data_quality",
    kinds=["docker", "data-quality", "achilles"],
    deps=[CONDITION_OCCURRENCE_ASSET_KEY],
    name="run_achilles_container",
)
def run_achilles_container(context: dg.AssetExecutionContext):
    image = os.getenv("ACHILLES_IMAGE", "achilles:latest")
    command = os.getenv("ACHILLES_COMMAND", "")

    return _run_data_quality_container(context=context, image=image, command=command, check_name="achilles")


def _run_data_quality_container(
    context: dg.AssetExecutionContext,
    image: str,
    command: str,
    check_name: str,
):
    if not image:
        raise RuntimeError(f"Set image for {check_name} data-quality container")

    db_env = _get_db_env()

    missing = [k for k in ("DB_SERVER", "DB_DATABASE", "DB_USERNAME", "DB_PASSWORD") if not db_env.get(k)]
    if missing:
        raise RuntimeError(f"Missing database env vars for data-quality container: {', '.join(missing)}")

    cmd = ["docker", "run", "--rm"]
    for key, value in db_env.items():
        cmd.extend(["-e", f"{key}={value}"])

    cmd.append(image)
    if command:
        cmd.extend(shlex.split(command))

    context.log.info(f"Running {check_name} data-quality container with image {image}")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.stdout:
        context.log.info(result.stdout[-4000:])
    if result.stderr:
        context.log.warning(result.stderr[-4000:])

    if result.returncode != 0:
        raise dg.Failure(
            description="Data-quality container failed",
            metadata={
                "check": check_name,
                "image": image,
                "return_code": result.returncode,
            },
        )

    return dg.MaterializeResult(
        metadata={
            "check": check_name,
            "image": image,
            "return_code": result.returncode,
        }
    )
