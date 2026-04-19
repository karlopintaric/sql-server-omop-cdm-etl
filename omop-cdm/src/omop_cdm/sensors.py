import os
from pathlib import Path

import dagster as dg

from shared.sensors.run_status import DagsterEmailNotificationService


EMAIL_RECIPIENTS_FILE = Path(__file__).parent / "email_recipients.yaml"
MONITORED_JOBS = {
    "load_csvs",
    "build_static_entities",
    "transform_event_data",
    "run_all_data_quality",
    "export_to_s3",
}


def _build_email_service() -> DagsterEmailNotificationService | None:
    required_vars = [
        "EXCHANGE_USERNAME",
        "EXCHANGE_PASSWORD",
        "EXCHANGE_SERVER",
        "EXCHANGE_EMAIL",
    ]
    missing = [name for name in required_vars if not os.getenv(name)]
    if missing:
        return None

    return DagsterEmailNotificationService(
        exchange_username=os.environ["EXCHANGE_USERNAME"],
        exchange_password=os.environ["EXCHANGE_PASSWORD"],
        exchange_server=os.environ["EXCHANGE_SERVER"],
        exchange_email=os.environ["EXCHANGE_EMAIL"],
        email_recipients_file=EMAIL_RECIPIENTS_FILE,
        dagster_ui_url=os.getenv("DAGSTER_UI_URL", "http://localhost:3001"),
    )


@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.SUCCESS,
    minimum_interval_seconds=int(os.getenv("SENSOR_INTERVAL", "60")),
)
def email_on_run_success(context: dg.RunStatusSensorContext):
    job_name = context.dagster_run.job_name or ""
    if job_name not in MONITORED_JOBS:
        return dg.SkipReason(f"Job '{job_name}' is not monitored for email alerts")

    service = _build_email_service()
    if not service:
        return dg.SkipReason("Exchange credentials not configured; skipping success email")

    service.send_success_notification(job_name=job_name, run_id=context.dagster_run.run_id)


@dg.run_failure_sensor(
    minimum_interval_seconds=int(os.getenv("SENSOR_INTERVAL", "60")),
)
def email_on_run_failure(context: dg.RunFailureSensorContext):
    job_name = context.dagster_run.job_name or ""
    if job_name not in MONITORED_JOBS:
        return dg.SkipReason(f"Job '{job_name}' is not monitored for email alerts")

    service = _build_email_service()
    if not service:
        return dg.SkipReason("Exchange credentials not configured; skipping failure email")

    service.send_failure_notification(
        job_name=job_name,
        run_id=context.dagster_run.run_id,
        failure_context=context,
    )