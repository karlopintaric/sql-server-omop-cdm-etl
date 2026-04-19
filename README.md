# sqlserver-dagster-dbt-omop-etl

Dagster + dbt workspace for OMOP CDM ETL on SQL Server.

This repository includes:

- ETL code and jobs in `omop-cdm/src/omop_cdm`
- dbt project and SQL models in `omop-cdm/dbt`
- Shared resources/loaders in `libs/shared`
- Dagster instance/runtime configuration in `dagster.yaml` and `workspace.yaml`
- Infrastructure for Dagster metadata DB + HTTPS auth proxy in `docker-compose.yaml` and `auth/`

## Repository structure

```text
.
├── .env.example
├── dagster.yaml
├── workspace.yaml
├── docker-compose.yaml
├── dagster-webserver.service
├── dagster-daemon.service
├── start.sh
├── auth/
│   ├── certs/            # TLS cert/key mounted into Traefik
│   └── dynamic/
│       └── dagster.yaml  # Traefik routers/services/middlewares
├── libs/shared/
└── omop-cdm/
        ├── data/
        ├── dbt/
        └── src/omop_cdm/
```

## What Dagster runs

Code location: `omop_cdm` from `omop-cdm/src/omop_cdm/definitions.py` (configured in `workspace.yaml`).

Main jobs:

- `load_csvs`
- `build_static_entities`
- `transform_event_data`
- `run_all_data_quality`
- `export_to_s3`

Job intent:

- `load_csvs`: loads vocabulary CSV files and sample source events CSV into SQL Server.
- `build_static_entities`: runs dbt models tagged `static` (for example `person`).
- `transform_event_data`: runs dbt models tagged `partitioned` with 6-month partitions.
- `run_all_data_quality`: runs dashboard, onboarding, and achilles containers.
- `export_to_s3`: exports `condition_occurrence` to S3.

## Prerequisites

- Python 3.10-3.12
- `uv`
- SQL Server access
- Docker (required for Dagster metadata PostgreSQL and auth proxy stack)
- systemd (only for production service mode)

## Environment setup

Create your env file:

```bash
cp .env.example .env
```

Fill at least these values in `.env`:

- Dagster metadata DB: `DAGSTER_POSTGRES_USER`, `DAGSTER_POSTGRES_PASSWORD`, `DAGSTER_POSTGRES_DB`
- SQL Server: `DB_SERVER`, `DB_PORT`, `DB_DATABASE`, `DB_USERNAME`, `DB_PASSWORD`
- Dagster local dirs: `DAGSTER_LOGS_DIR`, `DAGSTER_ARTIFACTS_DIR`

If running through HTTPS + SSO proxy, also fill:

- `ETL_HOST`
- `OAUTH2_PROXY_OIDC_ISSUER_URL`
- `OAUTH2_PROXY_CLIENT_ID`
- `OAUTH2_PROXY_CLIENT_SECRET`
- `OAUTH2_PROXY_COOKIE_SECRET`
- `OAUTH2_PROXY_COOKIE_DOMAIN`
- `DAGSTER_UI_URL`

## Python dependencies

```bash
uv sync
cd omop-cdm && uv sync
```

## Auth and certificates setup

Traefik and oauth2-proxy are configured in `docker-compose.yaml` and `auth/dynamic/dagster.yaml`.

Certificate files expected by Traefik:

- `auth/certs/fullchain.crt`
- `auth/certs/rsa.key`

These paths are hardcoded in `auth/dynamic/dagster.yaml`:

```yaml
tls:
    certificates:
        - certFile: /certs/fullchain.crt
            keyFile: /certs/rsa.key
```

Keycloak/OIDC side:

- OAuth2 Proxy runs on `127.0.0.1:4180`
- Dagster UI is proxied to `127.0.0.1:3000`
- Set your Keycloak client redirect URI to:
    - `https://<ETL_HOST>/oauth2/callback`

Start auth stack:

```bash
docker compose up -d traefik oauth2-proxy
```

## How to run Dagster

### Development mode

1. Start metadata DB:

```bash
docker compose up -d postgres
```

2. Start Dagster dev server:

```bash
cd omop-cdm
uv run dg dev
```

This gives local Dagster UI, typically on `http://127.0.0.1:3000`.

### Production/systemd mode

Service units are provided in:

- `dagster-webserver.service`
- `dagster-daemon.service`

Important before enabling:

- Replace `User=your-username` and `Group=your-group` in both service files.
- Ensure `DAGSTER_HOME=%h/.local/share/dagster/home` exists and contains `dagster.yaml`.

Then:

```bash
sudo cp dagster-webserver.service /etc/systemd/system/
sudo cp dagster-daemon.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable dagster-webserver dagster-daemon
sudo systemctl start dagster-webserver dagster-daemon
```

Helper restart script:

```bash
./start.sh
```

## Full stack start (metadata + auth)

```bash
docker compose up -d postgres traefik oauth2-proxy
```

## dbt quick usage

```bash
cd omop-cdm/dbt
uv run dbt deps
uv run dbt debug
uv run dbt run
uv run dbt test
```

### dbt model execution layout

- Static models are selected by `tag:static`.
- Partitioned models are selected by `tag:partitioned`.
- Partitioned Dagster dbt asset uses half-year windows (`2014-01`, `2014-07`, ...).

Example local checks:

```bash
cd omop-cdm/dbt
uv run dbt ls --select tag:static --output name
uv run dbt ls --select tag:partitioned --output name
uv run dbt run --target dev --select tag:static
```

## dbt pool concurrency

The dbt Dagster asset is assigned to pool `dbt`.

Set pool limit to 1 (CLI):

```bash
dagster instance concurrency set dbt 1
dagster instance concurrency get
```

Or set it in Dagster UI:

1. Open Dagster UI.
2. Navigate to Concurrency/Pools.
3. Set pool `dbt` limit to `1`.

## Recommended run sequence

1. `load_csvs`
2. `build_static_entities`
3. `transform_event_data` (choose partition key, for example `2014-01`)
4. `run_all_data_quality` (optional; container env/image dependent)
5. `export_to_s3` (optional; requires S3 env vars)

## Data-quality container configuration

Container assets run `docker run --rm` with DB env vars injected.

Required DB vars (resolved from deployment mode):

- `DB_SERVER` / `DB_SERVER_DEV`
- `DB_PORT` / `DB_PORT_DEV`
- `DB_DATABASE` / `DB_DATABASE_DEV`
- `DB_USERNAME` / `DB_USERNAME_DEV`
- `DB_PASSWORD` / `DB_PASSWORD_DEV`

Image and command env vars:

- Dashboard: `DATA_QUALITY_DASHBOARD_IMAGE`, `DATA_QUALITY_DASHBOARD_COMMAND`
- Onboarding: `CDM_ONBOARDING_IMAGE`, `CDM_ONBOARDING_COMMAND`
- Achilles: `ACHILLES_IMAGE`, `ACHILLES_COMMAND`

Note: container failures are surfaced as Dagster asset failures with check name, image, and return code metadata.

## Notes

- `dagster.yaml` expects PostgreSQL metadata DB at `localhost:5455`.
- `workspace.yaml` points to `omop-cdm/.venv/bin/python`.
