# syntax=docker/dockerfile:1.6

# ============================================================================
# DAGSTER DEPLOYMENT CONTAINER
# ============================================================================
# This Dockerfile builds the main Dagster container that runs:
# - dagster-webserver (web UI)
# - dagster-daemon (background services)
#
# The container does not include user code - it connects to user code containers
# via gRPC connections defined in workspace.yaml
# ============================================================================

FROM python:3.13-slim-bookworm

# ----------------------------------------------------------------------------
# Base settings + DAGSTER_HOME
# ----------------------------------------------------------------------------
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DAGSTER_HOME=/opt/dagster/dagster_home \
    VIRTUAL_ENV=/opt/venv \
    PATH="/opt/venv/bin:$PATH"

# Working directory (root for all Dagster files)
WORKDIR /opt/dagster

# ----------------------------------------------------------------------------
# Install uv (faster pip replacement) + Dagster core components in venv
# ----------------------------------------------------------------------------
# uv is a standalone binary - apt/curl not required
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Create virtualenv + install Dagster core components (cache-friendly)
RUN python -m venv "$VIRTUAL_ENV" \
 && --mount=type=cache,target=/root/.cache/uv \
    uv pip install --python "$VIRTUAL_ENV/bin/python" \
      dagster \
      dagster-graphql \
      dagster-webserver \
      dagster-postgres \
      dagster-docker

# dagster - core Dagster framework
# dagster-graphql - GraphQL API for the web UI
# dagster-webserver - web UI service
# dagster-postgres - PostgreSQL storage backend
# dagster-docker - Docker run launcher for job execution

# ----------------------------------------------------------------------------
# Create required directories + copy configuration
# ----------------------------------------------------------------------------
RUN mkdir -p $DAGSTER_HOME

# Copy configuration (workspace.yaml defines gRPC user code locations)
COPY dagster.yaml workspace.yaml $DAGSTER_HOME/

# Set working directory (Dagster expects DAGSTER_HOME here)
WORKDIR $DAGSTER_HOME
