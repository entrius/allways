# syntax=docker/dockerfile:1.4
FROM python:3.12-slim-bookworm

# Install system dependencies
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    build-essential curl git \
 && rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install --break-system-packages uv

WORKDIR /app

# Point uv at /opt/venv so `uv sync` installs there (default is .venv in cwd).
ENV VENV_DIR=/opt/venv
ENV VIRTUAL_ENV=$VENV_DIR
ENV UV_PROJECT_ENVIRONMENT=$VENV_DIR
ENV PATH="$VENV_DIR/bin:$PATH"

# Install locked deps first (source not yet present) — cache-friendly.
COPY pyproject.toml uv.lock ./
RUN uv venv --python python3 $VENV_DIR && uv sync --frozen --no-install-project

# Install the project itself against the locked dep set.
COPY . .
RUN uv sync --frozen
