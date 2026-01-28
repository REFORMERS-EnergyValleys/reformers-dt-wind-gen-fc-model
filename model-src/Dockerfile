# Build the runtime image
FROM python:3.12-slim AS dependency-builder

WORKDIR /app

# Install git to allow uv to fetch packages from git repositories
RUN apt-get update && \
    apt-get install -y --no-install-recommends git

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install dependencies using uv
COPY pyproject.toml ./
COPY uv.lock* ./
RUN if [ -f uv.lock ]; then uv sync --frozen; else uv sync; fi

FROM python:3.12-slim

WORKDIR /app

ARG VIRTUAL_ENV=/app/.venv

# Collect resources from previous build stage.
COPY --from=dependency-builder /app/.venv ${VIRTUAL_ENV}
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Copy runtime source code
COPY default-configs ./default-configs
COPY eolica-runtime ./eolica-runtime
COPY config.yml ./config.yml

CMD ["python", "-m", "eolica-runtime", "-c", "config.yml"]
