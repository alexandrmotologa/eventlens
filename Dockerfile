# Multi-stage lightweight Dockerfile for EventLens
FROM python:3.12-slim AS builder

WORKDIR /build

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir hatchling uv

COPY pyproject.toml README.md ./
COPY src/ ./src/

RUN uv build --wheel --out-dir /dist

FROM python:3.12-slim AS runtime

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install built wheel
COPY --from=builder /dist/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl && rm -rf /tmp/*.whl

# Create non-root user
RUN useradd -m -u 10001 eventlens && chown -R eventlens:eventlens /app
USER eventlens

EXPOSE 8080

ENTRYPOINT ["eventlens"]
CMD ["web", "--host", "0.0.0.0", "--port", "8080", "--demo"]
