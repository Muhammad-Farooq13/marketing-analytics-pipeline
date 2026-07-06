# syntax=docker/dockerfile:1

FROM python:3.12-slim

LABEL maintainer="Muhammad Farooq Shafi <mfarooqsgafee333@gmail.com>" \
      description="Marketing analytics ELT pipeline (DuckDB-based, batch job)"

RUN groupadd -r appuser && useradd -r -g appuser appuser

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY dags/ ./dags/

RUN mkdir -p data/raw data/warehouse docs assets && chown -R appuser:appuser /app
USER appuser

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# This image runs the pipeline as a BATCH JOB (not a long-running server --
# there's nothing to health-check or expose a port for; the "product" is the
# DuckDB warehouse file and the DQ/report artifacts it produces).
CMD ["python", "-m", "src.pipeline.dag"]
