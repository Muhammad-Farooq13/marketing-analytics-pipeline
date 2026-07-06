.PHONY: help install install-dev pipeline report test lint format docker-build docker-run clean

PYTHON := python3

help:
	@echo "Available targets:"
	@echo "  install       Install production dependencies"
	@echo "  install-dev   Install development dependencies"
	@echo "  pipeline      Run the full ELT pipeline (generate -> stage -> DQ -> transform -> gate)"
	@echo "  report        Generate charts + markdown report from the warehouse"
	@echo "  test          Run the test suite"
	@echo "  lint          Run ruff + black --check"
	@echo "  format        Auto-format code with black + ruff --fix"
	@echo "  docker-build  Build the Docker image"
	@echo "  docker-run    Run the pipeline via docker-compose"
	@echo "  clean         Remove generated artifacts"

install:
	pip install -r requirements.txt

install-dev:
	pip install -r requirements-dev.txt

pipeline:
	$(PYTHON) -m src.pipeline.dag

report:
	$(PYTHON) -m src.pipeline.report

test:
	pytest tests/ -v --cov=src --cov-report=term-missing

lint:
	ruff check src/ tests/ dags/
	black --check src/ tests/ dags/

format:
	ruff check --fix src/ tests/ dags/
	black src/ tests/ dags/

docker-build:
	docker build -t marketing-analytics-pipeline:latest .

docker-run:
	docker compose up --build

clean:
	rm -rf data/raw/* data/warehouse/*.duckdb docs/*.json docs/*.md assets/*.png .pytest_cache **/__pycache__
