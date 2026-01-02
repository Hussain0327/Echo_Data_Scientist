.PHONY: help install dev test lint format clean docker-up docker-down dbt-run dbt-test prefect-start pipeline generate-data benchmark lint-sql

help:
	@echo "Available commands:"
	@echo ""
	@echo "Development:"
	@echo "  install        Install dependencies"
	@echo "  dev            Start development server"
	@echo "  test           Run tests"
	@echo ""
	@echo "Docker:"
	@echo "  docker-up      Start all services"
	@echo "  docker-down    Stop all services"
	@echo "  docker-logs    Stream container logs"
	@echo ""
	@echo "dbt:"
	@echo "  dbt-run        Run dbt models"
	@echo "  dbt-test       Run dbt tests"
	@echo "  dbt-build      Run and test dbt models"
	@echo "  dbt-docs       Generate and serve dbt docs"
	@echo "  dbt-lineage    View dbt lineage graph"
	@echo ""
	@echo "Data Generation & Benchmarks:"
	@echo "  generate-data  Generate synthetic data (1M rows)"
	@echo "  benchmark      Run query benchmarks"
	@echo ""
	@echo "Orchestration:"
	@echo "  prefect-start  Start Prefect server"
	@echo "  pipeline       Run the daily metrics pipeline"
	@echo ""
	@echo "Quality:"
	@echo "  lint-sql       Lint SQL with SQLFluff"
	@echo "  gx-docs        Build Great Expectations docs"

install:
	pip3 install -r requirements.txt
	cd dbt && dbt deps

dev:
	python3 -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

test:
	python3 -m pytest tests/ -v --cov=app --cov-report=term-missing

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

docker-logs:
	docker-compose logs -f

dbt-deps:
	cd dbt && dbt deps

dbt-run:
	cd dbt && dbt run

dbt-test:
	cd dbt && dbt test

dbt-build:
	cd dbt && dbt build

dbt-docs:
	cd dbt && dbt docs generate && dbt docs serve

prefect-start:
	python3 -m prefect server start

prefect-worker:
	python3 -m prefect worker start -p default-pool

pipeline:
	python3 -m orchestration.flows.daily_metrics

pipeline-ingest:
	python3 -m orchestration.flows.data_ingestion

pipeline-experiment:
	python3 -m orchestration.flows.experiment_analysis

gx-docs:
	cd data_quality && python3 -m great_expectations docs build

# Data Generation & Benchmarks
generate-data:
	python3 -m generators.cli generate --scale 1M --output-dir ./data/generated/1M

generate-data-10m:
	python3 -m generators.cli generate --scale 10M --output-dir ./data/generated/10M --format parquet

benchmark:
	python3 -m benchmarks.run_all --table transactions --output benchmarks/results.md

# dbt lineage
dbt-lineage:
	cd dbt && dbt docs generate && dbt docs serve --port 8081

# SQL linting
lint-sql:
	sqlfluff lint dbt/models/ --dialect postgres

lint-sql-fix:
	sqlfluff fix dbt/models/ --dialect postgres

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	rm -rf dbt/target dbt/dbt_packages dbt/logs
