.PHONY: help install lint format test test-unit test-integration \
        run-bronze run-silver run-gold run-quality run-maintenance \
        up down status init-data build deploy \
        example-read-postgres example-write-iceberg example-read-iceberg example-consume-kafka

# ── Python env ────────────────────────────────────────────────────────────────
PYTHON   := python
PIP      := $(PYTHON) -m pip
PYTEST   := $(PYTHON) -m pytest
RUFF     := $(PYTHON) -m ruff

help:
	@echo ""
	@echo "╔══════════════════════════════════════════════════════╗"
	@echo "║   Big Data Platform for Banking — Root Makefile      ║"
	@echo "╚══════════════════════════════════════════════════════╝"
	@echo ""
	@echo "Dev"
	@echo "  make install           Install all dependencies (editable)"
	@echo "  make lint              Run ruff linter"
	@echo "  make format            Run ruff formatter"
	@echo "  make test              Run all tests"
	@echo "  make test-unit         Run unit tests only"
	@echo "  make test-integration  Run integration tests only"
	@echo ""
	@echo "Pipeline"
	@echo "  make init-schemas      Bootstrap Iceberg namespaces & tables via Spark SQL"
	@echo "  make run-bronze        Run bronze ingestion pipeline"
	@echo "  make run-silver        Run silver transform pipeline"
	@echo "  make run-gold          Run gold aggregation pipeline"
	@echo "  make run-quality       Run data quality checks"
	@echo "  make run-maintenance   Run Iceberg maintenance"
	@echo ""
	@echo "Examples"
	@echo "  make example-read-postgres"
	@echo "  make example-write-iceberg"
	@echo "  make example-read-iceberg"
	@echo "  make example-consume-kafka"
	@echo ""
	@echo "Infra (delegates to infra/Makefile)"
	@echo "  make up                Create k3d cluster + deploy dev stack"
	@echo "  make down              Delete k3d cluster"
	@echo "  make status            kubectl get pods -A"
	@echo "  make init-data         Run bootstrap init scripts"
	@echo "  make build             Build all Docker images"
	@echo "  make deploy            Apply Kustomize dev overlay"
	@echo ""

# ── Dev ───────────────────────────────────────────────────────────────────────
install:
	$(PIP) install -e ".[dev]"

lint:
	$(RUFF) check pipeline/ tests/

format:
	$(RUFF) format pipeline/ tests/

test:
	$(PYTEST)

test-unit:
	$(PYTEST) tests/unit/

test-integration:
	$(PYTEST) tests/integration/

# ── Pipeline ──────────────────────────────────────────────────────────────────
init-schemas:
	$(PYTHON) -m pipeline.schemas.init_schemas --layer all

run-bronze:
	$(PYTHON) -m pipeline.jobs.bronze.ingest_postgres

run-silver:
	$(PYTHON) -m pipeline.jobs.silver.demo

run-gold:
	$(PYTHON) -m pipeline.jobs.gold.demo

run-quality:
	$(PYTHON) -m pipeline.jobs.quality.data_quality

run-maintenance:
	$(PYTHON) -m pipeline.jobs.maintenance.iceberg_maintenance

# ── Examples ──────────────────────────────────────────────────────────────────
example-read-postgres:
	$(PYTHON) scripts/example/spark_read_postgres.py

example-write-iceberg:
	$(PYTHON) scripts/example/spark_write_iceberg.py

example-read-iceberg:
	$(PYTHON) scripts/example/spark_read_iceberg.py

example-consume-kafka:
	$(PYTHON) scripts/example/spark_consume_kafka.py

# ── Infra (delegate) ─────────────────────────────────────────────────────────
up:
	$(MAKE) -C infra up

down:
	$(MAKE) -C infra down

status:
	$(MAKE) -C infra status

init-data:
	$(MAKE) -C infra init-data

build:
	$(MAKE) -C infra build-all

deploy:
	$(MAKE) -C infra deploy-lakehouse
