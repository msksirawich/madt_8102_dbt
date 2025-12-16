# Makefile for MADT8102 DBT Pipeline
# Supports dev (DuckDB) and prod (BigQuery) environments

# Default environment and date
ENV ?= dev
DATE ?= $(shell date -v-1d +%Y-%m-%d)
START_DATE ?= $(DATE)
END_DATE ?= $(DATE)

# Paths
PROJECT_ROOT := $(shell pwd)
INGESTION_DIR := $(PROJECT_ROOT)/ingestion
CONFIG_DIR := $(INGESTION_DIR)/config
DATA_DIR := $(INGESTION_DIR)/data
PYTHON := python3
DBT := dbt

# BigQuery credentials (for prod)
BQ_CREDENTIALS ?= ~/Desktop/madt-8102-dbt-bq-key.json

# Colors for output
COLOR_RESET := \033[0m
COLOR_BLUE := \033[1;34m
COLOR_GREEN := \033[1;32m
COLOR_YELLOW := \033[1;33m

# Tables to ingest
TABLES := users applications companies education job_postings job_skills profiles skills streaming_job_activity user_skills work_history

.PHONY: help
help: ## Show this help message
	@echo "$(COLOR_BLUE)MADT8102 DBT Pipeline - Makefile Commands$(COLOR_RESET)"
	@echo ""
	@echo "$(COLOR_GREEN)Usage:$(COLOR_RESET)"
	@echo "  make [target] ENV=[dev|prod] DATE=[YYYY-MM-DD]"
	@echo ""
	@echo "$(COLOR_GREEN)Environment Variables:$(COLOR_RESET)"
	@echo "  ENV          Environment (dev or prod, default: dev)"
	@echo "  DATE         Execution date (default: yesterday)"
	@echo "  START_DATE   Start date for range (default: DATE)"
	@echo "  END_DATE     End date for range (default: DATE)"
	@echo ""
	@echo "$(COLOR_GREEN)Available targets:$(COLOR_RESET)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(COLOR_YELLOW)%-20s$(COLOR_RESET) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(COLOR_GREEN)Examples:$(COLOR_RESET)"
	@echo "  make pipeline                          # Run full pipeline in dev with yesterday's date"
	@echo "  make pipeline ENV=prod DATE=2024-12-01 # Run full pipeline in prod with specific date"
	@echo "  make ingest ENV=dev                    # Run ingestion only in dev"
	@echo "  make dbt-run ENV=prod                  # Run dbt models in prod"

.PHONY: check-env
check-env: ## Validate environment setup
	@echo "$(COLOR_BLUE)Checking environment: $(ENV)$(COLOR_RESET)"
	@echo "Execution date: $(DATE)"
	@if [ "$(ENV)" = "prod" ]; then \
		echo "Checking BigQuery credentials..."; \
		if [ ! -f "$(BQ_CREDENTIALS)" ]; then \
			echo "$(COLOR_YELLOW)Warning: BigQuery credentials not found at $(BQ_CREDENTIALS)$(COLOR_RESET)"; \
		else \
			echo "$(COLOR_GREEN)BigQuery credentials found$(COLOR_RESET)"; \
		fi; \
	fi

.PHONY: install
install: ## Install Python dependencies
	@echo "$(COLOR_BLUE)Installing Python dependencies...$(COLOR_RESET)"
	cd $(INGESTION_DIR) && $(PYTHON) -m pip install -r requirements.txt
	@echo "$(COLOR_GREEN)Dependencies installed$(COLOR_RESET)"

.PHONY: dbt-deps
dbt-deps: ## Install dbt dependencies
	@echo "$(COLOR_BLUE)Installing dbt dependencies...$(COLOR_RESET)"
	$(DBT) deps
	@echo "$(COLOR_GREEN)DBT dependencies installed$(COLOR_RESET)"

.PHONY: setup
setup: install dbt-deps ## Setup all dependencies
	@echo "$(COLOR_GREEN)Setup complete$(COLOR_RESET)"

# ============================================================================
# INGESTION TARGETS
# ============================================================================

.PHONY: ingest-table
ingest-table: ## Ingest a single table (requires TABLE=<name>)
	@if [ -z "$(TABLE)" ]; then \
		echo "$(COLOR_YELLOW)Error: TABLE variable required$(COLOR_RESET)"; \
		echo "Usage: make ingest-table TABLE=users DATE=2024-12-01"; \
		exit 1; \
	fi
	@echo "$(COLOR_BLUE)Ingesting $(TABLE) for date $(DATE) in $(ENV) environment...$(COLOR_RESET)"
	@if [ "$(ENV)" = "dev" ]; then \
		cd $(INGESTION_DIR) && $(PYTHON) main.py \
			--config config/csv_$(TABLE)_to_duckdb.yaml \
			--execution-date $(DATE); \
	else \
		echo "$(COLOR_YELLOW)Prod ingestion to GCS not implemented yet$(COLOR_RESET)"; \
		exit 1; \
	fi

.PHONY: ingest
ingest: check-env ## Ingest all tables
	@echo "$(COLOR_BLUE)Starting ingestion for all tables ($(ENV) environment, date: $(DATE))...$(COLOR_RESET)"
	@for table in $(TABLES); do \
		echo "$(COLOR_GREEN)Ingesting $$table...$(COLOR_RESET)"; \
		$(MAKE) ingest-table TABLE=$$table DATE=$(DATE) ENV=$(ENV) --no-print-directory || exit 1; \
	done
	@echo "$(COLOR_GREEN)All tables ingested successfully$(COLOR_RESET)"

.PHONY: ingest-range
ingest-range: check-env ## Ingest all tables for a date range (requires START_DATE and END_DATE)
	@echo "$(COLOR_BLUE)Starting ingestion for date range $(START_DATE) to $(END_DATE)...$(COLOR_RESET)"
	@for table in $(TABLES); do \
		echo "$(COLOR_GREEN)Ingesting $$table...$(COLOR_RESET)"; \
		cd $(INGESTION_DIR) && $(PYTHON) main.py \
			--config config/csv_$${table}_to_duckdb.yaml \
			--start-date $(START_DATE) \
			--end-date $(END_DATE); \
	done
	@echo "$(COLOR_GREEN)All tables ingested for date range$(COLOR_RESET)"

# ============================================================================
# DBT TARGETS
# ============================================================================

.PHONY: dbt-debug
dbt-debug: ## Debug dbt connection
	@echo "$(COLOR_BLUE)Debugging dbt connection ($(ENV))...$(COLOR_RESET)"
	$(DBT) debug --target $(ENV)

.PHONY: dbt-run
dbt-run: ## Run all dbt models
	@echo "$(COLOR_BLUE)Running dbt models ($(ENV) environment)...$(COLOR_RESET)"
	$(DBT) run --target $(ENV) --vars '{execution_date: $(DATE)}'
	@echo "$(COLOR_GREEN)DBT models executed successfully$(COLOR_RESET)"

.PHONY: dbt-run-bronze
dbt-run-bronze: ## Run bronze layer models only
	@echo "$(COLOR_BLUE)Running bronze layer models ($(ENV))...$(COLOR_RESET)"
	$(DBT) run --models bronze.* --target $(ENV) --vars '{execution_date: $(DATE)}'

.PHONY: dbt-run-silver
dbt-run-silver: ## Run silver layer models only
	@echo "$(COLOR_BLUE)Running silver layer models ($(ENV))...$(COLOR_RESET)"
	$(DBT) run --models silver.* --target $(ENV) --vars '{execution_date: $(DATE)}'

.PHONY: dbt-run-gold
dbt-run-gold: ## Run gold layer models only
	@echo "$(COLOR_BLUE)Running gold layer models ($(ENV))...$(COLOR_RESET)"
	$(DBT) run --models gold.* --target $(ENV) --vars '{execution_date: $(DATE)}'

.PHONY: dbt-test
dbt-test: ## Run dbt tests
	@echo "$(COLOR_BLUE)Running dbt tests ($(ENV))...$(COLOR_RESET)"
	$(DBT) test --target $(ENV)
	@echo "$(COLOR_GREEN)All tests passed$(COLOR_RESET)"

.PHONY: dbt-test-silver
dbt-test-silver: ## Run tests for silver layer only
	@echo "$(COLOR_BLUE)Running silver layer tests ($(ENV))...$(COLOR_RESET)"
	$(DBT) test --models silver.* --target $(ENV)

.PHONY: dbt-snapshot
dbt-snapshot: ## Run dbt snapshots
	@echo "$(COLOR_BLUE)Running dbt snapshots ($(ENV))...$(COLOR_RESET)"
	$(DBT) snapshot --target $(ENV)

.PHONY: dbt-docs
dbt-docs: ## Generate and serve dbt documentation
	@echo "$(COLOR_BLUE)Generating dbt documentation...$(COLOR_RESET)"
	$(DBT) docs generate
	@echo "$(COLOR_GREEN)Documentation generated. Serving at http://localhost:8080$(COLOR_RESET)"
	$(DBT) docs serve --port 8080

.PHONY: dbt-compile
dbt-compile: ## Compile dbt models
	@echo "$(COLOR_BLUE)Compiling dbt models ($(ENV))...$(COLOR_RESET)"
	$(DBT) compile --target $(ENV) --vars '{execution_date: $(DATE)}'

# ============================================================================
# BIGQUERY TARGETS (PROD ONLY)
# ============================================================================

.PHONY: bq-setup
bq-setup: ## Setup BigQuery tables from DDL files (prod only)
	@if [ "$(ENV)" != "prod" ]; then \
		echo "$(COLOR_YELLOW)BigQuery setup is only for prod environment$(COLOR_RESET)"; \
		exit 1; \
	fi
	@echo "$(COLOR_BLUE)Setting up BigQuery tables...$(COLOR_RESET)"
	cd $(INGESTION_DIR) && $(PYTHON) run_bigquery_ddl.py --credentials $(BQ_CREDENTIALS)
	@echo "$(COLOR_GREEN)BigQuery tables created$(COLOR_RESET)"

.PHONY: bq-validate
bq-validate: ## Validate BigQuery DDL files (dry-run)
	@echo "$(COLOR_BLUE)Validating BigQuery DDL files...$(COLOR_RESET)"
	cd $(INGESTION_DIR) && $(PYTHON) run_bigquery_ddl.py --dry-run --credentials $(BQ_CREDENTIALS)

# ============================================================================
# FULL PIPELINE TARGETS
# ============================================================================

.PHONY: pipeline
pipeline: check-env ingest dbt-run dbt-test ## Run complete end-to-end pipeline (ingest + dbt)
	@echo "$(COLOR_GREEN)======================================$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)Pipeline completed successfully!$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)Environment: $(ENV)$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)Date: $(DATE)$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)======================================$(COLOR_RESET)"

.PHONY: pipeline-bronze
pipeline-bronze: check-env ingest dbt-run-bronze ## Run pipeline up to bronze layer
	@echo "$(COLOR_GREEN)Bronze layer pipeline completed$(COLOR_RESET)"

.PHONY: pipeline-silver
pipeline-silver: check-env ingest dbt-run-bronze dbt-run-silver dbt-test-silver ## Run pipeline up to silver layer
	@echo "$(COLOR_GREEN)Silver layer pipeline completed$(COLOR_RESET)"

.PHONY: pipeline-no-test
pipeline-no-test: check-env ingest dbt-run ## Run pipeline without tests
	@echo "$(COLOR_GREEN)Pipeline completed (tests skipped)$(COLOR_RESET)"

.PHONY: pipeline-dev
pipeline-dev: ## Run complete dev pipeline (shortcut for ENV=dev)
	@$(MAKE) pipeline ENV=dev --no-print-directory

.PHONY: pipeline-prod
pipeline-prod: ## Run complete prod pipeline (shortcut for ENV=prod)
	@$(MAKE) pipeline ENV=prod --no-print-directory

# ============================================================================
# VALIDATION AND MONITORING
# ============================================================================

.PHONY: validate
validate: dbt-compile dbt-test ## Validate models and run tests
	@echo "$(COLOR_GREEN)Validation complete$(COLOR_RESET)"

.PHONY: check-data
check-data: ## Check if DuckDB database exists and show table counts
	@echo "$(COLOR_BLUE)Checking data in DuckDB...$(COLOR_RESET)"
	@if [ -f "$(DATA_DIR)/ingestion.duckdb" ]; then \
		echo "$(COLOR_GREEN)DuckDB database found$(COLOR_RESET)"; \
		$(PYTHON) -c "import duckdb; conn = duckdb.connect('$(DATA_DIR)/ingestion.duckdb'); \
			tables = conn.execute('SELECT table_schema, table_name, \
			(SELECT COUNT(*) FROM ' || table_schema || '.' || table_name || ') as row_count \
			FROM information_schema.tables WHERE table_schema = \"bronze\" ORDER BY table_name').fetchall(); \
			print('\nBronze Layer Tables:'); \
			print('-' * 60); \
			print(f\"{'Schema':<15} {'Table':<30} {'Rows':>10}\"); \
			print('-' * 60); \
			for row in tables: print(f\"{row[0]:<15} {row[1]:<30} {row[2]:>10,}\"); \
			print('-' * 60); \
			total = sum(row[2] for row in tables); \
			print(f\"{'TOTAL':<45} {total:>10,}\"); \
			conn.close()"; \
	else \
		echo "$(COLOR_YELLOW)DuckDB database not found. Run 'make ingest' first.$(COLOR_RESET)"; \
	fi

.PHONY: show-config
show-config: ## Show current configuration
	@echo "$(COLOR_BLUE)Current Configuration:$(COLOR_RESET)"
	@echo "  Environment:     $(ENV)"
	@echo "  Date:            $(DATE)"
	@echo "  Start Date:      $(START_DATE)"
	@echo "  End Date:        $(END_DATE)"
	@echo "  Project Root:    $(PROJECT_ROOT)"
	@echo "  Ingestion Dir:   $(INGESTION_DIR)"
	@echo "  DBT Target:      $(ENV)"
	@if [ "$(ENV)" = "prod" ]; then \
		echo "  BQ Credentials:  $(BQ_CREDENTIALS)"; \
	fi

# ============================================================================
# CLEANUP TARGETS
# ============================================================================

.PHONY: clean-dbt
clean-dbt: ## Clean dbt artifacts
	@echo "$(COLOR_BLUE)Cleaning dbt artifacts...$(COLOR_RESET)"
	rm -rf target/
	rm -rf dbt_packages/
	rm -rf logs/
	@echo "$(COLOR_GREEN)DBT artifacts cleaned$(COLOR_RESET)"

.PHONY: clean-data
clean-data: ## Clean DuckDB database (WARNING: deletes all data)
	@echo "$(COLOR_YELLOW)WARNING: This will delete the DuckDB database$(COLOR_RESET)"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		rm -f $(DATA_DIR)/ingestion.duckdb; \
		echo "$(COLOR_GREEN)DuckDB database deleted$(COLOR_RESET)"; \
	else \
		echo "Cancelled"; \
	fi

.PHONY: clean-pycache
clean-pycache: ## Clean Python cache files
	@echo "$(COLOR_BLUE)Cleaning Python cache...$(COLOR_RESET)"
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@echo "$(COLOR_GREEN)Python cache cleaned$(COLOR_RESET)"

.PHONY: clean
clean: clean-dbt clean-pycache ## Clean all generated files (except data)
	@echo "$(COLOR_GREEN)Cleanup complete$(COLOR_RESET)"

.PHONY: clean-all
clean-all: clean clean-data ## Clean everything including data
	@echo "$(COLOR_GREEN)Complete cleanup done$(COLOR_RESET)"

# ============================================================================
# DEVELOPMENT HELPERS
# ============================================================================

.PHONY: logs
logs: ## Show dbt logs
	@if [ -f "logs/dbt.log" ]; then \
		tail -n 100 logs/dbt.log; \
	else \
		echo "$(COLOR_YELLOW)No logs found$(COLOR_RESET)"; \
	fi

.PHONY: list-tables
list-tables: ## List all tables to ingest
	@echo "$(COLOR_BLUE)Tables configured for ingestion:$(COLOR_RESET)"
	@for table in $(TABLES); do echo "  - $$table"; done

.PHONY: version
version: ## Show tool versions
	@echo "$(COLOR_BLUE)Tool Versions:$(COLOR_RESET)"
	@echo "Python:  $$($(PYTHON) --version)"
	@echo "DBT:     $$($(DBT) --version | head -n 1)"
	@echo "Make:    $$(make --version | head -n 1)"

# Default target
.DEFAULT_GOAL := help
