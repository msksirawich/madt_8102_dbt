# Makefile Quick Reference Guide

This guide provides quick instructions for using the Makefile to run your end-to-end data pipeline.

## Quick Start

### 1. First Time Setup
```bash
# Install all dependencies
make setup

# Check your environment configuration
make show-config
```

### 2. Run Full Pipeline

**Development (DuckDB):**
```bash
# Run complete pipeline for yesterday's date
make pipeline

# Run for specific date
make pipeline DATE=2024-12-01

# Shortcut command
make pipeline-dev DATE=2024-12-01
```

**Production (BigQuery):**
```bash
# First time: Setup BigQuery tables
make bq-setup ENV=prod

# Run complete pipeline
make pipeline ENV=prod DATE=2024-12-01

# Shortcut command
make pipeline-prod DATE=2024-12-01
```

## Common Commands

### Show All Available Commands
```bash
make help
```

### Ingestion Only
```bash
# Ingest all tables for a specific date
make ingest DATE=2024-12-01

# Ingest for date range
make ingest-range START_DATE=2024-12-01 END_DATE=2024-12-05

# Ingest single table
make ingest-table TABLE=users DATE=2024-12-01
```

### DBT Transformations Only
```bash
# Run all dbt models
make dbt-run

# Run specific layers
make dbt-run-bronze
make dbt-run-silver
make dbt-run-gold

# Run with specific environment
make dbt-run ENV=prod DATE=2024-12-01
```

### Testing
```bash
# Run all tests
make dbt-test

# Test specific layer
make dbt-test-silver

# Validate models (compile + test)
make validate
```

### Data Validation
```bash
# Check data in DuckDB
make check-data

# Show current configuration
make show-config
```

### Documentation
```bash
# Generate and serve documentation
make dbt-docs
# Open http://localhost:8080 in browser
```

### Cleanup
```bash
# Clean dbt artifacts only
make clean-dbt

# Clean everything except data
make clean

# Clean everything including data (WARNING: deletes DuckDB)
make clean-all
```

## Environment Variables

You can customize the pipeline execution with these variables:

| Variable | Description | Default | Example |
|----------|-------------|---------|---------|
| `ENV` | Environment (dev/prod) | `dev` | `ENV=prod` |
| `DATE` | Execution date | Yesterday | `DATE=2024-12-01` |
| `START_DATE` | Start date for range | `DATE` | `START_DATE=2024-12-01` |
| `END_DATE` | End date for range | `DATE` | `END_DATE=2024-12-05` |
| `BQ_CREDENTIALS` | BigQuery credentials path | `~/Desktop/madt-8102-dbt-bq-key.json` | `BQ_CREDENTIALS=/path/to/key.json` |

## Pipeline Workflows

### Development Workflow
```bash
# 1. Ingest data to DuckDB
make ingest DATE=2024-12-01

# 2. Run transformations
make dbt-run

# 3. Run tests
make dbt-test

# 4. Check data quality
make check-data

# Or run everything at once:
make pipeline DATE=2024-12-01
```

### Production Workflow
```bash
# 1. First time only: Setup BigQuery
make bq-setup ENV=prod

# 2. Run full pipeline (note: GCS ingestion not yet implemented)
make dbt-run ENV=prod DATE=2024-12-01
make dbt-test ENV=prod
```

### Layer-by-Layer Execution
```bash
# Bronze layer only
make pipeline-bronze DATE=2024-12-01

# Up to silver layer (includes tests)
make pipeline-silver DATE=2024-12-01

# Full pipeline
make pipeline DATE=2024-12-01
```

## Tips and Tricks

### Running Multiple Dates
```bash
# Option 1: Use date range
make ingest-range START_DATE=2024-12-01 END_DATE=2024-12-05

# Option 2: Loop through dates
for date in 2024-12-{01..05}; do
  make pipeline DATE=$date
done
```

### Debugging
```bash
# Check dbt connection
make dbt-debug

# Compile models to see generated SQL
make dbt-compile

# View recent logs
make logs
```

### Development Cycle
```bash
# 1. Make changes to models
# 2. Compile to check syntax
make dbt-compile

# 3. Run specific model
dbt run --models silver.mst_users --target dev

# 4. Test
make dbt-test-silver

# 5. Check results
make check-data
```

## Tables Reference

The following tables are ingested automatically:

1. `users` - User accounts
2. `profiles` - User profiles
3. `applications` - Job applications
4. `companies` - Company information
5. `education` - Education history
6. `job_postings` - Job listings
7. `job_skills` - Skills required for jobs
8. `skills` - Skills master data
9. `streaming_job_activity` - Streaming events
10. `user_skills` - User skill associations
11. `work_history` - Employment history

## Troubleshooting

### DuckDB database not found
```bash
# Solution: Run ingestion first
make ingest DATE=2024-12-01
```

### BigQuery authentication error
```bash
# Solution: Check credentials path
make show-config

# Update path if needed
make pipeline ENV=prod BQ_CREDENTIALS=/correct/path/to/key.json
```

### DBT compilation errors
```bash
# Solution: Debug connection
make dbt-debug

# Check profiles
cat profiles.yml
```

### Tests failing
```bash
# Run tests for specific layer
make dbt-test-silver

# Check data
make check-data
```

## Project Structure

```
madt_8102_dbt/
├── Makefile                    # Pipeline automation
├── dbt_project.yml            # DBT project config
├── profiles.yml               # Environment configs
├── models/
│   ├── bronze/                # Raw data views
│   ├── silver/                # Cleaned data tables
│   └── gold/                  # Business metrics
├── ingestion/
│   ├── main.py               # Ingestion script
│   ├── config/               # Pipeline configs
│   ├── sources/              # Data sources
│   ├── targets/              # Data targets
│   └── data/                 # DuckDB + CSV files
└── tests/                     # DBT tests
```

## Advanced Usage

### Custom Configuration
```bash
# Override multiple variables
make pipeline \
  ENV=prod \
  DATE=2024-12-01 \
  BQ_CREDENTIALS=/custom/path/key.json
```

### Selective Table Ingestion
```bash
# Ingest specific tables only
for table in users companies job_postings; do
  make ingest-table TABLE=$table DATE=2024-12-01
done
```

### Continuous Integration
```bash
# Full validation pipeline
make setup
make pipeline DATE=2024-12-01
make validate
```

## Next Steps

1. Review your configuration: `make show-config`
2. Run a test pipeline: `make pipeline-dev DATE=2024-12-01`
3. Validate results: `make check-data`
4. Generate documentation: `make dbt-docs`
5. Deploy to production: `make pipeline-prod DATE=2024-12-01`

For more help: `make help`
