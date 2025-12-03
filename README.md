# MADT8102 DBT Project

A data pipeline project implementing the Medallion Architecture (Bronze → Silver → Gold) using dbt for data transformation and analytics.

## Project Overview

This project demonstrates a complete data engineering pipeline:
- **Data Ingestion**: Configuration-driven framework for loading data from CSV/PostgreSQL to Google Cloud Storage
- **Data Transformation**: dbt models implementing medallion architecture
- **Data Quality**: Built-in tests and data validation
- **Analytics**: Business metrics and customer segmentation

## Architecture

### Medallion Architecture Layers

**Bronze Layer** (Raw Data)
- Materialized as views
- 1:1 representation of source data
- No transformations applied
- Tables: `customers`, `orders`

**Silver Layer** (Cleaned Data)
- Materialized as tables
- Data cleaning and standardization
- Quality filters applied
- Tables: `silver_customers`, `silver_orders`

**Gold Layer** (Business Metrics)
- Materialized as tables
- Aggregated business metrics
- Ready for analytics and BI tools
- Tables: `gold_customer_metrics`, `gold_daily_sales`

## Data Models

### Customers
- **Bronze**: Raw customer data from source
- **Silver**: Cleaned with email standardization, full name generation
- **Gold**: Customer metrics including lifetime value, segmentation, and status

### Orders
- **Bronze**: Raw order data from source
- **Silver**: Cleaned with date parsing and status validation
- **Gold**: Daily sales metrics and aggregations

## Project Structure

```
madt_8102_dbt/
├── ingestion/              # Data ingestion framework (CSV/PostgreSQL → GCS)
│   ├── config/             # Pipeline configurations
│   ├── sources/            # Source connectors (CSV, PostgreSQL)
│   ├── targets/            # Target connectors (GCS)
│   ├── bigquery/           # BigQuery DDL files
│   └── data/               # Sample CSV data
├── models/                 # dbt transformation models
│   ├── bronze/             # Raw data models
│   ├── silver/             # Cleaned data models
│   └── gold/               # Business metric models
├── seeds/                  # Static reference data
├── snapshots/              # SCD Type 2 snapshots
├── tests/                  # Custom data tests
└── macros/                 # Reusable dbt macros
```

## Setup

### Prerequisites
- Python 3.8+
- dbt-core
- dbt-bigquery (for prod) or dbt-duckdb (for dev)
- Google Cloud credentials (for BigQuery)

### Installation

1. Install dbt:
```bash
pip install dbt-bigquery  # For production
# OR
pip install dbt-duckdb    # For development
```

2. Install ingestion dependencies:
```bash
cd ingestion
pip install -r requirements.txt
```

3. Configure profiles:
- Edit `profiles.yml` with your BigQuery project details
- For dev: Uses DuckDB (local)
- For prod: Uses BigQuery (cloud)

## Usage

### Running the Pipeline

**1. Data Ingestion** (CSV → GCS)
```bash
cd ingestion
python main.py --config config/csv_customers_pipeline_config.yaml --execution-date 2024-12-01
python main.py --config config/csv_orders_pipeline_config.yaml --execution-date 2024-12-01
```

**2. Create BigQuery Tables**
```bash
cd ingestion
python run_bigquery_ddl.py
```

**3. Run dbt Transformations**
```bash
# Run all models
dbt run

# Run specific layer
dbt run --models bronze.*
dbt run --models silver.*
dbt run --models gold.*

# Run with prod profile
dbt run --target prod
```

**4. Run Tests**
```bash
dbt test
```

**5. Generate Documentation**
```bash
dbt docs generate
dbt docs serve
```

## Key Features

- **Execution Date Support**: Use `--vars '{"execution_date": "2024-12-01"}'` for incremental loads
- **Data Quality Tests**: Built-in tests for uniqueness, nullability, and accepted values
- **Snapshots**: SCD Type 2 tracking for customer changes
- **Modular Design**: Easy to add new sources, models, and transformations
- **Multi-target Support**: Switch between DuckDB (dev) and BigQuery (prod)

## Configuration

### dbt Variables
Set in `dbt_project.yml`:
- `execution_date`: Defaults to yesterday's date
- Can be overridden via command line: `--vars '{"execution_date": "2024-12-01"}'`

### Data Sources
Configure in `models/bronze/sources/sources.yml`:
- BigQuery external tables pointing to GCS
- Schema and column definitions
- Data quality tests

## Resources

- [dbt Documentation](https://docs.getdbt.com/docs/introduction)
- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [dbt Best Practices](https://docs.getdbt.com/guides/best-practices)
- [Ingestion Framework](./ingestion/README.md)

## Development

### Adding New Models
1. Create SQL file in appropriate layer folder (`bronze/`, `silver/`, `gold/`)
2. Define source in `sources.yml` (for bronze) or reference upstream models
3. Add tests in schema.yml
4. Run and test: `dbt run --models your_model && dbt test --models your_model`

### Adding New Data Sources
See [Ingestion README](./ingestion/README.md) for details on adding new source connectors.
