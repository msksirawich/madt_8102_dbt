# Data Ingestion Framework

A simple, configuration-driven data ingestion framework for loading data from various sources to Google Cloud Storage with Hive-style partitioning.

## Features

- Configuration-driven using YAML
- Modular source and target architecture
- Multiple source support: PostgreSQL, CSV
- Execution date filtering
- Hive-style partitioning (e.g., `dt=2024-12-01`)
- Support for Parquet and JSONL formats
- BigQuery DDL execution script for creating tables

## Project Structure

```
ingestion/
├── bigquery/
│   ├── README.md                           # BigQuery DDL documentation
│   ├── orders_external_table.sql           # Orders external table DDL
│   └── users_external_table.sql            # Users external table DDL
├── config/
│   ├── pipeline_config.yaml                # PostgreSQL pipeline configuration
│   ├── csv_pipeline_config.yaml            # CSV pipeline configuration (users)
│   └── csv_orders_pipeline_config.yaml     # CSV pipeline configuration (orders)
├── data/
│   ├── users.csv                            # Sample CSV data
│   └── orders.csv                           # Sample CSV data
├── sources/
│   ├── __init__.py
│   ├── postgres_source.py                   # PostgreSQL source module
│   └── csv_source.py                        # CSV source module
├── targets/
│   ├── __init__.py
│   └── gcs_target.py                        # GCS target module with Hive partitioning
├── main.py                                   # Main ingestion script
├── run_bigquery_ddl.py                       # BigQuery DDL execution script
├── requirements.txt
└── README.md
```

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure your pipeline:

**For PostgreSQL source**, edit `config/pipeline_config.yaml`:

```yaml
database:
  host: localhost
  port: 5432
  database: your_database_name
  user: your_username
  password: your_password
  schema: public

pipeline:
  name: postgres_to_gcs
  source_type: postgres

  source:
    table: your_table_name
    date_column: created_at

  target:
    bucket: your-gcs-bucket-name
    path: raw_data/your_table_name
    partition_column: created_at
    file_format: parquet
    credentials_path: /path/to/your/service-account-key.json
```

**For CSV source**, edit `config/csv_pipeline_config.yaml`:

```yaml
pipeline:
  name: csv_to_gcs
  source_type: csv

  source:
    file_path: data/users.csv
    encoding: utf-8
    date_column: created_at

  target:
    bucket: your-gcs-bucket-name
    path: raw_data/users
    partition_column: created_at
    file_format: parquet
    credentials_path: /path/to/your/service-account-key.json
```

3. Set up Google Cloud credentials:

The framework supports multiple ways to authenticate with GCS:

**Option 1: Specify in config (Recommended)**
```yaml
target:
  credentials_path: /path/to/your/service-account-key.json
```

**Option 2: Environment variable**
```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/service-account-key.json
```

**Option 3: Application Default Credentials**
When running in GCP (Cloud Run, GCE, Cloud Functions), no credentials needed.

## Usage

### PostgreSQL to GCS

Run the ingestion script with an execution date:

```bash
python main.py --execution-date 2024-12-01
```

With custom config file:
```bash
python main.py --config config/pipeline_config.yaml --execution-date 2024-12-01
```

### CSV to GCS

Run with CSV source (using sample data):

```bash
# Ingest users data for 2024-12-01
python main.py --config config/csv_pipeline_config.yaml --execution-date 2024-12-01

# Ingest orders data for 2024-12-02
python main.py --config config/csv_orders_pipeline_config.yaml --execution-date 2024-12-02
```

## How It Works

1. **Configuration Loading**: Loads pipeline settings from YAML file
2. **Data Extraction**: Connects to source (PostgreSQL/CSV) and extracts data filtered by `execution_date`
3. **Data Transformation**: Converts date strings to proper date types
4. **Data Loading**: Writes parquet files directly to GCS with Hive-style partitioning
5. **Partitioning**: Data is written to `gs://{bucket}/{path}/dt={execution_date}/*.parquet`

## GCS Structure

The framework creates a clean Hive-partitioned structure:

```
gs://madt8102_bronze/
└── users/
    ├── dt=2024-12-01/
    │   └── 20241201_143022.parquet
    ├── dt=2024-12-02/
    │   └── 20241202_093015.parquet
    └── dt=2024-12-03/
        └── 20241203_120533.parquet
```

**No extra folders, no metadata tables** - just clean parquet files in Hive partitions!

## Example

For execution date `2024-12-01` with config:
- Bucket: `my-data-lake`
- Path: `raw_data/users`
- Partition column: `created_at`

Data will be written to:
```
gs://my-data-lake/raw_data/users/dt=2024-12-01/
```

## BigQuery Table Creation

After ingesting data to GCS, you can create BigQuery external tables to query the data:

### Quick Start

```bash
# Run all DDL files in bigquery folder
python run_bigquery_ddl.py

# With specific credentials
python run_bigquery_ddl.py --credentials /path/to/service-account.json

# Run specific files only
python run_bigquery_ddl.py --files users_external_table.sql orders_external_table.sql

# Validate without executing (dry run)
python run_bigquery_ddl.py --dry-run
```

### Adding New DDL Files

1. Create your SQL file in the `bigquery/` folder (e.g., `my_table.sql`)
2. Write your `CREATE OR REPLACE TABLE` or `CREATE OR REPLACE EXTERNAL TABLE` statement
3. Run the script to execute it on BigQuery

Example DDL file:
```sql
CREATE OR REPLACE EXTERNAL TABLE `project-id.dataset.table_name`
(
  id INT64,
  name STRING,
  created_at DATE
)
WITH PARTITION COLUMNS (
  dt DATE
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://bucket-name/path/dt=*/*'],
  hive_partition_uri_prefix = 'gs://bucket-name/path'
);
```

See `bigquery/README.md` for detailed documentation and examples.

## Extending the Framework

### Adding New Sources

Create a new source module in `sources/`:

```python
class MySource:
    def __init__(self, config):
        self.config = config

    def extract_data(self, **kwargs):
        # Return iterator of dictionaries
        yield {"column": "value"}
```

### Adding New Targets

Create a new target module in `targets/`:

```python
class MyTarget:
    def __init__(self, target_config):
        self.config = target_config

    def get_destination_config(self, partition_value):
        return {"destination": "...", "bucket_url": "..."}
```

## Notes

- Execution date must be in `YYYY-MM-DD` format
- The framework uses dlt's filesystem destination for GCS
- Default write disposition is `append`
- Supports batch processing for efficient memory usage
