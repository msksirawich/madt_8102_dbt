# Data Ingestion Framework

A simple, configuration-driven data ingestion framework for loading data from various sources to multiple targets including Google Cloud Storage and DuckDB.

## Features

- Configuration-driven using YAML
- Modular source and target architecture
- Multiple source support: PostgreSQL, CSV
- Multiple target support: Google Cloud Storage (GCS), DuckDB
- Execution date filtering
- Hive-style partitioning (e.g., `dt=2024-12-01`) for GCS
- Partition column support for DuckDB
- Support for Parquet and JSONL formats
- BigQuery DDL execution script for creating tables

## Project Structure

```
ingestion/
├── bigquery/
│   ├── README.md                           # BigQuery DDL documentation
│   ├── customers_external_table.sql        # Customers external table DDL
│   └── orders_external_table.sql           # Orders external table DDL
├── config/
│   ├── pipeline_config.yaml                # PostgreSQL pipeline configuration
│   ├── csv_customers_pipeline_config.yaml  # CSV pipeline configuration (customers)
│   ├── csv_orders_pipeline_config.yaml     # CSV pipeline configuration (orders)
│   └── csv_users_pipeline_config.yaml      # CSV pipeline configuration (users)
├── data/
│   ├── raw_customers.csv                   # Sample customer CSV data
│   ├── raw_orders.csv                      # Sample order CSV data
│   ├── users.csv                           # Sample user CSV data
│   └── orders.csv                          # Sample order CSV data
├── sources/
│   ├── __init__.py
│   ├── postgres_source.py                  # PostgreSQL source module
│   └── csv_source.py                       # CSV source module
├── targets/
│   ├── __init__.py
│   ├── gcs_target.py                       # GCS target module with Hive partitioning
│   └── duckdb_target.py                    # DuckDB target module
├── main.py                                 # Main ingestion script
├── run_bigquery_ddl.py                     # BigQuery DDL execution script
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

**For CSV source to GCS**, edit `config/csv_customers_pipeline_config.yaml`:

```yaml
pipeline:
  name: csv_to_gcs
  source_type: csv

  source:
    file_path: data/raw_customers.csv
    encoding: utf-8
    date_column: created_at

  target:
    type: gcs
    bucket: your-gcs-bucket-name
    path: raw_data/customers
    partition_column: created_at
    file_format: parquet
    credentials_path: /path/to/your/service-account-key.json
```

**For CSV source to DuckDB**, edit `config/csv_to_duckdb_pipeline_config.yaml`:

```yaml
pipeline:
  name: csv_to_duckdb
  source_type: csv

  source:
    file_path: data/raw_customers.csv
    encoding: utf-8
    date_column: created_at

  target:
    type: duckdb
    database_path: data/ingestion.duckdb
    table_name: customers
    partition_column: dt
    if_exists: overwrite_partition  # Options: append, replace, overwrite_partition
```

3. Set up credentials:

**For GCS target** - The framework supports multiple ways to authenticate with GCS:

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

**For DuckDB target** - No credentials needed. Just specify the database file path:

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
# Ingest customers data for 2024-12-01
python main.py --config config/csv_customers_pipeline_config.yaml --execution-date 2024-12-01

# Ingest orders data for 2024-12-02
python main.py --config config/csv_orders_pipeline_config.yaml --execution-date 2024-12-02

# Ingest users data for 2024-12-01
python main.py --config config/csv_users_pipeline_config.yaml --execution-date 2024-12-01
```

### CSV to DuckDB

Run with CSV source to DuckDB:

```bash
# Ingest customers data for 2024-12-01
python main.py --config config/csv_to_duckdb_pipeline_config.yaml --execution-date 2024-12-01

# Ingest data for multiple dates (incremental append)
python main.py --config config/csv_to_duckdb_pipeline_config.yaml --execution-date 2024-12-02
python main.py --config config/csv_to_duckdb_pipeline_config.yaml --execution-date 2024-12-03
```

### PostgreSQL to DuckDB

Run with PostgreSQL source to DuckDB:

```bash
# Ingest data for 2024-12-01
python main.py --config config/postgres_to_duckdb_pipeline_config.yaml --execution-date 2024-12-01
```

### DuckDB Write Modes

DuckDB target supports three write modes:

1. **`append`**: Adds new records to the table without removing existing data
   - Use when you want to keep all historical records
   - May create duplicates if same data is ingested multiple times

2. **`replace`**: Drops and recreates the entire table
   - Use when you want to completely refresh all data
   - Removes all existing partitions

3. **`overwrite_partition`** (Recommended): Deletes only the specific partition being written
   - Deletes existing records where `dt = execution_date`
   - Inserts new records for that partition
   - Keeps all other partitions intact
   - Ideal for incremental daily loads

**Example behavior with `overwrite_partition`:**
```bash
# Day 1: Ingest data for 2024-12-01 (creates partition)
python main.py --config config/csv_to_duckdb_pipeline_config.yaml --execution-date 2024-12-01
# Table has: 100 records for dt=2024-12-01

# Day 2: Ingest data for 2024-12-02 (creates new partition)
python main.py --config config/csv_to_duckdb_pipeline_config.yaml --execution-date 2024-12-02
# Table has: 100 records for dt=2024-12-01, 150 records for dt=2024-12-02

# Day 3: Re-ingest corrected data for 2024-12-01 (overwrites partition)
python main.py --config config/csv_to_duckdb_pipeline_config.yaml --execution-date 2024-12-01
# Table has: 120 records for dt=2024-12-01, 150 records for dt=2024-12-02
# (Only the 2024-12-01 partition was replaced, 2024-12-02 remains unchanged)
```

### Querying DuckDB Data

After ingestion, you can query your DuckDB database using SQL:

```python
import duckdb

# Connect to database
con = duckdb.connect('data/ingestion.duckdb')

# Query data
result = con.execute('SELECT * FROM customers WHERE dt = ?', ['2024-12-01']).fetchdf()
print(result)

# Aggregate queries
stats = con.execute('''
    SELECT dt, COUNT(*) as record_count
    FROM customers
    GROUP BY dt
    ORDER BY dt
''').fetchdf()
print(stats)

con.close()
```

## How It Works

1. **Configuration Loading**: Loads pipeline settings from YAML file
2. **Data Extraction**: Connects to source (PostgreSQL/CSV) and extracts data filtered by `execution_date`
3. **Data Transformation**: Converts date strings to proper date types
4. **Data Loading**:
   - **GCS**: Writes parquet files directly to GCS with Hive-style partitioning
   - **DuckDB**: Inserts data into DuckDB table with partition column
5. **Partitioning**:
   - **GCS**: Data is written to `gs://{bucket}/{path}/dt={execution_date}/*.parquet`
   - **DuckDB**: Partition column `dt` is added to each record

## GCS Structure

The framework creates a clean Hive-partitioned structure:

```
gs://madt8102_bronze/
├── customers/
│   ├── dt=2024-12-01/
│   │   └── 20241201_143022.parquet
│   ├── dt=2024-12-02/
│   │   └── 20241202_093015.parquet
│   └── dt=2024-12-03/
│       └── 20241203_120533.parquet
└── orders/
    ├── dt=2024-12-01/
    │   └── 20241201_143025.parquet
    └── dt=2024-12-02/
        └── 20241202_093018.parquet
```

**No extra folders, no metadata tables** - just clean parquet files in Hive partitions!

## DuckDB Structure

For DuckDB targets, data is stored in a single database file with tables:

```
data/ingestion.duckdb
├── customers (table)
│   ├── customer_id
│   ├── email
│   ├── first_name
│   ├── last_name
│   ├── created_at
│   ├── updated_at
│   └── dt (partition column)
└── orders (table)
    ├── order_id
    ├── customer_id
    ├── order_date
    ├── amount
    ├── status
    ├── created_at
    ├── updated_at
    └── dt (partition column)
```

The `dt` column allows for efficient filtering by date:
```sql
SELECT * FROM customers WHERE dt = '2024-12-01'
```

## Example

For execution date `2024-12-01` with config:
- Bucket: `madt8102_bronze`
- Path: `customers`
- Partition column: `created_at`

Data will be written to:
```
gs://madt8102_bronze/customers/dt=2024-12-01/
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
python run_bigquery_ddl.py --files customers_external_table.sql orders_external_table.sql

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

## DuckDB Advantages

DuckDB is a great alternative to GCS for:

- **Local Development**: Test your pipelines locally without cloud dependencies
- **Small to Medium Datasets**: Efficient for datasets that fit on a single machine
- **Fast Analytics**: DuckDB is optimized for analytical queries
- **SQL Interface**: Query data directly using standard SQL
- **No Cloud Costs**: Store and query data locally without cloud storage fees
- **Portability**: Single file database that can be easily shared or backed up

## Notes

- Execution date must be in `YYYY-MM-DD` format
- The framework uses dlt's filesystem destination for GCS
- For DuckDB, **`overwrite_partition`** is the recommended write mode for daily incremental loads
- Supports batch processing for efficient memory usage
- DuckDB database files are created automatically if they don't exist
- For GCS, ensure proper authentication is configured
- For DuckDB, ensure the target directory exists or has write permissions

## Available Configuration Files

### GCS Targets
- `csv_customers_pipeline_config.yaml`: Ingests customer data from CSV to GCS
- `csv_orders_pipeline_config.yaml`: Ingests order data from CSV to GCS
- `csv_users_pipeline_config.yaml`: Ingests user data from CSV to GCS
- `pipeline_config.yaml`: Template for PostgreSQL to GCS

### DuckDB Targets
- `csv_to_duckdb_pipeline_config.yaml`: Ingests CSV data to DuckDB
- `postgres_to_duckdb_pipeline_config.yaml`: Template for PostgreSQL to DuckDB

## Data Files

Sample CSV files are provided in the `data/` folder:
- `raw_customers.csv`: Customer records with customer_id, email, first_name, last_name, created_at, updated_at
- `raw_orders.csv`: Order records with order_id, customer_id, order_date, amount, status, created_at, updated_at
- `users.csv`: User records (alternative dataset)
- `orders.csv`: Order records (alternative dataset)
