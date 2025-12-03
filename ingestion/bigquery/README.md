# BigQuery DDL Files

This folder contains SQL DDL (Data Definition Language) files for creating tables in BigQuery.

## Structure

Place your `CREATE OR REPLACE TABLE` or `CREATE OR REPLACE EXTERNAL TABLE` SQL statements in this folder as `.sql` files.

## Running DDL Scripts

Use the `run_bigquery_ddl.py` script to execute these DDL files on BigQuery.

### Prerequisites

1. Install required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Set up Google Cloud credentials (choose one):
   - Set `GOOGLE_APPLICATION_CREDENTIALS` environment variable:
     ```bash
     export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
     ```
   - Or use `gcloud auth application-default login` for default credentials
   - Or pass credentials directly using `--credentials` flag

### Usage Examples

**Run all SQL files:**
```bash
python run_bigquery_ddl.py
```

**Run with specific credentials:**
```bash
python run_bigquery_ddl.py --credentials /path/to/service-account.json
```

**Run specific files:**
```bash
python run_bigquery_ddl.py --files customers_external_table.sql orders_external_table.sql
```

**Run files matching a pattern:**
```bash
python run_bigquery_ddl.py --pattern "*_external_table.sql"
```

**Dry run (validate without executing):**
```bash
python run_bigquery_ddl.py --dry-run
```

**Continue on errors:**
```bash
python run_bigquery_ddl.py --continue-on-error
```

### Command Line Options

- `--sql-folder`: Path to folder with SQL files (default: `bigquery`)
- `--credentials`: Path to service account JSON credentials
- `--pattern`: Pattern to filter SQL files (e.g., `"customers*.sql"`)
- `--files`: Specific SQL files to execute (space-separated)
- `--dry-run`: Validate queries without executing
- `--continue-on-error`: Continue if a file fails

## SQL File Guidelines

1. **File naming**: Use descriptive names ending in `.sql`
   - Examples: `customers_external_table.sql`, `orders_external_table.sql`

2. **SQL format**: Use `CREATE OR REPLACE` statements
   ```sql
   CREATE OR REPLACE EXTERNAL TABLE `project-id.dataset.table_name`
   (
     column1 TYPE,
     column2 TYPE
   )
   OPTIONS (...);
   ```

3. **Comments**: Use `--` for SQL comments. The script will strip these before execution.

4. **Project and dataset**: Update placeholders like `your-project-id` and `your_dataset` with actual values.

## Current Files

- `customers_external_table.sql`: External table for customers data from GCS (with Hive partitioning)
- `orders_external_table.sql`: External table for orders data from GCS (with Hive partitioning)

## Troubleshooting

**Authentication errors:**
- Ensure `GOOGLE_APPLICATION_CREDENTIALS` is set correctly
- Verify service account has BigQuery permissions (BigQuery Admin or Data Editor)

**Syntax errors:**
- Run with `--dry-run` first to validate SQL
- Check that project-id and dataset names are correct
- Ensure GCS URIs are accessible

**Permission errors:**
- Service account needs `BigQuery Data Editor` or `BigQuery Admin` role
- For external tables, needs access to GCS buckets

## External Table Structure

The DDL files create external tables that:
- Point to Hive-partitioned data in Google Cloud Storage
- Use Parquet format for efficient querying
- Include partition columns (dt DATE) for date-based filtering
- Follow the pattern: `gs://bucket-name/table-name/dt=YYYY-MM-DD/*.parquet`

Example structure:
```sql
CREATE OR REPLACE EXTERNAL TABLE `madt-8102-479812.bronze.customers`
(
  customer_id INT64,
  email STRING,
  first_name STRING,
  last_name STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
WITH PARTITION COLUMNS (
  dt DATE
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://madt8102_bronze/customers/dt=*/*'],
  hive_partition_uri_prefix = 'gs://madt8102_bronze/customers'
);
```

## Integration with dbt

These external tables serve as the source layer for the dbt project:
1. Data is ingested to GCS using the ingestion framework
2. BigQuery external tables are created using these DDL scripts
3. dbt models reference these tables as sources (defined in `models/bronze/sources/sources.yml`)
4. dbt transforms the data through Bronze → Silver → Gold layers
