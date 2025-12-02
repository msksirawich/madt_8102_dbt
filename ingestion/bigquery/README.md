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
python run_bigquery_ddl.py --files users_external_table.sql orders_external_table.sql
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
- `--pattern`: Pattern to filter SQL files (e.g., `"users*.sql"`)
- `--files`: Specific SQL files to execute (space-separated)
- `--dry-run`: Validate queries without executing
- `--continue-on-error`: Continue if a file fails

## SQL File Guidelines

1. **File naming**: Use descriptive names ending in `.sql`
   - Examples: `users_external_table.sql`, `orders_table.sql`

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

- `users_external_table.sql`: External table for users data from GCS
- `orders_external_table.sql`: External table for orders data from GCS

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
