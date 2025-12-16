# Full Load Mode Design

## Overview
Design for a full load mode that ingests all data from source in a single execution, automatically partitions by date, and writes to GCS organized by partitions.

## Current Behavior
- **Incremental Load**: Filters data by `--start-date` and `--end-date` parameters
- **Date-based extraction**: Only extracts records within the specified date range
- **Partition writing**: Groups by `dt` column and writes each partition separately

## Proposed Full Load Mode

### Use Cases
1. **Initial data load**: Load all historical data for the first time
2. **Backfill**: Reload all data without specifying date ranges
3. **Data refresh**: Complete reload of the entire dataset
4. **Migration**: Moving data from one environment to another

---

## Design Option 1: CLI Flag Approach (Recommended)

### Command Line Interface
```bash
# Full load - load all data
python main.py --config config/csv_users_pipeline_config.yaml --full-load

# Incremental load (existing behavior)
python main.py --config config/csv_users_pipeline_config.yaml --start-date 2024-12-01 --end-date 2024-12-07
```

### Behavior
1. **When `--full-load` is specified**:
   - Skip date filtering in source extraction
   - Read ALL records from CSV file
   - Add `dt` column based on the date column in each record
   - Group records by `dt` value
   - Write all partitions in a single execution

2. **Mutually exclusive with date parameters**:
   - Cannot use `--full-load` with `--start-date` or `--end-date`
   - Validation error if both are specified

### Processing Flow
```
1. Parse arguments
   └─> If --full-load: skip date parameters
   └─> If dates provided: use incremental mode

2. Extract data
   └─> Full load: extract ALL records
   └─> Incremental: extract records in date range

3. Transform data
   └─> Parse timestamp columns
   └─> Add 'dt' column from date_column
   └─> Add '_ingestion_time' column

4. Partition data
   └─> Group DataFrame by 'dt' column
   └─> Result: Dictionary of {date: dataframe}

5. Write to GCS
   └─> For each partition:
       ├─> Construct path: gs://bucket/table/dt=YYYY-MM-DD/
       ├─> Generate filename with timestamp
       └─> Write parquet file

6. Summary
   └─> Print statistics:
       ├─> Total records processed
       ├─> Number of partitions created
       ├─> Date range covered
       └─> Size of data written
```

### Code Changes Required

#### 1. `main.py` - Argument Parser
```python
parser.add_argument(
    '--full-load',
    action='store_true',
    default=False,
    help='Load all data without date filtering. Cannot be used with --start-date or --end-date.'
)

# Validation
if args.full_load and (args.start_date or args.end_date):
    parser.error("--full-load cannot be used with --start-date or --end-date")

if not args.full_load and not args.start_date and not args.execution_date:
    parser.error("Either --full-load or date parameters must be provided")
```

#### 2. `sources/csv_source.py` - Extract Method
```python
def extract_data(
    self,
    date_column: str,
    start_date: str = None,
    end_date: str = None,
    full_load: bool = False,
    batch_size: int = 1000
) -> Iterator[Dict[str, Any]]:
    """Extract data from CSV file.

    Args:
        date_column: Column name to filter by date
        start_date: Start date (required if not full_load)
        end_date: End date (required if not full_load)
        full_load: If True, extract all data without filtering
        batch_size: Number of rows to yield per batch
    """
    with open(file_path, 'r', encoding=self.encoding) as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            if full_load:
                # No filtering - yield all records
                yield row
            else:
                # Filter by date range (existing logic)
                if date_column in row:
                    row_date = row[date_column].split()[0]
                    if start_date <= row_date <= end_date:
                        yield row
```

#### 3. `sources/postgres_source.py` - Extract Method
```python
def extract_data(
    self,
    table: str,
    date_column: str,
    start_date: str = None,
    end_date: str = None,
    full_load: bool = False,
    schema: str = 'public',
    batch_size: int = 1000
) -> Iterator[Dict[str, Any]]:
    """Extract data from PostgreSQL table.

    Args:
        table: Table name
        date_column: Column name to filter by date
        start_date: Start date (required if not full_load)
        end_date: End date (required if not full_load)
        full_load: If True, extract all data without filtering
        schema: Schema name
        batch_size: Number of rows to fetch per batch
    """
    if full_load:
        # Query all data
        query = f"SELECT * FROM {schema}.{table}"
    else:
        # Query with date filter (existing logic)
        query = f"""
            SELECT * FROM {schema}.{table}
            WHERE {date_column}::date BETWEEN %s AND %s
        """
    # Execute and yield...
```

#### 4. `main.py` - Run Ingestion
```python
def run_ingestion(config: dict, start_date: str = None, end_date: str = None, full_load: bool = False):
    """Run data ingestion pipeline.

    Args:
        config: Configuration dictionary
        start_date: Start date (ignored if full_load=True)
        end_date: End date (ignored if full_load=True)
        full_load: If True, load all data without date filtering
    """
    if full_load:
        print("Starting FULL LOAD ingestion - loading all data...")
    else:
        print(f"Starting INCREMENTAL ingestion for date range: {start_date} to {end_date}")

    # Extract data
    with source:
        if source_type == 'csv':
            data = source.extract_data(
                date_column=source_config['date_column'],
                start_date=start_date,
                end_date=end_date,
                full_load=full_load
            )
        # ... rest of extraction logic

    # The rest stays the same - grouping by 'dt' and writing partitions
```

### Makefile Integration
```makefile
.PHONY: ingest-full
ingest-full: check-env ## Full load - ingest all data for all tables
	@echo "$(COLOR_BLUE)Starting FULL LOAD ingestion for all tables ($(ENV) environment)...$(COLOR_RESET)"
	@for table in $(TABLES); do \
		echo "$(COLOR_GREEN)Full loading $$table...$(COLOR_RESET)"; \
		if [ "$(ENV)" = "dev" ]; then \
			cd $(INGESTION_DIR) && $(PYTHON) main.py \
				--config config/csv_$${table}_to_duckdb.yaml \
				--full-load; \
		else \
			cd $(INGESTION_DIR) && $(PYTHON) main.py \
				--config config/csv_$${table}_pipeline_config.yaml \
				--full-load; \
		fi; \
	done
	@echo "$(COLOR_GREEN)Full load completed for all tables$(COLOR_RESET)"

.PHONY: ingest-full-table
ingest-full-table: ## Full load a single table (requires TABLE=<name>)
	@if [ -z "$(TABLE)" ]; then \
		echo "$(COLOR_YELLOW)Error: TABLE variable required$(COLOR_RESET)"; \
		exit 1; \
	fi
	@echo "$(COLOR_BLUE)Full loading $(TABLE) in $(ENV) environment...$(COLOR_RESET)"
	@if [ "$(ENV)" = "dev" ]; then \
		cd $(INGESTION_DIR) && $(PYTHON) main.py \
			--config config/csv_$(TABLE)_to_duckdb.yaml \
			--full-load; \
	else \
		cd $(INGESTION_DIR) && $(PYTHON) main.py \
			--config config/csv_$(TABLE)_pipeline_config.yaml \
			--full-load; \
	fi
```

---

## Design Option 2: Configuration-based Approach

### Config File
Add a `load_mode` parameter to pipeline config:

```yaml
pipeline:
  name: csv_to_gcs
  source_type: csv
  load_mode: full  # Options: full, incremental (default)

  source:
    file_path: data/profiles.csv
    encoding: utf-8
    date_column: created_at

  target:
    bucket: madt8102_bronze
    path: profiles
    partition_column: dt
```

### Pros & Cons
**Pros:**
- Configuration is self-documenting
- Can have different configs for full vs incremental

**Cons:**
- Less flexible - need separate config files
- More complex to maintain

---

## Comparison

| Feature | Option 1: CLI Flag | Option 2: Config-based |
|---------|-------------------|------------------------|
| Flexibility | ✅ High | ❌ Low |
| Ease of use | ✅ Simple command | ❌ Need multiple configs |
| Maintainability | ✅ Single config | ❌ Duplicate configs |
| Clear intent | ✅ Explicit in command | ⚠️ Hidden in config |
| Backward compatibility | ✅ Yes | ✅ Yes |

---

## Recommended Approach

**Option 1: CLI Flag** is recommended because:
1. More flexible - same config works for both modes
2. Clearer user intent when running commands
3. Easier to maintain - no duplicate configs
4. Better for automation and scripting

---

## Expected Output

### Full Load Execution
```bash
$ make ingest-full-table TABLE=users ENV=prod

Starting FULL LOAD ingestion - loading all data...
Source type: CSV
Target type: GCS
Target base path: gs://madt8102_bronze/users
Extracting data from: data/users.csv
✓ Added _ingestion_time column: 2024-12-16 10:30:00

Found 7 unique dates to partition: ['2024-06-01', '2024-07-01', '2024-08-01', '2024-09-01', '2024-10-01', '2024-11-01', '2024-12-01']

Writing 150 records to gs://madt8102_bronze/users/dt=2024-06-01/20241216_103000.parquet...
✓ Successfully wrote 150 records to partition dt=2024-06-01

Writing 200 records to gs://madt8102_bronze/users/dt=2024-07-01/20241216_103001.parquet...
✓ Successfully wrote 200 records to partition dt=2024-07-01

... (continues for all partitions)

✓ Total 1500 records written across 7 partitions
  Total memory size: 234.56 KB
  Date range: 2024-06-01 to 2024-12-01

Ingestion completed successfully!
```

---

## Performance Considerations

### Memory Management
- **Full load loads ALL data into memory at once**
- For large files (>1GB), consider:
  1. **Chunked processing**: Process data in chunks
  2. **Streaming partitioning**: Write partitions as soon as we have enough data
  3. **Memory limits**: Set max records in memory before flush

### Optimization Strategy
```python
# Option A: Load all, then partition (simpler, works for small-medium datasets)
data_list = list(data)  # Load all
df = pd.DataFrame(data_list)
# Group and write

# Option B: Stream and partition (better for large datasets)
partition_buffers = {}  # {date: [records]}
for record in data:
    dt = extract_date(record)
    partition_buffers[dt].append(record)

    # Flush when buffer is large
    if len(partition_buffers[dt]) >= BUFFER_SIZE:
        write_partition(dt, partition_buffers[dt])
        partition_buffers[dt] = []
```

**Recommendation**: Start with Option A (simpler), add Option B if needed for very large files.

---

## Testing Plan

### Test Cases
1. **Full load with small dataset**: Verify all data loaded and partitioned correctly
2. **Full load with multiple partitions**: Verify each partition created
3. **Full load + incremental load**: Ensure no conflicts
4. **Error handling**: Invalid config, missing files, etc.
5. **Validation**: Cannot use --full-load with date parameters

### Test Commands
```bash
# Test 1: Full load single table
make ingest-full-table TABLE=users ENV=prod

# Test 2: Full load all tables
make ingest-full ENV=prod

# Test 3: Verify partitions created in GCS
gsutil ls gs://madt8102_bronze/users/

# Test 4: Query in BigQuery
bq query "SELECT dt, COUNT(*) FROM \`madt-8102-479812.madt8102_bronze.users\` GROUP BY dt ORDER BY dt"
```

---

## Implementation Checklist

- [ ] Update argument parser in `main.py`
- [ ] Add validation for mutually exclusive arguments
- [ ] Update `run_ingestion()` function signature
- [ ] Update `CSVSource.extract_data()` method
- [ ] Update `PostgresSource.extract_data()` method
- [ ] Add full load targets to Makefile
- [ ] Update help documentation
- [ ] Add tests for full load mode
- [ ] Update README with examples
- [ ] Test with sample data

---

## Future Enhancements

1. **Parallel partition writing**: Write multiple partitions concurrently
2. **Progress tracking**: Show progress bar for large loads
3. **Resume capability**: Resume failed full loads from last partition
4. **Validation mode**: Dry-run to see what would be loaded
5. **Incremental + Full hybrid**: Load full for old data, incremental for recent
