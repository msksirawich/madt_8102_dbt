#!/usr/bin/env python3
"""Main ingestion script for data ingestion to GCS."""

import argparse
import yaml
from datetime import datetime, date as date_type
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sources import PostgresSource, CSVSource
from targets import GCSTarget, DuckDBTarget


def load_config(config_path: str) -> dict:
    """Load configuration from YAML file.

    Args:
        config_path: Path to YAML configuration file

    Returns:
        Configuration dictionary
    """
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def validate_date(date_string: str) -> str:
    """Validate date format (YYYY-MM-DD).

    Args:
        date_string: Date string to validate

    Returns:
        Validated date string

    Raises:
        ValueError: If date format is invalid
    """
    try:
        datetime.strptime(date_string, '%Y-%m-%d')
        return date_string
    except ValueError:
        raise ValueError(f"Invalid date format: {date_string}. Expected YYYY-MM-DD")


def run_ingestion(config: dict, start_date: str = None, end_date: str = None, full_load: bool = False):
    """Run data ingestion pipeline.

    Args:
        config: Configuration dictionary
        start_date: Start date to filter data (YYYY-MM-DD), required if not full_load
        end_date: End date to filter data (YYYY-MM-DD), defaults to start_date if not provided
        full_load: If True, load all data without date filtering
    """
    if full_load:
        print("Starting FULL LOAD ingestion - loading all data...")
    else:
        # Default end_date to start_date for backward compatibility
        if end_date is None:
            end_date = start_date

        if start_date == end_date:
            print(f"Starting ingestion for date: {start_date}")
        else:
            print(f"Starting ingestion for date range: {start_date} to {end_date}")

    # Get configurations
    pipeline_config = config['pipeline']
    source_config = pipeline_config['source']
    target_config = pipeline_config['target']
    source_type = pipeline_config.get('source_type', 'postgres')

    # Initialize source based on type
    if source_type == 'csv':
        print(f"Source type: CSV")
        source = CSVSource(source_config)
        dataset_name = Path(source_config['file_path']).stem  # Use filename as dataset name
        source_identifier = source_config['file_path']
    elif source_type == 'postgres':
        print(f"Source type: PostgreSQL")
        db_config = config['database']
        source = PostgresSource(db_config)
        dataset_name = source_config['table']
        source_identifier = source_config['table']
    else:
        raise ValueError(f"Unsupported source type: {source_type}")

    # Initialize target based on type
    target_type = target_config.get('type', 'gcs')  # Default to GCS for backward compatibility

    if target_type == 'duckdb':
        target = DuckDBTarget(target_config)
        print(f"Target type: DuckDB")
    elif target_type == 'gcs':
        target = GCSTarget(target_config)
        print(f"Target type: GCS")
    else:
        raise ValueError(f"Unsupported target type: {target_type}")

    # Setup target credentials
    target.setup_credentials()

    if target_type == 'gcs':
        # Get base destination configuration (without specific partition)
        dest_config = target.get_destination_config(end_date)
        base_url = dest_config['base_url']  # gs://bucket
        table_path = dest_config['table_path']  # e.g., 'users'
        print(f"Target base path: {base_url}/{table_path}")
    else:  # duckdb
        # Get destination configuration (use end_date for partitioning)
        dest_config = target.get_destination_config(end_date)
        database_path = dest_config['database_path']
        table_name = dest_config['table_name']
        print(f"Target database: {database_path}")
        print(f"Target table: {table_name}")

    # Extract data
    print(f"Extracting data from: {source_identifier}")

    with source:
        if source_type == 'csv':
            data = source.extract_data(
                date_column=source_config['date_column'],
                start_date=start_date,
                end_date=end_date,
                full_load=full_load
            )
        else:  # postgres
            data = source.extract_data(
                table=source_config['table'],
                date_column=source_config['date_column'],
                start_date=start_date,
                end_date=end_date,
                full_load=full_load,
                schema=config['database'].get('schema', 'public')
            )

        # Convert data to list
        data_list = list(data)

        if not data_list:
            print("No data to load for this date.")
            return

        # Get the configured date column for dynamic partition assignment
        date_column = source_config.get('date_column')

        # Convert timestamp strings to datetime objects and add dt partition column
        for record in data_list:
            # Auto-detect and convert timestamp columns
            # Look for columns that:
            # - End with '_at' (created_at, updated_at, applied_at, published_at, etc.)
            # - End with '_date' (start_date, end_date, etc.)
            # - Contain 'timestamp' (event_timestamp, etc.)
            for key, value in record.items():
                # Check if column name matches timestamp patterns
                is_timestamp_column = (
                    key.endswith('_at') or
                    key.endswith('_date') or
                    'timestamp' in key.lower()
                )

                if is_timestamp_column:
                    if isinstance(value, str):
                        if value.strip():  # Non-empty string
                            try:
                                # Parse timestamp string to datetime object (preserves time)
                                record[key] = datetime.fromisoformat(value)
                            except (ValueError, AttributeError):
                                pass  # Keep as string if parsing fails
                        else:
                            # Empty string - convert to None for proper NULL handling
                            record[key] = None

            # Add dt column for partitioning (date only)
            # Priority: configured date_column > created_at > updated_at
            if date_column and date_column in record and isinstance(record[date_column], datetime):
                record['dt'] = record[date_column].date()
            elif 'created_at' in record and isinstance(record['created_at'], datetime):
                record['dt'] = record['created_at'].date()
            elif 'updated_at' in record and isinstance(record['updated_at'], datetime):
                record['dt'] = record['updated_at'].date()

        # Convert to DataFrame
        df = pd.DataFrame(data_list)

        # Add ingestion timestamp column
        df['_ingestion_time'] = pd.Timestamp.now()
        print(f"✓ Added _ingestion_time column: {df['_ingestion_time'].iloc[0]}")

        if target_type == 'gcs':
            # Check if 'dt' column exists for partitioning
            if 'dt' not in df.columns:
                print("Warning: 'dt' column not found. Writing all data to single partition.")
                # Write all data to single partition with end_date
                partition = f"dt={end_date}"
                full_path = f"{base_url}/{table_path}/{partition}"

                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"{timestamp}.parquet"
                file_path = f"{full_path}/{filename}"

                print(f"Writing {len(df)} records to {file_path}...")
                df.to_parquet(
                    file_path,
                    engine='pyarrow',
                    compression='snappy',
                    index=False,
                    coerce_timestamps='us'
                )
                print(f"✓ Successfully wrote {len(df)} records to {file_path}")
            else:
                # Group by 'dt' and write each partition separately
                unique_dates = df['dt'].unique()
                print(f"Found {len(unique_dates)} unique dates to partition: {sorted([str(d) for d in unique_dates])}")

                total_records = 0
                for dt_value in sorted(unique_dates):
                    # Filter data for this date
                    df_partition = df[df['dt'] == dt_value]

                    # Construct partition path
                    partition = f"dt={dt_value}"
                    full_path = f"{base_url}/{table_path}/{partition}"

                    # Generate filename with timestamp
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    filename = f"{timestamp}.parquet"
                    file_path = f"{full_path}/{filename}"

                    # Write parquet file directly to GCS
                    print(f"Writing {len(df_partition)} records to {file_path}...")

                    # Use coerce_timestamps='us' for BigQuery compatibility (microsecond precision)
                    df_partition.to_parquet(
                        file_path,
                        engine='pyarrow',
                        compression='snappy',
                        index=False,
                        coerce_timestamps='us'
                    )

                    total_records += len(df_partition)
                    print(f"✓ Successfully wrote {len(df_partition)} records to partition {partition}")

                print(f"✓ Total {total_records} records written across {len(unique_dates)} partitions")
                print(f"  Total memory size: {df.memory_usage(deep=True).sum() / 1024:.2f} KB")

        elif target_type == 'duckdb':
            # Write to DuckDB
            print(f"Writing {len(df)} records to DuckDB...")

            with target:
                target.write_data(df, end_date)

            print(f"  Memory size: {df.memory_usage(deep=True).sum() / 1024:.2f} KB")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Ingest data from various sources (PostgreSQL, CSV) to Google Cloud Storage or DuckDB'
    )
    parser.add_argument(
        '--config',
        type=str,
        default='config/pipeline_config.yaml',
        help='Path to pipeline configuration YAML file'
    )
    parser.add_argument(
        '--execution-date',
        type=str,
        help='(Deprecated) Execution date for data filtering (YYYY-MM-DD format). Use --start-date instead.'
    )
    parser.add_argument(
        '--start-date',
        type=str,
        help='Start date for data filtering (YYYY-MM-DD format)'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        help='End date for data filtering (YYYY-MM-DD format). Defaults to start-date if not provided.'
    )
    parser.add_argument(
        '--full-load',
        action='store_true',
        default=False,
        help='Load all data without date filtering. Cannot be used with date parameters.'
    )

    args = parser.parse_args()

    # Validate mutually exclusive arguments
    if args.full_load and (args.start_date or args.end_date or args.execution_date):
        parser.error("--full-load cannot be used with --start-date, --end-date, or --execution-date")

    # Handle date parameters
    if args.full_load:
        # Full load mode - no dates needed
        start_date = None
        end_date = None
    elif args.execution_date and not args.start_date:
        # Backward compatibility with --execution-date
        start_date = validate_date(args.execution_date)
        end_date = start_date
        print("Note: --execution-date is deprecated. Use --start-date and --end-date instead.")
    elif args.start_date:
        # Incremental mode with date range
        start_date = validate_date(args.start_date)
        end_date = validate_date(args.end_date) if args.end_date else start_date
    else:
        parser.error("Either --full-load or date parameters (--start-date or --execution-date) must be provided")

    # Validate date range (only for incremental mode)
    if not args.full_load and start_date > end_date:
        raise ValueError(f"start_date ({start_date}) cannot be after end_date ({end_date})")

    # Load configuration
    config = load_config(args.config)

    # Run ingestion
    try:
        run_ingestion(config, start_date, end_date, full_load=args.full_load)
        print("Ingestion completed successfully!")
    except Exception as e:
        print(f"Error during ingestion: {str(e)}")
        raise


if __name__ == '__main__':
    main()
