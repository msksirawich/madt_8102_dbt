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


def run_ingestion(config: dict, start_date: str, end_date: str = None):
    """Run data ingestion pipeline.

    Args:
        config: Configuration dictionary
        start_date: Start date to filter data (YYYY-MM-DD)
        end_date: End date to filter data (YYYY-MM-DD), defaults to start_date if not provided
    """
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

    # Get destination configuration (use end_date for partitioning)
    dest_config = target.get_destination_config(end_date)

    if target_type == 'gcs':
        base_url = dest_config['base_url']  # gs://bucket
        table_path = dest_config['table_path']  # e.g., 'users'
        partition = dest_config['partition']  # e.g., 'dt=2024-12-01'

        # Construct final path: gs://bucket/table/dt=2024-12-01
        full_path = f"{base_url}/{table_path}/{partition}"
        print(f"Target path: {full_path}")
    else:  # duckdb
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
                end_date=end_date
            )
        else:  # postgres
            data = source.extract_data(
                table=source_config['table'],
                date_column=source_config['date_column'],
                start_date=start_date,
                end_date=end_date,
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
            # Convert timestamp string columns to datetime objects
            # Check common timestamp column names
            timestamp_columns = ['created_at', 'updated_at', 'event_timestamp', 'timestamp']
            if date_column and date_column not in timestamp_columns:
                timestamp_columns.append(date_column)

            for key, value in record.items():
                if isinstance(value, str) and key in timestamp_columns:
                    try:
                        # Parse timestamp string to datetime object (preserves time)
                        record[key] = datetime.fromisoformat(value)
                    except (ValueError, AttributeError):
                        pass  # Keep as string if parsing fails

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
            # Generate filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{timestamp}.parquet"
            file_path = f"{full_path}/{filename}"

            # Write parquet file directly to GCS
            print(f"Writing {len(df)} records to {file_path}...")

            # Write using pandas to_parquet with gcsfs (simpler approach)
            # Use coerce_timestamps='us' for BigQuery compatibility (microsecond precision)
            df.to_parquet(
                file_path,
                engine='pyarrow',
                compression='snappy',
                index=False,
                coerce_timestamps='us'  # Convert timestamps to microseconds (BigQuery compatible)
            )

            print(f"✓ Successfully wrote {len(df)} records to {file_path}")
            print(f"  File size: {df.memory_usage(deep=True).sum() / 1024:.2f} KB")

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

    args = parser.parse_args()

    # Handle backward compatibility with --execution-date
    if args.execution_date and not args.start_date:
        start_date = validate_date(args.execution_date)
        end_date = start_date
        print("Note: --execution-date is deprecated. Use --start-date and --end-date instead.")
    elif args.start_date:
        start_date = validate_date(args.start_date)
        end_date = validate_date(args.end_date) if args.end_date else start_date
    else:
        parser.error("Either --execution-date or --start-date must be provided")

    # Validate date range
    if start_date > end_date:
        raise ValueError(f"start_date ({start_date}) cannot be after end_date ({end_date})")

    # Load configuration
    config = load_config(args.config)

    # Run ingestion
    try:
        run_ingestion(config, start_date, end_date)
        print("Ingestion completed successfully!")
    except Exception as e:
        print(f"Error during ingestion: {str(e)}")
        raise


if __name__ == '__main__':
    main()
