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
from targets import GCSTarget


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


def run_ingestion(config: dict, execution_date: str):
    """Run data ingestion pipeline.

    Args:
        config: Configuration dictionary
        execution_date: Date to filter data (YYYY-MM-DD)
    """
    print(f"Starting ingestion for date: {execution_date}")

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

    # Initialize target
    target = GCSTarget(target_config)

    # Setup GCS credentials
    target.setup_credentials()

    # Get destination configuration
    dest_config = target.get_destination_config(execution_date)
    base_url = dest_config['base_url']  # gs://bucket
    table_path = dest_config['table_path']  # e.g., 'users'
    partition = dest_config['partition']  # e.g., 'dt=2024-12-01'

    # Construct final path: gs://bucket/table/dt=2024-12-01
    full_path = f"{base_url}/{table_path}/{partition}"

    print(f"Target path: {full_path}")

    # Extract data
    print(f"Extracting data from: {source_identifier}")

    with source:
        if source_type == 'csv':
            data = source.extract_data(
                date_column=source_config['date_column'],
                execution_date=execution_date
            )
        else:  # postgres
            data = source.extract_data(
                table=source_config['table'],
                date_column=source_config['date_column'],
                execution_date=execution_date,
                schema=config['database'].get('schema', 'public')
            )

        # Convert data to list
        data_list = list(data)

        if not data_list:
            print("No data to load for this date.")
            return

        # Convert timestamp strings to datetime objects and add dt partition column
        for record in data_list:
            # Convert timestamp string columns to datetime objects
            for key, value in record.items():
                if isinstance(value, str) and key in ['created_at', 'updated_at']:
                    try:
                        # Parse timestamp string to datetime object (preserves time)
                        record[key] = datetime.fromisoformat(value)
                    except (ValueError, AttributeError):
                        pass  # Keep as string if parsing fails

            # Add dt column for partitioning (date only)
            if 'created_at' in record and isinstance(record['created_at'], datetime):
                record['dt'] = record['created_at'].date()
            elif 'updated_at' in record and isinstance(record['updated_at'], datetime):
                record['dt'] = record['updated_at'].date()

        # Convert to DataFrame
        df = pd.DataFrame(data_list)

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


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Ingest data from various sources (PostgreSQL, CSV) to Google Cloud Storage'
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
        required=True,
        help='Execution date for data filtering (YYYY-MM-DD format)'
    )

    args = parser.parse_args()

    # Validate execution date
    execution_date = validate_date(args.execution_date)

    # Load configuration
    config = load_config(args.config)

    # Run ingestion
    try:
        run_ingestion(config, execution_date)
        print("Ingestion completed successfully!")
    except Exception as e:
        print(f"Error during ingestion: {str(e)}")
        raise


if __name__ == '__main__':
    main()
