#!/usr/bin/env python3
"""Script to execute BigQuery DDL files from the bigquery folder."""

import argparse
import os
from pathlib import Path
from typing import List, Optional
from google.cloud import bigquery
from google.api_core import exceptions


def setup_bigquery_client(credentials_path: Optional[str] = None) -> bigquery.Client:
    """Initialize BigQuery client.

    Args:
        credentials_path: Optional path to service account credentials JSON file.
                         If not provided, uses default credentials from environment.

    Returns:
        BigQuery client instance
    """
    if credentials_path:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
        print(f"Using credentials from: {credentials_path}")
    else:
        print("Using default credentials from environment")

    return bigquery.Client()


def get_sql_files(sql_folder: str, file_pattern: Optional[str] = None) -> List[Path]:
    """Get list of SQL files to execute.

    Args:
        sql_folder: Path to folder containing SQL files
        file_pattern: Optional pattern to filter files (e.g., 'users*.sql')

    Returns:
        List of Path objects for SQL files
    """
    folder = Path(sql_folder)

    if not folder.exists():
        raise FileNotFoundError(f"Folder not found: {sql_folder}")

    if file_pattern:
        sql_files = sorted(folder.glob(file_pattern))
    else:
        sql_files = sorted(folder.glob('*.sql'))

    if not sql_files:
        pattern_msg = f" matching '{file_pattern}'" if file_pattern else ""
        raise FileNotFoundError(f"No SQL files found{pattern_msg} in {sql_folder}")

    return sql_files


def read_sql_file(file_path: Path) -> str:
    """Read SQL content from file.

    Args:
        file_path: Path to SQL file

    Returns:
        SQL query string with comments removed
    """
    with open(file_path, 'r') as f:
        content = f.read()

    # Remove single-line comments that start with --
    lines = []
    for line in content.split('\n'):
        # Keep lines that aren't pure comments
        stripped = line.strip()
        if not stripped.startswith('--'):
            lines.append(line)

    return '\n'.join(lines).strip()


def execute_sql(client: bigquery.Client, sql: str, file_name: str, dry_run: bool = False) -> bool:
    """Execute SQL query on BigQuery.

    Args:
        client: BigQuery client
        sql: SQL query to execute
        file_name: Name of the SQL file (for logging)
        dry_run: If True, validate query without executing

    Returns:
        True if successful, False otherwise
    """
    try:
        if dry_run:
            print(f"\n[DRY RUN] Validating: {file_name}")
            print("-" * 60)
            # Create job config for dry run
            job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
            query_job = client.query(sql, job_config=job_config)

            # Dry run doesn't return results, just validates
            print(f"✓ Query validation successful")
            print(f"  This query will process {query_job.total_bytes_processed:,} bytes when run")
            return True
        else:
            print(f"\n[EXECUTE] Running: {file_name}")
            print("-" * 60)

            # Execute the query
            query_job = client.query(sql)
            query_job.result()  # Wait for completion

            print(f"✓ Successfully executed {file_name}")
            print(f"  Job ID: {query_job.job_id}")
            print(f"  Bytes processed: {query_job.total_bytes_processed:,}")
            return True

    except exceptions.GoogleAPIError as e:
        print(f"✗ Error executing {file_name}")
        print(f"  Error: {str(e)}")
        return False
    except Exception as e:
        print(f"✗ Unexpected error executing {file_name}")
        print(f"  Error: {str(e)}")
        return False


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Execute BigQuery DDL files from the bigquery folder',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all SQL files in bigquery folder
  python run_bigquery_ddl.py

  # Run all SQL files with custom credentials
  python run_bigquery_ddl.py --credentials /path/to/service-account.json

  # Run specific file pattern
  python run_bigquery_ddl.py --pattern "users*.sql"

  # Run specific SQL files
  python run_bigquery_ddl.py --files users_external_table.sql orders_external_table.sql

  # Dry run (validate only, don't execute)
  python run_bigquery_ddl.py --dry-run

  # Custom SQL folder
  python run_bigquery_ddl.py --sql-folder /path/to/sql/files
        """
    )

    parser.add_argument(
        '--sql-folder',
        type=str,
        default='bigquery',
        help='Path to folder containing SQL files (default: bigquery)'
    )

    parser.add_argument(
        '--credentials',
        type=str,
        help='Path to Google Cloud service account credentials JSON file'
    )

    parser.add_argument(
        '--pattern',
        type=str,
        help='Pattern to filter SQL files (e.g., "users*.sql")'
    )

    parser.add_argument(
        '--files',
        nargs='+',
        help='Specific SQL files to execute (space-separated)'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Validate queries without executing them'
    )

    parser.add_argument(
        '--continue-on-error',
        action='store_true',
        help='Continue executing remaining files even if one fails'
    )

    args = parser.parse_args()

    try:
        # Initialize BigQuery client
        print("=" * 60)
        print("BigQuery DDL Execution Script")
        print("=" * 60)

        client = setup_bigquery_client(args.credentials)

        # Get SQL files to execute
        if args.files:
            # Execute specific files
            sql_folder = Path(args.sql_folder)
            sql_files = [sql_folder / f for f in args.files]

            # Check if files exist
            for f in sql_files:
                if not f.exists():
                    raise FileNotFoundError(f"File not found: {f}")
        else:
            # Execute all files or filtered by pattern
            sql_files = get_sql_files(args.sql_folder, args.pattern)

        print(f"\nFound {len(sql_files)} SQL file(s) to execute:")
        for f in sql_files:
            print(f"  - {f.name}")

        if args.dry_run:
            print("\n⚠️  DRY RUN MODE: Queries will be validated but not executed")

        # Execute SQL files
        print("\n" + "=" * 60)
        print("Execution Results")
        print("=" * 60)

        successful = 0
        failed = 0

        for sql_file in sql_files:
            try:
                sql_content = read_sql_file(sql_file)

                if not sql_content:
                    print(f"\n⚠️  Skipping {sql_file.name}: Empty file")
                    continue

                success = execute_sql(
                    client,
                    sql_content,
                    sql_file.name,
                    dry_run=args.dry_run
                )

                if success:
                    successful += 1
                else:
                    failed += 1
                    if not args.continue_on_error:
                        print("\n❌ Stopping execution due to error")
                        print("   Use --continue-on-error to continue on errors")
                        break

            except Exception as e:
                print(f"\n✗ Error processing {sql_file.name}: {str(e)}")
                failed += 1
                if not args.continue_on_error:
                    print("\n❌ Stopping execution due to error")
                    break

        # Print summary
        print("\n" + "=" * 60)
        print("Summary")
        print("=" * 60)
        print(f"Total files: {len(sql_files)}")
        print(f"✓ Successful: {successful}")
        print(f"✗ Failed: {failed}")

        if failed == 0:
            print("\n✅ All operations completed successfully!")
            return 0
        else:
            print("\n⚠️  Some operations failed. Please check the errors above.")
            return 1

    except Exception as e:
        print(f"\n❌ Fatal error: {str(e)}")
        return 1


if __name__ == '__main__':
    exit(main())
