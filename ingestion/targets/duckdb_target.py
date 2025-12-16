"""DuckDB target module for local database storage."""

import duckdb
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
import pandas as pd


class DuckDBTarget:
    """DuckDB target for local database storage with partitioning support."""

    def __init__(self, target_config: Dict[str, Any]):
        """Initialize DuckDB target.

        Args:
            target_config: Target configuration dictionary
        """
        self.database_path = target_config['database_path']
        self.schema = target_config.get('schema', None)
        self.table_name = target_config['table_name']
        self.partition_column = target_config.get('partition_column', 'dt')
        self.if_exists = target_config.get('if_exists', 'append')  # append, replace, or overwrite_partition

        # Construct full table name with schema if provided
        if self.schema:
            self.full_table_name = f"{self.schema}.{self.table_name}"
        else:
            self.full_table_name = self.table_name

        # Create directory if it doesn't exist
        db_dir = Path(self.database_path).parent
        if db_dir and not db_dir.exists():
            db_dir.mkdir(parents=True, exist_ok=True)

        self.connection = None

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def connect(self):
        """Establish connection to DuckDB database."""
        self.connection = duckdb.connect(self.database_path)
        print(f"✓ Connected to DuckDB: {self.database_path}")

        # Create schema if it doesn't exist
        if self.schema:
            self.connection.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
            print(f"✓ Schema ensured: {self.schema}")

    def close(self):
        """Close DuckDB connection."""
        if self.connection:
            self.connection.close()
            print(f"✓ Closed DuckDB connection")

    def setup_credentials(self):
        """Setup credentials (not needed for DuckDB, included for interface compatibility)."""
        print(f"✓ DuckDB target initialized: {self.database_path}")

    def get_destination_config(self, execution_date: str) -> Dict[str, str]:
        """Get destination configuration for DuckDB.

        Args:
            execution_date: Execution date for partitioning

        Returns:
            Dictionary with DuckDB destination configuration
        """
        # Convert datetime to string if needed
        if isinstance(execution_date, datetime):
            execution_date = execution_date.strftime('%Y-%m-%d')

        config = {
            'destination': 'duckdb',
            'database_path': self.database_path,
            'table_name': self.table_name,
            'partition_column': self.partition_column,
            'partition_value': execution_date,
            'if_exists': self.if_exists
        }

        return config

    def write_data(self, df: pd.DataFrame, execution_date: str):
        """Write DataFrame to DuckDB table.

        Args:
            df: Pandas DataFrame to write
            execution_date: Execution date for partition column
        """
        if not self.connection:
            self.connect()

        # Add partition column if specified
        partition_value = pd.to_datetime(execution_date).date()
        if self.partition_column and self.partition_column not in df.columns:
            df[self.partition_column] = partition_value

        # Check if table exists
        if self.schema:
            table_exists = self.connection.execute(
                f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{self.schema}' AND table_name = '{self.table_name}'"
            ).fetchone()[0] > 0
        else:
            table_exists = self.connection.execute(
                f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{self.table_name}'"
            ).fetchone()[0] > 0

        if not table_exists:
            # Create table from DataFrame
            self.connection.execute(f"CREATE TABLE {self.full_table_name} AS SELECT * FROM df")
            print(f"✓ Created table: {self.full_table_name}")
            print(f"✓ Inserted {len(df)} records")
        else:
            if self.if_exists == 'replace':
                # Drop and recreate table
                self.connection.execute(f"DROP TABLE {self.full_table_name}")
                self.connection.execute(f"CREATE TABLE {self.full_table_name} AS SELECT * FROM df")
                print(f"✓ Replaced table: {self.full_table_name}")
                print(f"✓ Inserted {len(df)} records")
            elif self.if_exists == 'overwrite_partition':
                # Delete existing records for this partition
                delete_count = self.connection.execute(
                    f"SELECT COUNT(*) FROM {self.full_table_name} WHERE {self.partition_column} = ?",
                    [partition_value]
                ).fetchone()[0]

                if delete_count > 0:
                    self.connection.execute(
                        f"DELETE FROM {self.full_table_name} WHERE {self.partition_column} = ?",
                        [partition_value]
                    )
                    print(f"✓ Deleted {delete_count} existing records for partition {self.partition_column}={execution_date}")

                # Insert new data
                self.connection.execute(f"INSERT INTO {self.full_table_name} SELECT * FROM df")
                print(f"✓ Inserted {len(df)} records for partition {self.partition_column}={execution_date}")
            else:  # append
                # Insert data into existing table
                self.connection.execute(f"INSERT INTO {self.full_table_name} SELECT * FROM df")
                print(f"✓ Appended {len(df)} records to table: {self.full_table_name}")

        # Show table info
        row_count = self.connection.execute(f"SELECT COUNT(*) FROM {self.full_table_name}").fetchone()[0]
        print(f"  Total records in {self.full_table_name}: {row_count}")

        # Show partition info if using overwrite_partition
        if self.if_exists == 'overwrite_partition' and self.partition_column:
            partition_count = self.connection.execute(
                f"SELECT COUNT(DISTINCT {self.partition_column}) FROM {self.full_table_name}"
            ).fetchone()[0]
            print(f"  Total partitions in {self.full_table_name}: {partition_count}")

    def get_table_info(self) -> Dict[str, Any]:
        """Get information about the target table.

        Returns:
            Dictionary with table information
        """
        if not self.connection:
            self.connect()

        # Check if table exists
        if self.schema:
            table_exists = self.connection.execute(
                f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{self.schema}' AND table_name = '{self.table_name}'"
            ).fetchone()[0] > 0
        else:
            table_exists = self.connection.execute(
                f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{self.table_name}'"
            ).fetchone()[0] > 0

        if not table_exists:
            return {
                'exists': False,
                'row_count': 0,
                'schema': None
            }

        # Get row count
        row_count = self.connection.execute(f"SELECT COUNT(*) FROM {self.full_table_name}").fetchone()[0]

        # Get schema
        schema_info = self.connection.execute(f"DESCRIBE {self.full_table_name}").fetchdf()

        return {
            'exists': True,
            'row_count': row_count,
            'schema': schema_info.to_dict('records')
        }

    def query(self, sql: str) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame.

        Args:
            sql: SQL query to execute

        Returns:
            Query results as DataFrame
        """
        if not self.connection:
            self.connect()

        return self.connection.execute(sql).fetchdf()
