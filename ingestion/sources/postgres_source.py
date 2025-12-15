"""PostgreSQL source module for data ingestion."""

from datetime import datetime
from typing import Iterator, Dict, Any
import psycopg2
from psycopg2.extras import RealDictCursor


class PostgresSource:
    """PostgreSQL data source for extracting data."""

    def __init__(self, db_config: Dict[str, Any]):
        """Initialize PostgreSQL source.

        Args:
            db_config: Database configuration dictionary
        """
        self.db_config = db_config
        self.connection = None

    def connect(self):
        """Establish connection to PostgreSQL database."""
        self.connection = psycopg2.connect(
            host=self.db_config['host'],
            port=self.db_config['port'],
            database=self.db_config['database'],
            user=self.db_config['user'],
            password=self.db_config['password']
        )

    def disconnect(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None

    def extract_data(
        self,
        table: str,
        date_column: str,
        start_date: str,
        end_date: str = None,
        schema: str = 'public',
        batch_size: int = 1000
    ) -> Iterator[Dict[str, Any]]:
        """Extract data from PostgreSQL table filtered by date range.

        Args:
            table: Table name to extract from
            date_column: Column name to filter by date
            start_date: Start date to filter (YYYY-MM-DD format)
            end_date: End date to filter (YYYY-MM-DD format), defaults to start_date if not provided
            schema: Database schema (default: public)
            batch_size: Number of rows to fetch per batch

        Yields:
            Dictionary representing each row
        """
        if not self.connection:
            self.connect()

        # Default end_date to start_date for backward compatibility
        if end_date is None:
            end_date = start_date

        # Build query with date range filter
        query = f"""
            SELECT *
            FROM {schema}.{table}
            WHERE DATE({date_column}) BETWEEN %s AND %s
        """

        cursor = self.connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query, (start_date, end_date))

        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break

            for row in rows:
                yield dict(row)

        cursor.close()

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
