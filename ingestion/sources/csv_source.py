"""CSV source module for data ingestion."""

from datetime import datetime
from typing import Iterator, Dict, Any
import csv
from pathlib import Path


class CSVSource:
    """CSV data source for extracting data."""

    def __init__(self, source_config: Dict[str, Any]):
        """Initialize CSV source.

        Args:
            source_config: Source configuration dictionary
        """
        self.file_path = source_config.get('file_path')
        self.encoding = source_config.get('encoding', 'utf-8')

    def extract_data(
        self,
        date_column: str,
        execution_date: str,
        batch_size: int = 1000
    ) -> Iterator[Dict[str, Any]]:
        """Extract data from CSV file filtered by date.

        Args:
            date_column: Column name to filter by date
            execution_date: Date to filter (YYYY-MM-DD format)
            batch_size: Number of rows to yield per batch (for consistency)

        Yields:
            Dictionary representing each row
        """
        if not self.file_path:
            raise ValueError("file_path is required in source configuration")

        file_path = Path(self.file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {self.file_path}")

        with open(file_path, 'r', encoding=self.encoding) as csvfile:
            reader = csv.DictReader(csvfile)

            for row in reader:
                # Filter by date
                if date_column in row:
                    row_date = row[date_column].split()[0]  # Extract date part (YYYY-MM-DD)
                    if row_date == execution_date:
                        yield row

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        pass
