"""Google Cloud Storage target module with Hive-style partitioning."""

import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional


class GCSTarget:
    """GCS target with Hive-style partitioning support."""

    def __init__(self, target_config: Dict[str, Any]):
        """Initialize GCS target.

        Args:
            target_config: Target configuration dictionary
        """
        self.bucket = target_config['bucket']
        self.path = target_config['path']
        self.partition_column = target_config.get('partition_column')
        self.file_format = target_config.get('file_format', 'parquet')
        self.credentials_path = target_config.get('credentials_path')

        # Validate credentials if provided
        if self.credentials_path:
            self._validate_credentials()

    def get_partition_path(self, partition_value: str) -> str:
        """Generate Hive-style partition path.

        Args:
            partition_value: Value to partition by (e.g., '2024-12-01')

        Returns:
            Full GCS path with Hive partitioning (e.g., bucket/path/dt=2024-12-01)
        """
        if self.partition_column:
            # Convert date to YYYY-MM-DD if it's a datetime
            if isinstance(partition_value, datetime):
                partition_value = partition_value.strftime('%Y-%m-%d')

            # Create Hive-style partition: dt=YYYY-MM-DD
            partition_path = f"dt={partition_value}"
            return f"gs://{self.bucket}/{self.path}/{partition_path}"
        else:
            return f"gs://{self.bucket}/{self.path}"

    def _validate_credentials(self):
        """Validate that credentials file exists."""
        if not Path(self.credentials_path).exists():
            raise FileNotFoundError(
                f"GCS credentials file not found: {self.credentials_path}"
            )

    def setup_credentials(self):
        """Setup GCS credentials by setting environment variable.

        This sets GOOGLE_APPLICATION_CREDENTIALS which is automatically
        used by Google Cloud client libraries and dlt.
        """
        if self.credentials_path:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.credentials_path
            print(f"✓ GCS credentials loaded from: {self.credentials_path}")
        else:
            if 'GOOGLE_APPLICATION_CREDENTIALS' in os.environ:
                print(f"✓ Using GCS credentials from environment variable")
            else:
                print("⚠ No GCS credentials specified. Using Application Default Credentials")

    def get_destination_config(self, execution_date: str) -> Dict[str, str]:
        """Get dlt destination configuration for GCS.

        Args:
            execution_date: Execution date for partitioning

        Returns:
            Dictionary with GCS destination configuration
        """
        # Convert datetime to string if needed
        if isinstance(execution_date, datetime):
            execution_date = execution_date.strftime('%Y-%m-%d')

        # Build path WITHOUT table name (dlt will add table_name from pipeline.run)
        if self.partition_column:
            # gs://bucket/dt=2024-12-01 (dlt will add table_name to make: bucket/table/dt=2024-12-01)
            # But we want: bucket/table/dt=2024-12-01/table/
            # So we need: bucket_url = gs://bucket, and use layout or different approach

            # Actually, let's use: gs://bucket and configure table_name properly
            base_url = f"gs://{self.bucket}"
            partition = f"dt={execution_date}"
        else:
            base_url = f"gs://{self.bucket}"
            partition = ""

        config = {
            'destination': 'filesystem',
            'base_url': base_url,
            'table_path': self.path,  # e.g., 'users'
            'partition': partition,
            'file_format': self.file_format
        }

        # Add credentials if specified
        if self.credentials_path:
            config['credentials_path'] = self.credentials_path

        return config
