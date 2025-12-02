CREATE OR REPLACE EXTERNAL TABLE `madt-8102-479812.bronze.customers`
(
  customer_id STRING,
  email STRING,
  first_name STRING,
  last_name STRING,
  created_at TIMESTAMP_NANOS,
  updated_at TIMESTAMP_NANOS
)
WITH PARTITION COLUMNS (
  dt DATE  -- Hive partition column
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://madt8102_bronze/customers/*'],  -- Pattern: dt=YYYY-MM-DD/*.parquet
  hive_partition_uri_prefix = 'gs://madt8102_bronze/customers',
  require_hive_partition_filter = false
);
