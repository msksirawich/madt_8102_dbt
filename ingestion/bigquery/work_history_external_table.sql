-- BigQuery External Table DDL for Work History
-- This table reads from GCS with Hive-style partitioning (dt=YYYY-MM-DD)

CREATE OR REPLACE EXTERNAL TABLE `madt-8102-479812.madt8102_bronze.work_history`
(
  work_id STRING,
  user_id STRING,
  company_name STRING,
  title STRING,
  start_date TIMESTAMP,
  end_date STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _ingestion_time TIMESTAMP
)
WITH PARTITION COLUMNS (
  dt DATE  -- Hive partition column
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://madt8102_bronze/work_history/*'],  -- Pattern: dt=YYYY-MM-DD/*.parquet
  hive_partition_uri_prefix = 'gs://madt8102_bronze/work_history',
  require_hive_partition_filter = false
);

-- Notes:
-- 1. The 'dt' column is automatically populated from the Hive partition path
-- 2. Data location: gs://madt8102_bronze/work_history/dt=YYYY-MM-DD/
-- 3. require_hive_partition_filter = false allows queries without partition filter
--    Set to true to enforce partition filtering for cost optimization
-- 4. _ingestion_time is automatically added during ingestion
-- 5. end_date may be NULL for current positions

-- Example queries:

-- Query all data
-- SELECT * FROM `madt-8102-479812.madt8102_bronze.work_history`;

-- Query specific partition (recommended for large datasets)
-- SELECT * FROM `madt-8102-479812.madt8102_bronze.work_history`
-- WHERE dt = '2024-12-01';

-- Query with date range
-- SELECT * FROM `madt-8102-479812.madt8102_bronze.work_history`
-- WHERE dt BETWEEN '2024-12-01' AND '2024-12-07';

-- Query current positions (end_date is NULL)
-- SELECT * FROM `madt-8102-479812.madt8102_bronze.work_history`
-- WHERE end_date IS NULL AND dt = '2024-12-01';
