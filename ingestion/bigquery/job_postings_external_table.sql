-- BigQuery External Table DDL for Job Postings
-- This table reads from GCS with Hive-style partitioning (dt=YYYY-MM-DD)

CREATE OR REPLACE EXTERNAL TABLE `madt-8102-479812.madt8102_bronze.job_postings`
(
  job_id STRING,
  company_id STRING,
  title STRING,
  description_html STRING,
  min_salary STRING,
  max_salary STRING,
  status STRING,
  published_at TIMESTAMP,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _ingestion_time TIMESTAMP
)
WITH PARTITION COLUMNS (
  dt DATE  -- Hive partition column
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://madt8102_bronze/job_postings/*'],  -- Pattern: dt=YYYY-MM-DD/*.parquet
  hive_partition_uri_prefix = 'gs://madt8102_bronze/job_postings',
  require_hive_partition_filter = false
);

-- Notes:
-- 1. The 'dt' column is automatically populated from the Hive partition path
-- 2. Data location: gs://madt8102_bronze/job_postings/dt=YYYY-MM-DD/
-- 3. require_hive_partition_filter = false allows queries without partition filter
--    Set to true to enforce partition filtering for cost optimization
-- 4. _ingestion_time is automatically added during ingestion

-- Example queries:

-- Query all data
-- SELECT * FROM `madt-8102-479812.madt8102_bronze.job_postings`;

-- Query specific partition (recommended for large datasets)
-- SELECT * FROM `madt-8102-479812.madt8102_bronze.job_postings`
-- WHERE dt = '2024-12-01';

-- Query with date range
-- SELECT * FROM `madt-8102-479812.madt8102_bronze.job_postings`
-- WHERE dt BETWEEN '2024-12-01' AND '2024-12-07';
