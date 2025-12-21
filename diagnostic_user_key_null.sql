-- Diagnostic queries to identify why user_key is NULL in fact_job_content_engagement
-- Run these queries in BigQuery Console to diagnose the issue

-- Query 1: Check sample data from dim_user
-- Expected: Should see user records with user_id_natural matching USR000001, USR000002, etc.
SELECT
    user_key,
    user_id_natural,
    is_current,
    valid_from,
    valid_to
FROM `madt-8102-479812.madt8102_gold.dim_user`
WHERE is_current = true
ORDER BY user_id_natural
LIMIT 20;

-- Query 2: Count total current users in dim_user
SELECT
    COUNT(*) as total_current_users,
    MIN(user_id_natural) as min_user_id,
    MAX(user_id_natural) as max_user_id
FROM `madt-8102-479812.madt8102_gold.dim_user`
WHERE is_current = true;

-- Query 3: Check sample user_id values from streaming events
SELECT DISTINCT
    user_id,
    COUNT(*) as event_count
FROM `madt-8102-479812.madt8102_silver.txn_streaming_job_activity`
WHERE user_id IS NOT NULL
GROUP BY user_id
ORDER BY user_id
LIMIT 20;

-- Query 4: Count streaming events with NULL vs non-NULL user_id
SELECT
    CASE WHEN user_id IS NULL THEN 'NULL' ELSE 'NOT NULL' END as user_id_status,
    COUNT(*) as event_count
FROM `madt-8102-479812.madt8102_silver.txn_streaming_job_activity`
GROUP BY 1;

-- Query 5: Check for user_id mismatches (users in streaming but NOT in dim_user)
WITH streaming_users AS (
    SELECT DISTINCT user_id
    FROM `madt-8102-479812.madt8102_silver.txn_streaming_job_activity`
    WHERE user_id IS NOT NULL
),
dim_users AS (
    SELECT DISTINCT user_id_natural
    FROM `madt-8102-479812.madt8102_gold.dim_user`
    WHERE is_current = true
)
SELECT
    COUNT(DISTINCT su.user_id) as total_streaming_users,
    COUNT(DISTINCT du.user_id_natural) as total_dim_users,
    COUNT(DISTINCT CASE WHEN du.user_id_natural IS NULL THEN su.user_id END) as unmatched_users
FROM streaming_users su
LEFT JOIN dim_users du ON su.user_id = du.user_id_natural;

-- Query 6: Sample unmatched user_ids (if any)
WITH streaming_users AS (
    SELECT DISTINCT user_id
    FROM `madt-8102-479812.madt8102_silver.txn_streaming_job_activity`
    WHERE user_id IS NOT NULL
),
dim_users AS (
    SELECT DISTINCT user_id_natural
    FROM `madt-8102-479812.madt8102_gold.dim_user`
    WHERE is_current = true
)
SELECT su.user_id as unmatched_user_id
FROM streaming_users su
LEFT JOIN dim_users du ON su.user_id = du.user_id_natural
WHERE du.user_id_natural IS NULL
LIMIT 20;

-- Query 7: Check fact_job_content_engagement - count NULL user_keys
SELECT
    CASE WHEN user_key IS NULL THEN 'NULL' ELSE 'NOT NULL' END as user_key_status,
    COUNT(*) as row_count
FROM `madt-8102-479812.madt8102_gold.fact_job_content_engagement`
GROUP BY 1;

-- Query 8: Sample records from fact_job_content_engagement showing the issue
SELECT
    engagement_id,
    user_key,
    job_key,
    date_key,
    view_count
FROM `madt-8102-479812.madt8102_gold.fact_job_content_engagement`
LIMIT 20;
