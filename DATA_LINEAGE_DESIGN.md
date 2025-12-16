# Data Lineage Design: OLTP → Dimensions & Facts

## Architecture Overview

```
OLTP (Bronze) → Data Warehouse (Silver) → Data Marts (Gold)
```

---

## 🔷 DIMENSION TABLES

### 1. `dim_date`
**Type**: Static Reference Dimension
**Source**: Generated (date spine/calendar table)

**Transformation Logic**:
- Generate date range (e.g., 2020-01-01 to 2030-12-31)
- Derive attributes:
  - `date_key` = YYYYMMDD format (e.g., 20250101)
  - `is_weekend` = day of week IN (Saturday, Sunday)
  - `is_holiday` = lookup against holiday calendar

**No upstream OLTP dependency**

---

### 2. `dim_time`
**Type**: Static Reference Dimension
**Source**: Generated (time spine table)

**Transformation Logic**:
- Generate time buckets (00:00:00 to 23:59:59)
- Classify time segments:
  - `time_segment`:
    - "morning" = 06:00-12:00
    - "work hours" = 09:00-17:00
    - "evening" = 17:00-23:00

**No upstream OLTP dependency**

---

### 3. `dim_user`
**Type**: SCD Type 2 Dimension
**Upstream Sources**:
- `users` (primary)
- `profiles` (1:1 join)
- `education` (aggregation)
- `work_history` (aggregation)

**Transformation Logic**:
```sql
-- Step 1: Join core tables
SELECT
    ROW_NUMBER() OVER (ORDER BY u.user_id) AS user_key,
    u.user_id AS user_id_natural,
    p.headline AS current_role,

    -- Step 2: Calculate years_experience from work_history
    COALESCE(
        DATE_DIFF(
            CURRENT_DATE(),
            MIN(wh.start_date),
            YEAR
        ), 0
    ) AS years_experience,

    -- Step 3: Get highest degree from education (ranking: PhD > MS > BS)
    CASE
        WHEN MAX(CASE WHEN e.degree = 'PhD' THEN 1 END) = 1 THEN 'PhD'
        WHEN MAX(CASE WHEN e.degree = 'MS' THEN 1 END) = 1 THEN 'MS'
        WHEN MAX(CASE WHEN e.degree = 'BS' THEN 1 END) = 1 THEN 'BS'
        ELSE 'None'
    END AS highest_degree,

    -- SCD Type 2 tracking
    u.updated_at AS valid_from,
    NULL AS valid_to,
    TRUE AS is_current

FROM users u
LEFT JOIN profiles p ON u.user_id = p.user_id
LEFT JOIN education e ON u.user_id = e.user_id
LEFT JOIN work_history wh ON u.user_id = wh.user_id
WHERE u.is_active = TRUE
GROUP BY u.user_id, p.headline, u.updated_at
```

**Change Detection**: Track when `headline`, `years_experience`, or `highest_degree` changes

---

### 4. `dim_job`
**Type**: SCD Type 2 Dimension
**Upstream Sources**:
- `job_postings` (primary)
- `companies` (lookup)

**Transformation Logic**:
```sql
SELECT
    ROW_NUMBER() OVER (ORDER BY jp.job_id) AS job_key,
    jp.job_id AS job_id_natural,
    jp.title AS job_title,
    c.name AS company_name,
    c.industry AS company_industry,

    -- Calculate description word count
    ARRAY_LENGTH(
        SPLIT(
            REGEXP_REPLACE(jp.description_html, '<[^>]*>', ' '), -- Strip HTML tags
            ' '
        )
    ) AS description_length_words,

    jp.updated_at AS valid_from,
    NULL AS valid_to,
    TRUE AS is_current

FROM job_postings jp
INNER JOIN companies c ON jp.company_id = c.company_id
WHERE jp.status = 'open'
```

**Change Detection**: Track when `title`, `description_html`, or `company_id` changes

---

### 5. `dim_skill`
**Type**: Type 1 Dimension (slowly changing)
**Upstream Sources**:
- `skills` (direct mapping)

**Transformation Logic**:
```sql
SELECT
    ROW_NUMBER() OVER (ORDER BY skill_id) AS skill_key,
    name AS skill_name,
    category
FROM skills
```

**Simple 1:1 mapping with surrogate key**

---

## 🔶 FACT TABLES

### 1. `fact_job_content_engagement`
**Type**: Transaction Fact (Session-level grain)
**Upstream Sources**:
- `streaming_job_activity` (event stream - Bronze)
- `dim_job` (lookup)
- `dim_user` (lookup)
- `dim_date` (lookup)

**Transformation Logic**:
```sql
-- Step 1: Sessionize events and calculate engagement metrics
WITH session_metrics AS (
    SELECT
        session_id,
        job_id,
        user_cookie_id,
        DATE(event_timestamp) AS event_date,

        -- Duration: First to last event in session
        TIMESTAMP_DIFF(
            MAX(event_timestamp),
            MIN(event_timestamp),
            SECOND
        ) AS time_on_page_seconds,

        -- Max scroll depth from SCROLL events
        MAX(
            CASE WHEN event_type = 'SCROLL'
            THEN CAST(JSON_EXTRACT_SCALAR(event_properties, '$.scroll_depth_percent') AS INT64)
            END
        ) AS max_scroll_depth_percent,

        -- Did user click apply button?
        MAX(
            CASE WHEN event_type = 'CLICK'
                 AND JSON_EXTRACT_SCALAR(event_properties, '$.element_id') = 'apply_btn'
            THEN 1 ELSE 0 END
        ) AS did_click_apply

    FROM streaming_job_activity
    GROUP BY session_id, job_id, user_cookie_id, event_date
)

-- Step 2: Join with dimensions and apply business rules
SELECT
    sm.session_id AS engagement_id,
    dj.job_key,
    du.user_key,
    dd.date_key,
    sm.time_on_page_seconds,
    COALESCE(sm.max_scroll_depth_percent, 0) AS max_scroll_depth_percent,

    -- Bounce logic: < 10 seconds
    CASE WHEN sm.time_on_page_seconds < 10 THEN TRUE ELSE FALSE END AS is_bounce,

    CASE WHEN sm.did_click_apply = 1 THEN TRUE ELSE FALSE END AS did_click_apply

FROM session_metrics sm
LEFT JOIN dim_job dj ON sm.job_id = dj.job_id_natural AND dj.is_current = TRUE
LEFT JOIN dim_user du ON sm.user_cookie_id = du.user_id_natural AND du.is_current = TRUE
LEFT JOIN dim_date dd ON CAST(FORMAT_DATE('%Y%m%d', sm.event_date) AS INT64) = dd.date_key
```

**Business Rules**:
- Bounce = session < 10 seconds
- Session = group events by `session_id`

---

### 2. `fact_application_flow`
**Type**: Accumulating Snapshot Fact
**Upstream Sources**:
- `applications` (primary - OLTP state)
- `streaming_job_activity` (optional - intent tracking)
- `dim_job`, `dim_user`, `dim_date` (lookups)

**Transformation Logic**:
```sql
SELECT
    CONCAT(a.user_id, '_', a.job_id, '_', a.application_id) AS flow_id,
    dj.job_key,
    du.user_key,
    CAST(FORMAT_DATE('%Y%m%d', DATE(a.applied_at)) AS INT64) AS date_key,

    -- Exit step logic (funnel position)
    CASE
        WHEN a.current_status = 'applied' THEN 3
        WHEN a.current_status IN ('rejected', 'offer') THEN 3
        ELSE 1  -- Incomplete applications
    END AS exit_step,

    -- Completion flag
    CASE WHEN a.current_status = 'applied' THEN TRUE ELSE FALSE END AS is_completed

FROM applications a
LEFT JOIN dim_job dj ON a.job_id = dj.job_id_natural AND dj.is_current = TRUE
LEFT JOIN dim_user du ON a.user_id = du.user_id_natural AND du.is_current = TRUE
LEFT JOIN dim_date dd ON CAST(FORMAT_DATE('%Y%m%d', DATE(a.applied_at)) AS INT64) = dd.date_key
```

**Funnel Steps**:
- Step 1: View job listing
- Step 2: Click apply button
- Step 3: Submit application
- Completed = status = 'applied'

---

### 3. `fact_job_skill_demand`
**Type**: Factless Fact (many-to-many bridge)
**Upstream Sources**:
- `job_skills` (bridge table)
- `dim_job`, `dim_skill` (lookups)

**Transformation Logic**:
```sql
SELECT
    dj.job_key,
    ds.skill_key,
    js.importance AS importance_weight

FROM job_skills js
INNER JOIN dim_job dj ON js.job_id = dj.job_id_natural AND dj.is_current = TRUE
INNER JOIN dim_skill ds ON js.skill_id = ds.skill_key
```

**GNN Graph Edge**: Job → Skill (weighted by importance 1-5)

---

### 4. `fact_user_skill_competency`
**Type**: Factless Fact (many-to-many bridge)
**Upstream Sources**:
- `user_skills` (bridge table)
- `dim_user`, `dim_skill` (lookups)

**Transformation Logic**:
```sql
SELECT
    du.user_key,
    ds.skill_key,
    us.proficiency AS proficiency_level

FROM user_skills us
INNER JOIN dim_user du ON us.user_id = du.user_id_natural AND du.is_current = TRUE
INNER JOIN dim_skill ds ON us.skill_id = ds.skill_key
```

**GNN Graph Edge**: User → Skill (weighted by proficiency 1-5)

---

## 🔸 DATA MART (GOLD LAYER)

### `mart_job_performance_daily`
**Type**: Aggregate Fact (pre-aggregated for dashboards)
**Grain**: Job + Date
**Upstream Sources**:
- `fact_job_content_engagement`
- `fact_application_flow`
- `dim_date`, `dim_job`

**Transformation Logic**:
```sql
WITH engagement_agg AS (
    SELECT
        job_key,
        date_key,
        COUNT(*) AS view_count,
        COUNT(DISTINCT user_key) AS userview_count,
        SUM(time_on_page_seconds) AS total_view_duration_sec,
        MAX(time_on_page_seconds) AS max_view_duration_sec,
        AVG(time_on_page_seconds) AS avg_user_max_view_duration_sec,
        SUM(CASE WHEN is_bounce THEN 1 ELSE 0 END) AS bounce_count,
        SUM(
            CASE WHEN max_scroll_depth_percent > 70 AND did_click_apply = FALSE
            THEN 1 ELSE 0 END
        ) AS deepread_noapply_count
    FROM fact_job_content_engagement
    GROUP BY job_key, date_key
),

funnel_agg AS (
    SELECT
        job_key,
        date_key,
        COUNT(*) AS start_step1_count,
        SUM(CASE WHEN exit_step >= 2 THEN 1 ELSE 0 END) AS start_step2_count,
        SUM(CASE WHEN exit_step >= 3 THEN 1 ELSE 0 END) AS start_step3_count,
        SUM(CASE WHEN is_completed THEN 1 ELSE 0 END) AS completed_apply_count
    FROM fact_application_flow
    GROUP BY job_key, date_key
)

SELECT
    FARM_FINGERPRINT(CONCAT(CAST(ea.date_key AS STRING), '_', CAST(ea.job_key AS STRING))) AS job_day_sk,
    ea.date_key AS datekey,
    ea.job_key AS job_id,

    -- Engagement metrics
    COALESCE(ea.view_count, 0) AS view_count,
    COALESCE(ea.userview_count, 0) AS userview_count,
    COALESCE(ea.total_view_duration_sec, 0) AS total_view_duration_sec,
    COALESCE(ea.max_view_duration_sec, 0) AS max_view_duration_sec,
    COALESCE(ea.avg_user_max_view_duration_sec, 0.0) AS avg_user_max_view_duration_sec,

    -- Funnel metrics
    COALESCE(fa.start_step1_count, 0) AS start_step1_count,
    COALESCE(fa.start_step2_count, 0) AS start_step2_count,
    COALESCE(fa.start_step3_count, 0) AS start_step3_count,
    COALESCE(fa.completed_apply_count, 0) AS completed_apply_count,

    -- Drop-off calculations
    (COALESCE(fa.start_step1_count, 0) - COALESCE(fa.start_step2_count, 0)) AS drop_step1_count,
    (COALESCE(fa.start_step2_count, 0) - COALESCE(fa.start_step3_count, 0)) AS drop_step2_count,
    (COALESCE(fa.start_step3_count, 0) - COALESCE(fa.completed_apply_count, 0)) AS drop_step3_count,

    -- Reason engine metrics
    COALESCE(ea.bounce_count, 0) AS bounce_count,
    COALESCE(ea.deepread_noapply_count, 0) AS deepread_noapply_count,

    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at

FROM engagement_agg ea
FULL OUTER JOIN funnel_agg fa USING (job_key, date_key)
```

**Key Calculations**:
- `drop_step1_count` = Step 1 starts - Step 2 starts
- `drop_step2_count` = Step 2 starts - Step 3 starts
- `drop_step3_count` = Step 3 starts - Completed
- `deepread_noapply_count` = scroll > 70% AND no apply click (KPI)

---

## 📊 Data Flow Summary

```
┌─────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER (OLTP)                      │
├─────────────────────────────────────────────────────────────┤
│ Identity Profile: users, profiles, education, work_history │
│ Job Market: companies, job_postings, skills, job_skills    │
│ Transactions: applications, user_skills                    │
│ Event Stream: streaming_job_activity                       │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                   SILVER LAYER (Warehouse)                  │
├─────────────────────────────────────────────────────────────┤
│ DIMENSIONS:                                                 │
│  • dim_user ← users + profiles + education + work_history  │
│  • dim_job ← job_postings + companies                      │
│  • dim_skill ← skills                                       │
│  • dim_date, dim_time ← generated                          │
│                                                             │
│ FACTS:                                                      │
│  • fact_job_content_engagement ← streaming_job_activity    │
│  • fact_application_flow ← applications                    │
│  • fact_job_skill_demand ← job_skills                      │
│  • fact_user_skill_competency ← user_skills                │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                     GOLD LAYER (Marts)                      │
├─────────────────────────────────────────────────────────────┤
│ mart_job_performance_daily ← facts + dimensions            │
│  (Aggregated by job + date for dashboards)                 │
└─────────────────────────────────────────────────────────────┘
```

---

## 🔑 Key Design Decisions

### 1. **SCD Type 2 for User & Job Dimensions**
- Tracks historical changes in user profiles and job postings
- Enables trend analysis (e.g., "How did skill requirements change over time?")

### 2. **Sessionization Strategy**
- `session_id` groups related events in `streaming_job_activity`
- Enables calculation of engagement duration and bounce rates

### 3. **Fact Grain Choices**
- `fact_job_content_engagement`: One row per session (not per event)
- `fact_application_flow`: One row per application (accumulating snapshot)
- Skills facts: One row per relationship (bridge tables)

### 4. **GNN Graph Edges**
- `fact_job_skill_demand` = Job → Skill edges (weighted)
- `fact_user_skill_competency` = User → Skill edges (weighted)
- Enables skill-based job matching algorithms

### 5. **Bounce Logic**
- Defined as session duration < 10 seconds
- Critical for identifying low-quality traffic

### 6. **Deep Read KPI**
- `scroll_depth > 70% AND no apply click`
- Identifies interest without conversion (optimization target)

---

## 📈 Incremental Load Strategy

### Daily Refresh Pattern:
```
1. Load new OLTP records (WHERE _ingestion_time > last_run_timestamp)
2. Update SCD Type 2 dimensions (detect changes, close old records)
3. Process new streaming events (sessionize and aggregate)
4. Rebuild gold layer mart (full refresh or incremental merge)
```

### CDC Considerations:
- Use `_ingestion_time` for incremental loads
- Track `updated_at` for change detection in dimensions
- `streaming_job_activity` is append-only (no updates)

---

## End of Design Document
