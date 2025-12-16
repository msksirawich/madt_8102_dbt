{{
    config(
        materialized='table',
        schema='gold'
    )
}}

-- Gold layer: Job dimension (SCD Type 2)
-- References snapshot for historical tracking
-- Grain: One row per job per version (tracks historical changes)

with job_snapshot as (
    select * from {{ ref('snap_job_dimension') }}
),

job_dimension as (
    select
        -- Surrogate key (dbt_scd_id from snapshot)
        {{ dbt_utils.generate_surrogate_key(['job_id', 'dbt_valid_from']) }} as job_key,

        -- Natural key
        job_id as job_id_natural,

        -- Job attributes
        title as job_title,
        description_html,
        min_salary,
        max_salary,
        status as job_status,
        published_at,

        -- Salary calculations
        avg_salary,
        salary_range_width,

        -- Description metrics
        description_length_words,

        -- Company attributes
        company_name,
        company_industry,
        company_size,

        -- Derived classifications
        seniority_level,
        salary_tier,
        description_quality,

        -- SCD Type 2 tracking fields (from snapshot)
        dbt_valid_from as valid_from,
        dbt_valid_to as valid_to,
        case when dbt_valid_to is null then true else false end as is_current,

        -- Audit fields
        job_created_at,
        job_updated_at,
        current_timestamp as _created_at

    from job_snapshot
)

select * from job_dimension
order by job_id_natural, valid_from
