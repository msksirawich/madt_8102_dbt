{% snapshot snap_user_dimension %}

{{
    config(
        target_schema='gold',
        target_database=target.database,
        unique_key='user_id',
        strategy='check',
        check_cols='all',
        invalidate_hard_deletes=True
    )
}}

-- Snapshot source: User dimension combining identity, profile, education, and work history
-- This snapshot captures historical changes to user attributes over time
-- Strategy: Check specific columns for changes to create SCD Type 2 records

with users_base as (
    select *
    from {{ ref('mst_users') }}
    where is_active = 'True'  -- is_active is STRING, not BOOL
    -- and CAST(dt AS DATE) = CAST('{{ var("execution_date") }}' AS DATE)
),

profiles_cleaned as (
    select *
    from {{ ref('mst_profiles') }}
    -- where CAST(dt AS DATE) = CAST('{{ var("execution_date") }}' AS DATE)
),

education_ranked as (
    select
        user_id,
        degree,
        school_name,
        row_number() over (
            partition by user_id
            order by
                case degree
                    when 'PhD' then 1
                    when 'MS' then 2
                    when 'BS' then 3
                    else 4
                end,
                created_at desc
        ) as degree_rank
    from {{ ref('mst_education') }}
    -- where CAST(dt AS DATE) = CAST('{{ var("execution_date") }}' AS DATE)
),

highest_education as (
    select
        user_id,
        degree as highest_degree,
        school_name as highest_degree_school
    from education_ranked
    where degree_rank = 1
),

work_experience as (
    select
        user_id,
        count(*) as total_positions,
        date_diff(current_date(), min(cast(start_date as date)), year) as years_experience,
        max(
            case when end_date is null then title end
        ) as current_position,
        max(
            case when end_date is null then company_name end
        ) as current_company
    from {{ ref('mst_work_history') }}
    -- where CAST(dt AS DATE) = CAST('{{ var("execution_date") }}' AS DATE)
    group by user_id
),

user_snapshot_source as (
    select
        -- Natural key (unique_key for snapshot)
        u.user_id,

        -- User identity
        u.email,
        u.is_active,

        -- Profile attributes
        p.headline,
        p.summary,
        p.current_salary,
        p.willing_to_relocate,

        -- Education attributes
        coalesce(e.highest_degree, 'None') as highest_degree,
        e.highest_degree_school,

        -- Work experience attributes
        coalesce(w.years_experience, 0) as years_experience,
        coalesce(w.total_positions, 0) as total_positions,
        w.current_position,
        w.current_company,

        -- Career level classification
        case
            when coalesce(w.years_experience, 0) < 2 then 'Entry Level'
            when coalesce(w.years_experience, 0) between 2 and 5 then 'Mid Level'
            when coalesce(w.years_experience, 0) between 6 and 10 then 'Senior Level'
            when coalesce(w.years_experience, 0) > 10 then 'Executive Level'
            else 'Unknown'
        end as career_level,

        -- Education level rank
        case e.highest_degree
            when 'PhD' then 4
            when 'MS' then 3
            when 'BS' then 2
            when 'None' then 1
            else 0
        end as education_rank,

        -- Source timestamps
        u.created_at as user_created_at,
        u.updated_at as user_updated_at

    from users_base u
    left join profiles_cleaned p on u.user_id = p.user_id
    left join highest_education e on u.user_id = e.user_id
    left join work_experience w on u.user_id = w.user_id
)

select * from user_snapshot_source

{% endsnapshot %}
