
      
  
    
    

    create  table
      "medallion"."silver"."silver_customers_snapshot"
  
    as (
      
    

    select *,
        md5(coalesce(cast(customer_id as varchar ), '')
         || '|' || coalesce(cast(updated_at as varchar ), '')
        ) as dbt_scd_id,
        updated_at as dbt_updated_at,
        updated_at as dbt_valid_from,
        
  
  coalesce(nullif(updated_at, updated_at), null)
  as dbt_valid_to
from (
        



with source as (
    select * from "medallion"."main_bronze"."bronze_customers"
    where created_at::date = CAST('2023-01-01' AS DATE)
),

cleaned as (
    select
        customer_id,
        lower(trim(email)) as email,
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        first_name || ' ' || last_name as full_name,
        created_at,
        updated_at,
        current_timestamp as dbt_loaded_at
    from source
    where
        -- Data quality filters
        customer_id is not null
        and email is not null
        and email like '%@%.%'  -- Basic email validation
        and first_name is not null
        and last_name is not null
)

select * from cleaned

    ) sbq



    );
  
  
  