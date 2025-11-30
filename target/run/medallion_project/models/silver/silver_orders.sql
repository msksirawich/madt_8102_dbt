
        
            delete from "medallion"."main_silver"."silver_orders"
            where (
                order_id) in (
                select (order_id)
                from "silver_orders__dbt_tmp20251026171422519460"
            );

        
    

    insert into "medallion"."main_silver"."silver_orders" ("order_id", "customer_id", "order_date", "order_date_only", "amount", "status", "created_at", "updated_at", "dbt_loaded_at")
    (
        select "order_id", "customer_id", "order_date", "order_date_only", "amount", "status", "created_at", "updated_at", "dbt_loaded_at"
        from "silver_orders__dbt_tmp20251026171422519460"
    )
  