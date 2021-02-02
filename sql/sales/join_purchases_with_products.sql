create table if not exists stg_join_purchases_with_products as (
    select 
        t.*,
        p.product_name,
        p.product_category 
    from stg_purchases t
    left join stg_products p 
        on p.product_id = t.product_id 
)