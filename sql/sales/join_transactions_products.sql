create table if not exists stg_join_transactions_products as (
    select 
        t.*,
        p.productName,
        p.productCategory 
    from stg_transactions t
    left join stg_products p 
        on p.productId = t.productId 
)