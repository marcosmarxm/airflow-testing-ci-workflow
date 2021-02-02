create table if not exists join_transactions_products as (
    select 
        t.*,
        p.product_name,
        p.product_category 
    from transactions t
    left join products p 
        on p.product_id = t.product_id 
)