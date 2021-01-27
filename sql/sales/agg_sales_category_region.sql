drop agg_sales_category_region;

create table agg_sales_category_region as (
    select 
        category,
        region,
        sum(price)
    from transaction
    group by 1, 2
)