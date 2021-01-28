drop table if exists agg_sales_category;

create table if not exists agg_sales_category as (
    select 
        product_category,
        sum(total_revenue) as total_revenue_category
    from products_sales
    group by 1
)