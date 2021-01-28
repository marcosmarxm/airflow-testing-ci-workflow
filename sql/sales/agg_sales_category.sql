drop table if exists agg_sales_category;

create table if not exists agg_sales_category as (
    select 
        productCategory,
        sum(totalRevenue) as totalRevenueByCategory
    from product_sales
    group by 1
)