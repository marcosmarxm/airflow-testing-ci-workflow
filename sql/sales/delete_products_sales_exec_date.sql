delete from products_sales where "purchasedate"::text = cast({{ ds }} as text)