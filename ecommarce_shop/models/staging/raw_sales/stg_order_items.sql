with order_items as (
    select * from {{ source('ecommarce_shopes','order_items') }}
)
select * from  order_items