with orders as (
    select * from {{ source('ecommarce_shopes','orders') }}
)
select * from orders