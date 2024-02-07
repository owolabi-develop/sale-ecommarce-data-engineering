with sellers as (
    select * from {{ source('ecommarce_shopes','seller') }}
)
select * from sellers