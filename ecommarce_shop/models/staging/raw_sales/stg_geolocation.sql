with geolocations as (
    select * from {{ source('ecommarce_shopes','geolocation') }}
)
select * from geolocations