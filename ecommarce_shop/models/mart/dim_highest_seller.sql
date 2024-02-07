with best_sellers as (
    select * from {{ ref('stg_sellers') }}
),
seller_orders_items as (
    select 
    ORDER_ID,
	ORDER_ITEM_ID,
	PRODUCT_ID,
	SELLER_ID,
	SHIPPING_LIMIT_DATE,
	PRICE,
	FREIGHT_VALUE

    from {{ ref('stg_order_items')}}

),
highest_seller as (
    select
    oi.ORDER_ID,
    oi.ORDER_ITEM_ID,
    bs.seller_id
    from best_sellers bs
    join seller_orders_items io on io.SELLER_ID = bs.SELLER_ID 

)

select * from highest_seller
