with products as (
    select PRODUCT_ID, PRODUCT_CATEGORY_NAME, PRODUCT_NAME_LENGTH from {{ ref('stg_product') }}
),

product_details as (
    select * from {{ ref('stg_product_details') }}
),

selling_products_details as (
    select
    PRODUCT_ID,
    PRODUCT_DESCRIPTION_LENGTH,
	PRODUCT_PHOTOS_QTY,
	PRODUCT_WEIGHT_G,
	PRODUCT_LENGTH_CM,
	PRODUCT_HEIGHT_CM,
	PRODUCT_WIDTH_CM
    from product_details
),

orders_items as (
    select * from {{ ref('stg_order_items') }}
),
selling_orders_items as (
    select
    PRODUCT_ID,
    ORDER_ID,
	ORDER_ITEM_ID,
	count(PRODUCT_ID) as number_of_product,
	SELLER_ID,
	PRICE
    from orders_items
    group by ORDER_ID, ORDER_ITEM_ID, SELLER_ID, PRICE, PRODUCT_ID
   
),
sellers as (
    select * from {{ ref('stg_sellers') }}
),
product_sellers as (
    select 
    SELLER_ID,
	SELLER_ZIP_CODE_PREFIX,
	SELLER_CITY,
	SELLER_STATE

    from sellers
),
bestproduct as (
    select 
    p.product_id,
    p.PRODUCT_CATEGORY_NAME,

    spd.PRODUCT_DESCRIPTION_LENGTH,
    spd.PRODUCT_PHOTOS_QTY,
    spd.PRODUCT_WEIGHT_G,
    spd.PRODUCT_LENGTH_CM,
    spd.PRODUCT_HEIGHT_CM,
    spd.PRODUCT_WIDTH_CM,

    soi.number_of_product,
    soi.ORDER_ITEM_ID,
    soi.PRICE,
    soi.SELLER_ID,

    ps.SELLER_CITY,
    ps.SELLER_STATE

    from products p
    join selling_products_details spd on p.product_id = spd.product_id
    join selling_orders_items soi on soi.PRODUCT_ID = spd.PRODUCT_ID 
    join product_sellers ps on soi.seller_id = ps.seller_id
    
)

select * from bestproduct

