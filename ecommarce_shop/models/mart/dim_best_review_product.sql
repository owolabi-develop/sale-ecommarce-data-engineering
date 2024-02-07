
with order_items as (
    select 
    ORDER_ID,
	ORDER_ITEM_ID,
	PRODUCT_ID,
	SELLER_ID
    from {{ ref('stg_order_items') }}
),

products as (
    select
    PRODUCT_ID,
	PRODUCT_CATEGORY_NAME
    from {{ ref('stg_product') }}
),

order_reviews as (
    select * from {{ ref('stg_order_review') }}
),
reviews as (
    select
    REVIEW_ID,
	ORDER_ID,
	sum(REVIEW_SCORE) as total_reviews,
	REVIEW_COMMENT_TITLE,
	REVIEW_COMMENT_MESSAGE
    from order_reviews
    group by  REVIEW_ID, ORDER_ID, REVIEW_COMMENT_TITLE, REVIEW_COMMENT_MESSAGE
),
best_review as (
     select
     p.product_id,
     p.PRODUCT_CATEGORY_NAME,

     ordi.ORDER_ID,
     ordi.ORDER_ITEM_ID,

     rw.total_reviews,
     rw.REVIEW_ID,
	 rw.REVIEW_COMMENT_TITLE



    from order_items ordi
    join products p on p.product_id = ordi.PRODUCT_ID
    join reviews rw on rw.ORDER_ID = ordi.order_id

)

select * from best_review



