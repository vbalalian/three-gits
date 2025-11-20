with 

src_review as (

    select * from {{ source('raw_yelp_data', 'review') }}

),

restaurant_ids as (

    select distinct

        business_id as id

  from {{ ref('stg_restaurants') }}

),

restaurant_reviews as (

    select

        review_id,
        user_id,
        business_id,
        stars,
        date,
        text,
        useful,
        funny,
        cool

    from src_review

    where business_id in (select id from restaurant_ids)
    
)

select * from restaurant_reviews