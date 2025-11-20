with

restaurants as (

    select * from {{ ref('dim_restaurants') }}

),

reviews_meta as(

    select * from {{ ref('int_restaurant_review_meta') }}
    
),

qualifying_restaurants as (

    select

        restaurants.business_id,
        reviews_meta.first_review_date

    from restaurants

    left join reviews_meta
    on reviews_meta.business_id = restaurants.business_id

    where reviews_meta.review_range_days >= 365
    
    and reviews_meta.first_90_review_count > 2

)

select * from qualifying_restaurants