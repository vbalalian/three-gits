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
        restaurants.name,
        restaurants.address,
        restaurants.city,
        restaurants.state,
        restaurants.postal_code,
        restaurants.latitude,
        restaurants.longitude,
        restaurants.stars,
        restaurants.review_count,
        restaurants.is_open,
        restaurants.categories,
        reviews_meta.first_review_date,
        reviews_meta.first_90_review_count,
        reviews_meta.first_365_review_count
        
    from restaurants

    left join reviews_meta
    on reviews_meta.business_id = restaurants.business_id

    where reviews_meta.review_range_days >= 365
    
    and reviews_meta.first_90_review_count >= 3

    and reviews_meta.first_365_review_count >= 10

)

select * from qualifying_restaurants