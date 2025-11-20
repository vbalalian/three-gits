with

reviews as (

    select * from {{ ref('fct_restaurant_reviews') }}

),

qualifying_restaurants as(

    select * from {{ ref('int_qualifying_restaurants') }}
    
),

full_year_reviews as (

    select

        reviews.review_id,
        reviews.user_id,
        reviews.business_id,
        reviews.stars,
        reviews.date,
        reviews.text,
        reviews.useful,
        reviews.funny,
        reviews.cool

    from reviews

    left join qualifying_restaurants
    on qualifying_restaurants.business_id = reviews.business_id

    where date(reviews.date) <= date_add(
        date(qualifying_restaurants.first_review_date), INTERVAL 365 DAY
        )
)

select * from full_year_reviews