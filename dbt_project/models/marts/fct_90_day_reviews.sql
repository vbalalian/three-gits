with

year_reviews as (

    select * from {{ ref('fct_full_year_reviews') }}

),

qualifying_restaurants as (

    select * from {{ ref('dim_qualifying_restaurants') }}

),

_90_day_reviews as (

    select

        year_reviews.review_id,
        year_reviews.user_id,
        year_reviews.business_id,
        year_reviews.stars,
        year_reviews.date,
        year_reviews.text,
        year_reviews.useful,
        year_reviews.funny,
        year_reviews.cool

    from year_reviews

    left join qualifying_restaurants
    on year_reviews.business_id =  qualifying_restaurants.business_id
    where date <= date_add(
        qualifying_restaurants.first_review_date, INTERVAL 90 DAY
        )

)

select * from _90_day_reviews