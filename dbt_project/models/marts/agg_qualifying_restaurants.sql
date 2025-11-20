with

restaurants as (

    select * from {{ ref('int_qualifying_restaurants') }}

),

full_reviews as (

    select * from {{  ref('fct_full_year_reviews')  }}
),

train_reviews as (

    select * from {{ ref('fct_90_day_reviews') }}

),

aggregated as (

    select

        restaurants.business_id,

        max(restaurants.name) as name,
        max(restaurants.address) as address,
        max(restaurants.city) as city,
        max(restaurants.state) as state,
        max(restaurants.postal_code) as postal_code,
        max(restaurants.latitude) as latitude,
        max(restaurants.longitude) as longitude,
        max(restaurants.categories) as categories,
        max(restaurants.first_review_date) as first_review_date,
        max(restaurants.first_90_review_count) as first_90_review_count,
        max(restaurants.first_365_review_count) as first_365_review_count,

        round(avg(train_reviews.stars), 3) as avg_rating_90,
        round(avg(full_reviews.stars), 3) as avg_rating_365

    from restaurants

    left join full_reviews
    on full_reviews.business_id = restaurants.business_id

    left join train_reviews
    on train_reviews.business_id = restaurants.business_id

    group by restaurants.business_id

)

select * from aggregated