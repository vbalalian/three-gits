with

restaurants as (

    select * from {{ ref('dim_qualifying_restaurants')}}

),

reviews_90 as (

    select * from {{ ref('fct_90_day_reviews') }}

),

checkins_90 as (
  
  select * from {{ ref('int_90_day_restaurant_checkins') }}

),

checkins_per_restaurant as (
    select
        business_id,
        postal_code,
        count(*) as checkin_count_90
    from checkins_90
    group by business_id, postal_code
),

reviews_per_restaurant as (
    select
        business_id,
        count(distinct user_id) as num_users,
        avg(stars) as avg_stars_90
    from reviews_90
    group by business_id
),

agg_zip as (
    select
        restaurants.postal_code as zip,

        round(avg(restaurants.first_90_review_count), 2) as avg_num_reviews_90,
        round(avg(reviews_per_restaurant.avg_stars_90), 2) as avg_rating_90,
        round(avg(checkins_per_restaurant.checkin_count_90), 2) as avg_checkins_90,
        round(avg(reviews_per_restaurant.num_users), 2) as avg_users

    from restaurants

    left join reviews_per_restaurant
        on reviews_per_restaurant.business_id = restaurants.business_id

    left join checkins_per_restaurant
        on checkins_per_restaurant.business_id = restaurants.business_id

    group by zip
),

final as (

    select

        zip,
        avg_num_reviews_90,
        avg_rating_90,
        avg_checkins_90,
        avg_users

    from agg_zip

)

select * from final