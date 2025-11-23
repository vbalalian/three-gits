with

restaurants as (

    select * from {{ ref('dim_qualifying_restaurants') }}

),

full_reviews as (

    select * from {{  ref('fct_full_year_reviews')  }}
),

train_reviews as (

    select * from {{ ref('fct_90_day_reviews') }}

),

checkins as (

    select * from {{ ref('agg_restaurant_checkins') }}

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
        round(avg(full_reviews.stars), 3) as avg_rating_365,

        max(checkins.sun_cnt) as sun_cnt,
        max(checkins.mon_cnt) as mon_cnt,
        max(checkins.tues_cnt) as tues_cnt,
        max(checkins.wed_cnt) as wed_cnt,
        max(checkins.thur_cnt) as thur_cnt,
        max(checkins.fri_cnt) as fri_cnt,
        max(checkins.sat_cnt) as sat_cnt,
        max(checkins.morning_cnt) as morning_cnt,
        max(checkins.afternoon_cnt) as afternoon_cnt,
        max(checkins.evening_cnt) as evening_cnt,
        max(checkins.late_cnt) as late_cnt

    from restaurants

    left join full_reviews
    on full_reviews.business_id = restaurants.business_id

    left join train_reviews
    on train_reviews.business_id = restaurants.business_id

    left join checkins
    on checkins.business_id = restaurants.business_id

    group by restaurants.business_id

),

final as (

    select
    
        business_id,
        name,
        address,
        city,
        state,
        postal_code,
        latitude,
        longitude,
        categories,
        first_review_date,
        first_90_review_count,
        first_365_review_count,
        avg_rating_90,
        avg_rating_365,
        sun_cnt,
        mon_cnt,
        tues_cnt,
        wed_cnt,
        thur_cnt,
        fri_cnt,
        sat_cnt,
        morning_cnt,
        afternoon_cnt,
        evening_cnt,
        late_cnt

    from aggregated

)

select * from final