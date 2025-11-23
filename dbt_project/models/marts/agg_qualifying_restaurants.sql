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

_90_day_users as (

    select * from {{ ref('agg_90_day_users') }}

),

avg_rating_90 as (

    select

        business_id,
        round(avg(stars), 3) as avg_rating_90

    from train_reviews

    group by business_id

),

avg_rating_365 as (

    select

        business_id,
        round(avg(stars), 3) as avg_rating_365

    from full_reviews

    group by business_id

),

combined as (

    select

        restaurants.business_id,

        restaurants.name,
        restaurants.address,
        restaurants.city,
        restaurants.state,
        restaurants.postal_code,
        restaurants.latitude,
        restaurants.longitude,
        restaurants.categories,
        restaurants.first_review_date,
        restaurants.first_90_review_count,
        restaurants.first_365_review_count,

        avg_rating_90.avg_rating_90,
        avg_rating_365.avg_rating_365,

        checkins.sun_cnt,
        checkins.mon_cnt,
        checkins.tues_cnt,
        checkins.wed_cnt,
        checkins.thur_cnt,
        checkins.fri_cnt,
        checkins.sat_cnt,
        checkins.morning_cnt,
        checkins.afternoon_cnt,
        checkins.evening_cnt,
        checkins.late_cnt,

        _90_day_users.users,
        _90_day_users.users_avg_reviews,
        _90_day_users.users_avg_avg_stars,
        _90_day_users.users_avg_fans,
        _90_day_users.users_avg_cool,
        _90_day_users.users_avg_useful,
        _90_day_users.users_avg_funny,
        _90_day_users.users_avg_compliments,
        _90_day_users.users_avg_years_elite,
        _90_day_users.users_avg_num_friends

    from restaurants

    left join avg_rating_90
    on avg_rating_90.business_id = restaurants.business_id

    left join avg_rating_365
    on avg_rating_365.business_id = restaurants.business_id

    left join checkins
    on checkins.business_id = restaurants.business_id

    left join _90_day_users
    on _90_day_users.business_id = restaurants.business_id

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
        late_cnt,
        users,
        users_avg_reviews,
        users_avg_avg_stars,
        users_avg_fans,
        users_avg_cool,
        users_avg_useful,
        users_avg_funny,
        users_avg_compliments,
        users_avg_years_elite,
        users_avg_num_friends

    from combined

)

select * from final