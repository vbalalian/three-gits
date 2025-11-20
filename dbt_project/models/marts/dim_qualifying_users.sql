with

users as (

    select * from {{ ref('stg_users') }}

),

_90_day_reviews as (

    select * from {{ ref('fct_90_day_reviews') }}

),

qualifying_user_ids as (

    select

        distinct user_id

    from _90_day_reviews

),

qualifying_users as (

    select

        users.user_id,
        users.review_count,
        users.average_stars,
        users.yelping_since,
        users.fans,
        users.cool,
        users.useful,
        users.funny,
        users.compliments,
        users.years_elite,
        users.num_friends

    from users

    join qualifying_user_ids
        on users.user_id = qualifying_user_ids.user_id

)

select * from qualifying_users