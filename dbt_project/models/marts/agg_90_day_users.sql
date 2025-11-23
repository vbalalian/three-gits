with

reviews_90 as (

    select * from {{ ref('fct_90_day_reviews') }}

),

users as (

    select * from {{ ref('dim_qualifying_users') }}

),

aggregated as (

    select

        reviews_90.business_id,
        count(reviews_90.user_id) as users,
        round(avg(users.review_count), 2) as users_avg_reviews,
        round(avg(users.average_stars), 2) as users_avg_avg_stars,
        round(avg(users.fans), 2) as users_avg_fans,
        round(avg(users.cool), 2) as users_avg_cool,
        round(avg(users.useful), 2) as users_avg_useful,
        round(avg(users.funny), 2) as users_avg_funny,
        round(avg(users.compliments), 2) as users_avg_compliments,
        round(avg(users.years_elite), 2) as users_avg_years_elite,
        round(avg(users.num_friends), 2) as users_avg_num_friends

    from reviews_90

    inner join users
    on reviews_90.user_id = users.user_id

    group by business_id

),

final as (

    select

        business_id,
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
  
    from aggregated

)

select * from final