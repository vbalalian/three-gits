with

source_user as (

    select * from {{ source('raw_yelp_data', 'user') }}

),

transformed as (

    select

        user_id,
        review_count,
        average_stars,
        date(yelping_since) as yelping_since,
        fans,
        cool,
        useful,
        funny,
        compliment_list as compliments,
        length(format('%.0f', elite)) / 4 as years_elite,
        array_length(split(friends, ',')) as num_friends
    
    from source_user

    where review_count > 0
    
)

select * from transformed