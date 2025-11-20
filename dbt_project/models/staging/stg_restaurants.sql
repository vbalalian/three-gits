with

src_business as (

    select * from {{ source('raw_yelp_data', 'business') }}

),

restaurants as (

    select

        business_id,
        name,
        address,
        city,
        state,
        postal_code,
        latitude,
        longitude,
        stars,
        review_count,
        is_open,
        categories

    from src_business

    where categories like '%Restaurants, %'

)

select * from restaurants