with

full_reviews as (

    select * from {{  ref('fct_full_year_reviews')  }}
),

avg_ratings as (

    select 

        business_id,
        round(avg(stars), 2) as avg_rating

    from full_reviews

    group by business_id

)

select * from avg_ratings