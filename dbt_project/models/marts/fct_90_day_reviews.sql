with

year_reviews as (

    select * from {{ ref('fct_full_year_reviews') }}

),

business_review_meta as (

    select * from {{ ref('int_business_review_meta') }}

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

    left join business_review_meta
    on year_reviews.business_id = business_review_meta.business_id
    where date <= date_add(
        business_review_meta.first_review_date, INTERVAL 90 DAY
        )

)

select * from _90_day_reviews