with

reviews as (

    select * from {{ ref('fct_restaurant_reviews') }}

),

business_review_meta as(

    select * from {{ ref('int_business_review_meta') }}
    
),

first_90_count as (

    select

        reviews.business_id,
        count(*) as cnt

    from reviews

    left join business_review_meta
    on reviews.business_id = business_review_meta.business_id
    where reviews.date <= date_add(
        business_review_meta.first_review_date, INTERVAL 90 DAY
        )

    group by business_id

),

full_year_reviews as (

    select

        reviews.review_id,
        reviews.user_id,
        reviews.business_id,
        reviews.stars,
        reviews.date,
        reviews.text,
        reviews.useful,
        reviews.funny,
        reviews.cool

    from reviews

    left join business_review_meta
    on business_review_meta.business_id = reviews.business_id

    left join first_90_count
    on first_90_count.business_id = reviews.business_id
    where business_review_meta.review_range_days > 365
    and date(reviews.date) <= date_add(
        date(business_review_meta.first_review_date), INTERVAL 365 DAY
        )
    and first_90_count.cnt > 2

)

select * from full_year_reviews