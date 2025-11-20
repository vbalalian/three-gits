with

source_reviews as (

  select * from {{ ref('fct_restaurant_reviews') }}

),

business_review_meta as(

  select
    business_id,
    min(date) as first_review_date,
    max(date) as last_review_date,
    date_diff(max(date), min(date), day) as review_range_days

  from source_reviews
  group by business_id
)

select * from business_review_meta