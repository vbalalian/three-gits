with

source_reviews as (

  select * from {{ ref('fct_restaurant_reviews') }}

),

business_review_meta as(

  select

    business_id,

    min(date) over (partition by business_id) as first_review_date,
    max(date) over (partition by business_id) as last_review_date,

    date_diff(
      max(date) over (partition by business_id), 
      min(date) over (partition by business_id), 
      day
    ) as review_range_days,

    case when date <= date_add(
      min(date) over (partition by business_id), interval 90 day
      ) then 1 else 0 end as is_in_first_90,

    case when date <= date_add(
      min(date) over (partition by business_id), interval 365 day
      ) then 1 else 0 end as is_in_first_365

  from source_reviews

),

aggregated as (

  select
  
    business_id,
    first_review_date,
    last_review_date,
    review_range_days,

    sum(is_in_first_90) as first_90_review_count,
    sum(is_in_first_365) as first_365_review_count

  from business_review_meta

  group by 1, 2, 3, 4

)

select * from aggregated