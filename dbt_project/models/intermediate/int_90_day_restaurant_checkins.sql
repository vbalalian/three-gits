with

checkins as (

select * from {{ ref('fct_checkins') }}

),

qual_restaurants as (

select * from {{ ref('dim_qualifying_restaurants') }}

),

qual_checkins as (

  select

    checkins.business_id,
    checkins.checkin_datetime,
    checkins.checkin_date,
    checkins.checkin_hour,
    checkins.checkin_dayofweek

  from checkins
  inner join qual_restaurants
  on checkins.business_id = qual_restaurants.business_id
  
  where checkins.checkin_date <= date_add(
        date(qual_restaurants.first_review_date), INTERVAL 90 DAY
        )

),

final as (

  select

    business_id,
    checkin_datetime,
    checkin_date,
    checkin_hour,
    checkin_dayofweek

  from qual_checkins

)

select * from final