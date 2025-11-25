with

checkins as (
  
  select * from {{ ref('int_90_day_restaurant_checkins') }}

),

binned as (
    
  select

    business_id,

    count(case when checkin_dayofweek = 1 then 1 end) as sun_cnt,
    count(case when checkin_dayofweek = 2 then 1 end) as mon_cnt,
    count(case when checkin_dayofweek = 3 then 1 end) as tues_cnt,
    count(case when checkin_dayofweek = 4 then 1 end) as wed_cnt,
    count(case when checkin_dayofweek = 5 then 1 end) as thur_cnt,
    count(case when checkin_dayofweek = 6 then 1 end) as fri_cnt,
    count(case when checkin_dayofweek = 7 then 1 end) as sat_cnt,
    
    count(case when checkin_hour >= 10 and checkin_hour < 16 then 1 end) as morning_cnt,
    count(case when checkin_hour >= 16 and checkin_hour < 22 then 1 end) as afternoon_cnt,
    count(case when checkin_hour >= 22 or checkin_hour < 4 then 1 end) as evening_cnt,
    count(case when checkin_hour >= 4 and checkin_hour < 10 then 1 end) as late_cnt

  from checkins
  group by business_id

),

final as (

  select

    business_id,
    sun_cnt,
    mon_cnt,
    tues_cnt,
    wed_cnt,
    thur_cnt,
    fri_cnt,
    sat_cnt,
    morning_cnt,
    afternoon_cnt,
    evening_cnt,
    late_cnt

  from binned

)

select * from final