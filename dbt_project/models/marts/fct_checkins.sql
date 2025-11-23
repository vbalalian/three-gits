with

source as (

    select * from {{ ref('stg_checkins') }}

),

transformed as (

    select

        business_id,
        checkin_datetime,
        date(checkin_datetime) as checkin_date,
        extract(hour from checkin_datetime) as checkin_hour,
        extract(dayofweek from checkin_datetime) as checkin_dayofweek

    from source

),

final as (

    select

        business_id,
        checkin_datetime,
        checkin_date,
        checkin_hour,
        checkin_dayofweek

    from transformed

)

select * from final