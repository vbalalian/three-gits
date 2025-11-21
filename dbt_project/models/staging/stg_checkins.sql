with

source_checkin as (

    select * from {{ source('raw_yelp_data', 'checkin') }}

),

flattened as (

    select

        source_checkin.business_id,
        trim(checkin_datetime_str) as datetime_str

    from source_checkin,
    unnest(
        split(source_checkin.date, ',')
        ) as checkin_datetime_str

),

transformed as (

    select

        business_id,
        parse_datetime('%Y-%m-%d %H:%M:%S', datetime_str) as checkin_datetime

    from flattened

),

final as (

    select

        business_id,
        checkin_datetime

    from transformed
    
)

select * from final