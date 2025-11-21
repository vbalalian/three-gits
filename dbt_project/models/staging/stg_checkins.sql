with

source_checkin as (

    select * from {{ source('raw_yelp_data', 'checkin') }}

),

flattened as (

    select

        source_checkin.business_id,
        checkin_date

    from source_checkin,
    unnest(
        split(source_checkin.date, ',')
        ) as checkin_date

),

final as (

    select

        business_id,
        checkin_date

    from flattened
    
)

select * from final