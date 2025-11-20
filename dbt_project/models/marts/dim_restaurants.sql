with

source as (

    select * from {{ ref('stg_restaurants') }}

)

select * from source