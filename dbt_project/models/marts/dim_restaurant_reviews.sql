with

source as (

    select * from {{ ref('stg_restaurant_reviews') }}

)

select * from source