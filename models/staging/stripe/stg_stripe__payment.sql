with 

source as (

    select * from {{ source('stripe', 'payment') }}

),

renamed as (

    select

    from source

)

select * from renamed