with source as (

    select * from {{ source('main', 'fhv_bases') }}

),

renamed as (

    select
        column0,
        column1,
        column2,
        column3,
        filename

    from source

)

select * from renamed