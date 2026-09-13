with source as (

    select * from {{ source('raw', 'transactions') }}

),

deduplicated as (

    select distinct on (transaction_id) *
    from source
    order by transaction_id

),

renamed as (

    select
        transaction_id::text as transaction_id,
        trade_date::date as trade_date,
        customer_id::text as customer_id,
        account_id::text as account_id,
        security_id::text as security_id,
        amount::numeric as amount,
        upper(trim(currency)) as currency,
        upper(trim(country)) as country

    from deduplicated

)

select * from renamed
