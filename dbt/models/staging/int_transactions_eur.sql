with transactions as (

    select * from {{ ref('stg_transactions') }}

),

fx_rates as (

    select * from {{ ref('fx_rates') }}

),

joined as (

    select
        transactions.*,
        fx_rates.rate_to_eur,
        round(transactions.amount * fx_rates.rate_to_eur, 4) as amount_eur

    from transactions
    left join fx_rates
        on transactions.currency = fx_rates.currency

)

select * from joined
