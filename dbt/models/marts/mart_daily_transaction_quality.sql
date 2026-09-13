select
    trade_date,
    currency,
    count(*) as transaction_count,
    count(distinct customer_id) as unique_customer_count,
    sum(amount) as total_amount,
    round(avg(amount), 2) as avg_amount,
    min(amount) as min_amount,
    max(amount) as max_amount

from {{ ref('fct_transactions') }}
group by trade_date, currency
