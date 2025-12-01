select
    order_id,
    sum(amount) as total_amount
from {{ ref('stg_stripe__payment') }}
group by order_id
having sum(amount) < 0