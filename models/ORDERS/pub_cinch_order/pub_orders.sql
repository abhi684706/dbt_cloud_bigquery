{{ config(
    schema = 'pub_cinch_test',
    tags = ['pub_cinch_test'],
    materialized='view',
    alias = 'pub_orders',
    copy_grants = true
    ) }}



-- Aggregated customer metrics

with fct_orders as (
    select * from {{ ref('edw_orders') }}
),

customer_metrics as (
    select
        customer_id,
        count(*) as total_orders,
        count(case when is_completed then 1 end) as completed_orders,
        count(case when is_cancelled then 1 end) as cancelled_orders,
        sum(total_amount) as total_revenue,
        avg(total_amount) as avg_order_value,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        date_diff(max(order_date), min(order_date), day) as customer_lifetime_days,
        
        -- Customer segments
        case
            when count(*) = 1 then 'One-time'
            when count(*) between 2 and 3 then 'Occasional'
            else 'Frequent'
        end as customer_segment
        
    from fct_orders
    group by customer_id
)

select * from customer_metrics
