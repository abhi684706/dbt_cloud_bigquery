{{ config(
    schema = 'pub_cinch_test',
    tags = ['pub_cinch_test'],
    materialized='view',
    alias = 'pub_orders_agg_daily_sales',
    copy_grants = true
    ) }}


-- Daily sales aggregations

with fct_orders as (
    select * from {{ ref('edw_orders') }}
),

daily_sales as (
    select
        order_date,
        order_year,
        order_month,
        order_month_name,
        count(*) as order_count,
        count(distinct customer_id) as unique_customers,
        sum(total_amount) as total_revenue,
        avg(total_amount) as avg_order_value,
        
        -- By status
        count(case when is_completed then 1 end) as completed_orders,
        count(case when is_cancelled then 1 end) as cancelled_orders,
        sum(case when is_completed then total_amount end) as completed_revenue,
        
        -- By payment method
        count(case when payment_method = 'credit card' then 1 end) as credit_card_orders,
        count(case when payment_method = 'paypal' then 1 end) as paypal_orders,
        count(case when payment_method = 'debit card' then 1 end) as debit_card_orders
        
    from fct_orders
    group by 
        order_date,
        order_year,
        order_month,
        order_month_name
)

select * from daily_sales
order by order_date desc
