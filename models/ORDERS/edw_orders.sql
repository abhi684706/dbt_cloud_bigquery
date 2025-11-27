{{ config(materialized='view',
    alias = 'edw_orders') }}


with stg_orders as (
    select * from {{ ref('raw_orders') }}
),

final as (
    select
        order_id,
        customer_id,
        order_date,
        order_status,
        total_amount,
        payment_method,
        shipping_address,
        created_at,
        
        -- Derived fields
        extract(year from order_date) as order_year,
        extract(month from order_date) as order_month,
        extract(dayofweek from order_date) as order_day_of_week,
        format_date('%B', order_date) as order_month_name,
        
        -- Business logic
        case 
            when order_status = 'delivered' then true
            else false
        end as is_completed,
        
        case 
            when order_status = 'cancelled' then true
            else false
        end as is_cancelled,
        
        case
            when total_amount < 100 then 'Low'
            when total_amount between 100 and 300 then 'Medium'
            else 'High'
        end as order_value_tier,
        
        date_diff(current_date(), order_date, day) as days_since_order,
        
        _loaded_at
    from stg_orders
)

select * from final