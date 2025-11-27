{{ config(
    schema = 'raw_cinch_test',
    tags = ['raw_cinch_test'],
    materialized='view',
    alias = 'raw_orders',
    copy_grants = true
    ) }}


with source as (
    select * from {{ source('bigquery_source', 'orders') }}  --raw.orders
),

renamed as (
    select
        order_id,
        customer_id,
        order_date,
        lower(order_status) as order_status,
        total_amount,
        lower(payment_method) as payment_method,
        shipping_address,
        created_at,
        current_timestamp() as _loaded_at
    from source
)

select * from renamed