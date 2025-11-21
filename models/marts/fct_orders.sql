{{ config(
    materialized = 'incremental',
    unique_key   = 'order_id'
) }}

select
    o.order_id,
    o.customer_id,
    o.product_id,
    o.order_date,
    o.quantity,
    o.status,
    p.price,
    -- basic revenue metric
    o.quantity * p.price as revenue
from {{ ref('stg_orders') }} o
left join {{ ref('stg_products') }} p
    on o.product_id = p.product_id

{% if is_incremental() %}
    -- assume dbt runs once a day at 10am EST
    -- processing orders from yesterday (late arrival) and today
    where order_date >= (select max(order_date) from {{ this }})
{% endif %}
