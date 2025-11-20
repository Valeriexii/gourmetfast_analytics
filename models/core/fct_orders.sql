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
    -- only process orders newer than what we already have
    where order_date > (
    select coalesce(max(order_date), date '1900-01-01')
    from {{ this }}
    )
{% endif %}