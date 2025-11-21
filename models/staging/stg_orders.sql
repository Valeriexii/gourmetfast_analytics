{{ config(materialized='view') }}

with cleaned as (
    select
        cast(order_id as text)                     as order_id,
        cast(customer_id as text)                  as customer_id,
        cast(product_id as text)                   as product_id,
        try_cast(order_date as date)               as order_date,
        try_cast(quantity as integer)              as quantity,
        upper(trim(status))                        as status_raw
    from {{ source('main_raw', 'raw_orders') }}
)

select
    order_id,
    customer_id,
    product_id,
    order_date,
    quantity,
    case
        when status_raw in ('DELIVERED','PENDING', 'SHIPPED', 'CANCELLED', 'COMPLETED','RETURNED')
            then status_raw
        else 'UNKNOWN'
    end as status
from cleaned
-- assume negative & 0 quantity means bad data, remove it to clean the dataset
where quantity > 0
  and order_date is not null
  and order_id is not null
  and customer_id is not null
qualify
    row_number() over (
        partition by order_id
        order by order_date desc
    ) = 1
