-- tests/order_status_valid.sql

{{ config(severity='warn', store_failures=true) }}

select *
from {{ ref('fct_orders') }}
where status not in ('PENDING', 'SHIPPED', 'CANCELLED', 'COMPLETED', 'DELIVERED', 'RETURNED')