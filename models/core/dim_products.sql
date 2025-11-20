{{ config(
    materialized = 'incremental',
    unique_key   = 'product_id'
) }}

select
    product_id,
    product_name,
    category,
    price
from {{ ref('stg_products') }}