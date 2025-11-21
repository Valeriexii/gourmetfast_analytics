{{ config(materialized='view') }}

with cleaned as (
    select
        cast(product_id as text)                    as product_id,
        nullif(trim(name), '')                      as product_name,
        nullif(trim(category), '')                  as category,
        try_cast(price as decimal(18, 2))           as price
    from {{ source('main', 'raw_products') }}
)

select
    product_id,
    product_name,
    category,
    price
from cleaned
-- assume product can not be negative, remove to clean the dataset
where price >= 0
qualify
    row_number() over (
        partition by product_id
        order by price desc nulls last
    ) = 1
