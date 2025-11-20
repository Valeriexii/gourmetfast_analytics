{{ config(materialized='view') }}

with cleaned as (
    select
        cast(customer_id as text)      as customer_id,
        nullif(trim(name), '')         as customer_name,
        lower(nullif(trim(email), '')) as email,
        try_cast(signup_date as date)  as signup_date
    from {{ ref('raw_customers') }}
)

select *
from cleaned
where signup_date is not null
qualify
    row_number() over (
        partition by customer_id
        order by signup_date desc
    ) = 1