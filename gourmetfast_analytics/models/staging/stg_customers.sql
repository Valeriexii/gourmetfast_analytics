{{ config(materialized='view') }}

with source as (

    select
        customer_id,
        nullif(trim(name), '')            as customer_name,
        lower(nullif(trim(email), ''))    as email,
        try_cast(signup_date as date)     as signup_date
    from {{ ref('raw_customers') }}

),

deduped as (

    select *
    from (
        select
            *,
            row_number() over (
                partition by customer_id
                order by signup_date desc nulls last
            ) as rn
        from source
    )
    where rn = 1

)

select
    customer_id,
    customer_name,
    email,
    signup_date
from deduped
where signup_date is not null