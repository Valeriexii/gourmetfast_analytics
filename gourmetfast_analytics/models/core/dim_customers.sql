{{ config(
    materialized = 'incremental',
    unique_key   = 'customer_id'
) }}

select
    customer_id,
    customer_name,
    email,
    signup_date
from {{ ref('stg_customers') }}
