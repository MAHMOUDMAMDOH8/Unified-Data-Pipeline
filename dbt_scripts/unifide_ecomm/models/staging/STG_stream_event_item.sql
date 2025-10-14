{{
    config(
        materialized='incremental',
        strategy='append',
        unique_key='order_sk',
        indexes=[
            {"columns": ['order_id'], 'unique': False},
            {"columns": ['product_id'], 'unique': False}
        ],
        target_schema='staging'
    )
}}
with valid_orders as (
    select
        "ORDER_ID"::VARCHAR(25) as order_id,
        "PRODUCT_ID"::varchar(25) as product_id,
        "QUANTITY"::int as quantity,
        "UNIT_PRICE"::FLOAT as  price,
        "CATEGORY"::VARCHAR(25) as category
    from {{source('ecommerce_row','RAW_EVENTS_ITEMS')}}
    where 
        "ORDER_ID" is NOT NULL
        and "PRODUCT_ID" is not NULL
),
Final as(
    SELECT
        md5(order_id || product_id) as  order_sk , *
    from valid_orders 
)


{% if is_incremental() %}
-- Incremental logic: insert only new rows
select * 
from Final
where order_sk not in (select distinct order_sk from {{ this }})

{% else %}
-- Full refresh logic: load all rows
select * from Final
{% endif %}