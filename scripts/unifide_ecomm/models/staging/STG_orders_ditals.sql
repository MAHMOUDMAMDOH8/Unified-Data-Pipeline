{{
    config(
        materialized='incremental',
        strategy='append',
        unique_key='order_sk',
        indexes = 
        [
            {"columns": ['order_sk'], 'unique': False},
            {"columns": ['product_id'], 'unique': False}
        ],
        target_schema='staging'
    )
}}
with valid_orders as (
    SELECT
        "ORDER_ID":: varchar(20) as order_id,
        "PRODUCT_ID"::VARCHAR(20) as product_id,
        "QUANTITY"::int as quantity,
        "UNIT_PRICE":: FLOAT as price
    from {{source('ecommerce_row','RAW_ORDER_DETAILS')}}
    where "ORDER_ID" is not NULL
),
Final as (
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